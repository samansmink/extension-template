#include "glue_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

#include "glue_api.hpp"
#include "glue_types.hpp"
#include "storage/glue_catalog.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Shared: resolving the table and the partition spec
//===--------------------------------------------------------------------===//
struct GluePartitionTarget {
	GlueCatalog *catalog = nullptr;
	GlueTableInfo table;

	string TableName() const {
		return table.database_name + "." + table.name;
	}
};

GluePartitionTarget ResolveGlueTable(ClientContext &context, const string &function_name, const Value &table_name) {
	auto qualified = QualifiedName::Parse(table_name.GetValue<string>());
	if (qualified.Catalog().empty() || qualified.Schema().empty()) {
		throw BinderException("%s expects a fully qualified table name: '<catalog>.<schema>.<table>', got '%s'",
		                      function_name, table_name.GetValue<string>());
	}
	auto catalog = Catalog::GetCatalogEntry(context, qualified.Catalog());
	if (!catalog) {
		throw BinderException("Catalog '%s' does not exist", qualified.Catalog().GetIdentifierName());
	}
	if (catalog->GetCatalogType() != "glue") {
		throw BinderException("%s only works on tables of a Glue catalog, '%s' is a %s catalog", function_name,
		                      qualified.Catalog().GetIdentifierName(), catalog->GetCatalogType());
	}
	GluePartitionTarget result;
	result.catalog = &catalog->Cast<GlueCatalog>();
	if (!GlueAPI::GetTable(context, *result.catalog, qualified.Schema().GetIdentifierName(),
	                       qualified.Name().GetIdentifierName(), result.table)) {
		throw CatalogException("Table '%s.%s' does not exist in Glue catalog '%s'",
		                       qualified.Schema().GetIdentifierName(), qualified.Name().GetIdentifierName(),
		                       qualified.Catalog().GetIdentifierName());
	}
	if (result.table.GetFormat() != GlueTableFormat::HIVE) {
		throw NotImplementedException("%s only works on Hive tables, '%s' is a %s table", function_name,
		                              result.TableName(), result.table.GetFormatName());
	}
	if (result.table.partition_keys.empty()) {
		throw InvalidInputException("Table '%s' is not partitioned", result.TableName());
	}
	return result;
}

//! Turn a partition spec {key: value, ...} into the partition values (as strings) in partition key order. The spec
//! must name every partition key and nothing else.
vector<string> ParsePartitionSpec(const string &function_name, const GluePartitionTarget &target, const Value &spec) {
	auto &keys = target.table.partition_keys;
	vector<string> key_names;
	for (auto &key : keys) {
		key_names.push_back(key.name);
	}
	auto describe_keys = StringUtil::Join(key_names, ", ");
	if (spec.type().id() != LogicalTypeId::STRUCT || spec.IsNull()) {
		throw BinderException("%s expects the partition as a struct of the partition keys of table '%s': {%s}",
		                      function_name, target.TableName(), describe_keys);
	}
	auto &spec_types = StructType::GetChildTypes(spec.type());
	auto &spec_values = StructValue::GetChildren(spec);
	vector<string> result(keys.size());
	vector<bool> seen(keys.size(), false);
	for (idx_t i = 0; i < spec_types.size(); i++) {
		auto &name = spec_types[i].first.GetIdentifierName();
		idx_t key_index = DConstants::INVALID_INDEX;
		for (idx_t k = 0; k < keys.size(); k++) {
			if (StringUtil::CIEquals(keys[k].name, name)) {
				key_index = k;
				break;
			}
		}
		if (key_index == DConstants::INVALID_INDEX) {
			throw BinderException("'%s' is not a partition key of table '%s', the partition keys are: %s", name,
			                      target.TableName(), describe_keys);
		}
		if (seen[key_index]) {
			throw BinderException("Partition key '%s' is given twice", keys[key_index].name);
		}
		seen[key_index] = true;
		auto &value = spec_values[i];
		if (value.IsNull()) {
			result[key_index] = HivePartitioning::DEFAULT_PARTITION_NAME;
			continue;
		}
		// store the value the way the partition key type would render it
		auto key_type = GlueTypes::ToLogicalType(keys[key_index].type);
		result[key_index] = value.DefaultCastAs(key_type).ToString();
	}
	for (idx_t k = 0; k < keys.size(); k++) {
		if (!seen[k]) {
			throw BinderException("%s: the partition must name every partition key of table '%s' (%s), '%s' is missing",
			                      function_name, target.TableName(), describe_keys, keys[k].name);
		}
	}
	return result;
}

//! The location a partition gets when none is given: <table location>/<key>=<value>/...
string DefaultPartitionLocation(const GluePartitionTarget &target, const vector<string> &values) {
	auto location = target.table.location;
	StringUtil::RTrim(location, "/");
	if (location.empty()) {
		throw InvalidInputException("Table '%s' has no location in Glue, provide the partition location explicitly",
		                            target.TableName());
	}
	auto &keys = target.table.partition_keys;
	for (idx_t i = 0; i < keys.size(); i++) {
		location += "/" + HivePartitioning::Escape(keys[i].name) + "=";
		if (values[i] == HivePartitioning::DEFAULT_PARTITION_NAME) {
			location += values[i];
		} else {
			location += HivePartitioning::EscapeValue(values[i]);
		}
	}
	return location;
}

Value PartitionValueToValue(ClientContext &context, const string &str_value, const LogicalType &type) {
	if (str_value == HivePartitioning::DEFAULT_PARTITION_NAME) {
		return Value(type);
	}
	Value value(str_value);
	if (type.id() == LogicalTypeId::VARCHAR) {
		return value;
	}
	auto cast = value.TryCastAs(context, type);
	if (!cast) {
		return Value(type);
	}
	return value;
}

//! Bind data for the functions that change one partition and report one row when executed
struct GluePartitionChangeBindData : public TableFunctionData {
	GluePartitionTarget target;
	vector<string> values;
	vector<string> new_values;
	string location;
	bool if_not_exists = false;
	bool if_exists = false;
};

struct GluePartitionChangeState : public GlobalTableFunctionState {
	bool done = false;
};

unique_ptr<GlobalTableFunctionState> GluePartitionChangeInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GluePartitionChangeState>();
}

//===--------------------------------------------------------------------===//
// glue_partitions
//===--------------------------------------------------------------------===//
struct GluePartitionsBindData : public TableFunctionData {
	GluePartitionTarget target;
	vector<LogicalType> key_types;
	vector<GluePartitionInfo> partitions;
};

struct GluePartitionsState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

unique_ptr<FunctionData> GluePartitionsBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GluePartitionsBindData>();
	result->target = ResolveGlueTable(context, "glue_partitions", input.inputs[0]);
	auto &table = result->target.table;
	for (auto &key : table.partition_keys) {
		auto type = GlueTypes::ToLogicalType(key.type);
		result->key_types.push_back(type);
		names.emplace_back(key.name);
		return_types.push_back(type);
	}
	names.emplace_back("location");
	return_types.emplace_back(LogicalType::VARCHAR);
	result->partitions = GlueAPI::GetPartitions(context, *result->target.catalog, table.database_name, table.name);
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> GluePartitionsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GluePartitionsState>();
}

void GluePartitionsScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<GluePartitionsBindData>();
	auto &state = data.global_state->Cast<GluePartitionsState>();
	auto &keys = bind_data.target.table.partition_keys;
	idx_t count = 0;
	while (state.offset < bind_data.partitions.size() && count < STANDARD_VECTOR_SIZE) {
		auto &partition = bind_data.partitions[state.offset++];
		for (idx_t k = 0; k < keys.size(); k++) {
			Value value = k < partition.values.size()
			                  ? PartitionValueToValue(context, partition.values[k], bind_data.key_types[k])
			                  : Value(bind_data.key_types[k]);
			output.SetValue(k, count, value);
		}
		output.SetValue(keys.size(), count, Value(partition.location));
		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// glue_add_partition
//===--------------------------------------------------------------------===//
unique_ptr<FunctionData> GlueAddPartitionBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GluePartitionChangeBindData>();
	result->target = ResolveGlueTable(context, "glue_add_partition", input.inputs[0]);
	result->values = ParsePartitionSpec("glue_add_partition", result->target, input.inputs[1]);
	for (auto &option : input.named_parameters) {
		auto name = StringUtil::Lower(option.first.GetIdentifierName());
		if (name == "location") {
			result->location = option.second.GetValue<string>();
			StringUtil::RTrim(result->location, "/");
			if (result->location.empty()) {
				throw BinderException("glue_add_partition: 'location' must not be empty");
			}
		} else if (name == "if_not_exists") {
			result->if_not_exists = option.second.GetValue<bool>();
		}
	}
	if (result->location.empty()) {
		result->location = DefaultPartitionLocation(result->target, result->values);
	}
	names = {"location"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

void GlueAddPartitionScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GluePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GluePartitionChangeBindData>();
	auto &table = bind_data.target.table;
	GluePartitionInput partition;
	partition.values = bind_data.values;
	partition.location = bind_data.location;
	GlueAPI::CreatePartition(context, *bind_data.target.catalog, table.database_name, table.name, partition,
	                         bind_data.if_not_exists);
	output.SetValue(0, 0, Value(bind_data.location));
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// glue_drop_partition
//===--------------------------------------------------------------------===//
unique_ptr<FunctionData> GlueDropPartitionBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GluePartitionChangeBindData>();
	result->target = ResolveGlueTable(context, "glue_drop_partition", input.inputs[0]);
	result->values = ParsePartitionSpec("glue_drop_partition", result->target, input.inputs[1]);
	for (auto &option : input.named_parameters) {
		auto name = StringUtil::Lower(option.first.GetIdentifierName());
		if (name == "if_exists") {
			result->if_exists = option.second.GetValue<bool>();
		}
	}
	names = {"dropped"};
	return_types = {LogicalType::BOOLEAN};
	return std::move(result);
}

void GlueDropPartitionScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GluePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GluePartitionChangeBindData>();
	auto &table = bind_data.target.table;
	auto dropped =
	    GlueAPI::DeletePartition(context, *bind_data.target.catalog, table.database_name, table.name, bind_data.values);
	if (!dropped && !bind_data.if_exists) {
		throw CatalogException("Partition [%s] does not exist in Glue table '%s'",
		                       StringUtil::Join(bind_data.values, ", "), bind_data.target.TableName());
	}
	output.SetValue(0, 0, Value::BOOLEAN(dropped));
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// glue_rename_partition
//===--------------------------------------------------------------------===//
unique_ptr<FunctionData> GlueRenamePartitionBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<GluePartitionChangeBindData>();
	result->target = ResolveGlueTable(context, "glue_rename_partition", input.inputs[0]);
	result->values = ParsePartitionSpec("glue_rename_partition", result->target, input.inputs[1]);
	result->new_values = ParsePartitionSpec("glue_rename_partition", result->target, input.inputs[2]);
	names = {"location"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

void GlueRenamePartitionScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<GluePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<GluePartitionChangeBindData>();
	auto &table = bind_data.target.table;
	auto &catalog = *bind_data.target.catalog;
	GlueAPI::RenamePartition(context, catalog, table.database_name, table.name, bind_data.values, bind_data.new_values);
	GluePartitionInfo renamed;
	string location;
	if (GlueAPI::GetPartition(context, catalog, table.database_name, table.name, bind_data.new_values, renamed)) {
		location = renamed.location;
	}
	output.SetValue(0, 0, Value(location));
	output.SetCardinality(1);
}

} // namespace

TableFunction GetGluePartitionsFunction() {
	TableFunction function("glue_partitions", {LogicalType::VARCHAR}, GluePartitionsScan, GluePartitionsBind,
	                       GluePartitionsInit);
	return function;
}

TableFunction GetGlueAddPartitionFunction() {
	TableFunction function("glue_add_partition", {LogicalType::VARCHAR, LogicalType::ANY}, GlueAddPartitionScan,
	                       GlueAddPartitionBind, GluePartitionChangeInit);
	function.named_parameters["location"] = LogicalType::VARCHAR;
	function.named_parameters["if_not_exists"] = LogicalType::BOOLEAN;
	return function;
}

TableFunction GetGlueDropPartitionFunction() {
	TableFunction function("glue_drop_partition", {LogicalType::VARCHAR, LogicalType::ANY}, GlueDropPartitionScan,
	                       GlueDropPartitionBind, GluePartitionChangeInit);
	function.named_parameters["if_exists"] = LogicalType::BOOLEAN;
	return function;
}

TableFunction GetGlueRenamePartitionFunction() {
	TableFunction function("glue_rename_partition", {LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY},
	                       GlueRenamePartitionScan, GlueRenamePartitionBind, GluePartitionChangeInit);
	return function;
}

} // namespace duckdb
