#include "storage/glue_hive_insert.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

#include "glue_api.hpp"
#include "storage/glue_catalog.hpp"
#include "storage/glue_schema_entry.hpp"
#include "storage/glue_table.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Planning
//===--------------------------------------------------------------------===//
GlueHiveInsert::GlueHiveInsert(PhysicalPlan &physical_plan, LogicalOperator &op, GlueTable &table, bool discard)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT},
                       op.estimated_cardinality),
      table(table), discard(discard) {
}

static optional_ptr<CopyFunctionCatalogEntry> TryGetCopyFunction(DatabaseInstance &db, const string &name) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	auto entry = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, Identifier(name));
	if (!entry) {
		return nullptr;
	}
	return &entry->Cast<CopyFunctionCatalogEntry>();
}

PhysicalOperator &GlueHiveInsert::PlanWrite(ClientContext &context, PhysicalPlanGenerator &planner,
                                            LogicalOperator &op, GlueTable &table, PhysicalOperator &plan,
                                            const vector<Identifier> &names, const vector<LogicalType> &types) {
	auto &table_info = table.table_info;
	auto location = table_info.location;
	StringUtil::RTrim(location, "/");

	// The partition keys of the table, by position in the rows we are given (which may be in query order for
	// CREATE TABLE AS)
	vector<idx_t> partition_columns;
	for (auto &key : table_info.partition_keys) {
		bool found = false;
		for (idx_t i = 0; i < names.size(); i++) {
			if (StringUtil::CIEquals(names[i].GetIdentifierName(), key.name)) {
				partition_columns.push_back(i);
				found = true;
				break;
			}
		}
		if (!found) {
			throw InternalException("Partition key '%s' of Hive table '%s' not found in the rows to insert", key.name,
			                        table_info.name);
		}
	}

	auto copy_function = TryGetCopyFunction(*context.db, "parquet");
	if (!copy_function) {
		throw MissingExtensionException("Writing to Hive table '%s' requires the parquet extension", table_info.name);
	}
	auto copy_info = make_uniq<CopyInfo>();
	copy_info->file_path = location;
	copy_info->format = "parquet";
	copy_info->is_from = false;

	// Hive convention: partition columns live in the directory names, not in the files
	CopyFunctionBindInput bind_input(*copy_info);
	auto names_to_write = LogicalCopyToFile::GetNamesWithoutPartitions(names, partition_columns, false);
	auto types_to_write = LogicalCopyToFile::GetTypesWithoutPartitions(types, partition_columns, false);
	auto function_data =
	    copy_function->function.copy_to_bind(context, bind_input, names_to_write, types_to_write);

	auto &physical_copy = planner.Make<PhysicalCopyToFile>(
	    GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST),
	    copy_function->function, std::move(function_data), op.estimated_cardinality);
	auto &copy = physical_copy.Cast<PhysicalCopyToFile>();
	auto write_id = UUID::ToString(UUID::GenerateRandomUUID());
	copy.use_tmp_file = false;
	if (!partition_columns.empty()) {
		copy.file_path = location;
		copy.partition_output = true;
		copy.partition_columns = partition_columns;
		copy.write_partition_columns = false;
		copy.hive_file_pattern = true;
		copy.filename_pattern.SetFilenamePattern("duckdb_" + write_id + "_{i}");
		// with partitioned output the copy must not initialize a single (partition-less) output file
		copy.write_empty_file = true;
	} else {
		copy.file_path = location + "/duckdb_" + write_id + ".parquet";
		copy.partition_output = false;
		copy.write_empty_file = false;
	}
	copy.file_extension = "parquet";
	copy.overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
	copy.per_thread_output = false;
	copy.return_type = CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST;
	copy.names = names;
	copy.expected_types = types;
	copy.children.push_back(plan);

	auto &insert = planner.Make<GlueHiveInsert>(op, table, false);
	insert.children.push_back(physical_copy);
	return insert;
}

PhysicalOperator &GlueHiveInsert::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalInsert &op, GlueTable &table, optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for insertion into a Hive table");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for insertion into a Hive table");
	}
	D_ASSERT(plan);
	// bring the rows into the table's column order, filling in defaults (NULL) for unmentioned columns
	if (!op.column_index_map.empty()) {
		plan = &planner.ResolveDefaultsProjection(op, *plan);
	}
	vector<Identifier> names;
	vector<LogicalType> types;
	for (auto &column : table.GetColumns().Logical()) {
		names.push_back(column.Name());
		types.push_back(column.Type());
	}
	return PlanWrite(context, planner, op, table, *plan, names, types);
}

PhysicalOperator &GlueHiveInsert::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    LogicalCreateTable &op, PhysicalOperator &plan) {
	// Create the table in Glue first (Glue has no transactions, the table exists from here on even if the insert
	// fails), then write the query result into it
	auto &glue_catalog = op.schema.catalog.Cast<GlueCatalog>();
	auto transaction = glue_catalog.GetCatalogTransaction(context);
	auto entry = op.schema.CreateTable(transaction, *op.info);

	vector<Identifier> names;
	vector<LogicalType> types;
	for (auto &column : op.info->Base().columns.Logical()) {
		names.push_back(column.Name());
		types.push_back(column.Type());
	}
	if (!entry) {
		// CREATE TABLE IF NOT EXISTS on an existing table: nothing is created and nothing is inserted
		auto &base = op.info->Base();
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(base.GetTableName()));
		auto existing = op.schema.LookupEntry(transaction, lookup);
		auto &insert = planner.Make<GlueHiveInsert>(op, existing->Cast<GlueTable>(), true);
		insert.children.push_back(plan);
		return insert;
	}
	return PlanWrite(context, planner, op, entry->Cast<GlueTable>(), plan, names, types);
}

//===--------------------------------------------------------------------===//
// Execution
//===--------------------------------------------------------------------===//
struct GlueHiveInsertGlobalState : public GlobalSinkState {
	mutex lock;
	idx_t insert_count = 0;
	vector<string> written_files;
};

unique_ptr<GlobalSinkState> GlueHiveInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<GlueHiveInsertGlobalState>();
}

SinkResultType GlueHiveInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &state = input.global_state.Cast<GlueHiveInsertGlobalState>();
	lock_guard<mutex> guard(state.lock);
	if (discard) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	// the COPY reports (rows written, files written)
	for (idx_t r = 0; r < chunk.size(); r++) {
		state.insert_count += chunk.GetValue(0, r).GetValue<idx_t>();
		auto files = chunk.GetValue(1, r);
		if (files.IsNull()) {
			continue;
		}
		for (auto &file : ListValue::GetChildren(files)) {
			state.written_files.push_back(file.GetValue<string>());
		}
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType GlueHiveInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<GlueHiveInsertGlobalState>();
	auto &table_info = table.table_info;
	if (discard || table_info.partition_keys.empty() || state.written_files.empty()) {
		return SinkFinalizeType::READY;
	}

	// Register the partition directories the files were written to. The directory names are <key>=<value>, in
	// partition key order, directly below the table location.
	auto location = table_info.location;
	StringUtil::RTrim(location, "/");
	case_insensitive_map_t<GluePartitionInput> partitions;
	for (auto &file : state.written_files) {
		auto directory = file.substr(0, file.find_last_of('/'));
		if (partitions.find(directory) != partitions.end()) {
			continue;
		}
		auto parsed = HivePartitioning::Parse(file);
		GluePartitionInput partition;
		partition.location = directory;
		for (auto &key : table_info.partition_keys) {
			auto value = parsed.find(key.name);
			if (value == parsed.end()) {
				throw InternalException("Written file '%s' has no value for partition key '%s'", file, key.name);
			}
			partition.values.push_back(value->second);
		}
		partitions.emplace(directory, std::move(partition));
	}
	vector<GluePartitionInput> to_register;
	for (auto &entry : partitions) {
		to_register.push_back(entry.second);
	}
	auto &glue_catalog = table.catalog.Cast<GlueCatalog>();
	GlueAPI::BatchCreatePartitions(context, glue_catalog, table_info.database_name, table_info.name, to_register);
	return SinkFinalizeType::READY;
}

SourceResultType GlueHiveInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &state = sink_state->Cast<GlueHiveInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(state.insert_count)));
	return SourceResultType::FINISHED;
}

string GlueHiveInsert::GetName() const {
	return "GLUE_HIVE_INSERT";
}

InsertionOrderPreservingMap<string> GlueHiveInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name.GetIdentifierName();
	return result;
}

} // namespace duckdb
