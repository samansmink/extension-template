#include "storage/hive_multi_file_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_file_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/global_csv_state.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

//! The scan info of the hive scan being bound on this thread. The reader is created by the bound function's own bind
//! (read_parquet, read_csv, read_json) through get_multi_file_reader, which only sees the TableFunction, and the
//! function_info slot of the JSON reader is taken by its multi-file wrapper: so the scan info is handed over here.
static thread_local shared_ptr<HiveScanInfo> *current_scan_info = nullptr;

struct HiveScanInfoScope {
	explicit HiveScanInfoScope(shared_ptr<HiveScanInfo> &info) {
		current_scan_info = &info;
	}
	~HiveScanInfoScope() {
		current_scan_info = nullptr;
	}
};

//===--------------------------------------------------------------------===//
// HiveScanInfo
//===--------------------------------------------------------------------===//
idx_t HiveScanInfo::GetPartitionKeyIndex(const string &name) const {
	for (idx_t i = 0; i < partition_keys.size(); i++) {
		if (StringUtil::CIEquals(partition_keys[i], name)) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

const GluePartitionInfo &HiveScanInfo::GetPartitionOfFile(const string &path) const {
	lock_guard<mutex> guard(file_partitions_lock);
	auto entry = file_partitions.find(path);
	if (entry == file_partitions.end()) {
		throw InternalException("Hive scan of '%s.%s': data file '%s' does not belong to any Glue partition",
		                        database_name, table_name, path);
	}
	return partitions[entry->second];
}

string HiveScanInfo::Describe() const {
	if (database_name.empty()) {
		return table_name;
	}
	return database_name + "." + table_name;
}

//! The data files directly below 'location'. Files whose name starts with '_' or '.' (_SUCCESS, .crc, ...) are not
//! data, as in Hive.
static void ListDataFiles(FileSystem &fs, const string &location, vector<OpenFileInfo> &files) {
	auto directory = location;
	StringUtil::RTrim(directory, "/");
	if (directory.empty()) {
		return;
	}
	for (auto &file : fs.GlobFiles(directory + "/*", FileGlobOptions::ALLOW_EMPTY)) {
		auto name = file.path.substr(file.path.find_last_of('/') + 1);
		if (name.empty() || name[0] == '_' || name[0] == '.') {
			continue;
		}
		files.push_back(file);
	}
}

//===--------------------------------------------------------------------===//
// HiveMultiFileList
//===--------------------------------------------------------------------===//
HiveMultiFileList::HiveMultiFileList(ClientContext &context, shared_ptr<HiveScanInfo> scan_info_p,
                                     vector<idx_t> partition_indexes_p)
    : LazyMultiFileList(&context), client_context(context), scan_info(std::move(scan_info_p)),
      partition_indexes(std::move(partition_indexes_p)) {
}

bool HiveMultiFileList::ExpandNextPath() const {
	// called with the list's lock held; expands one partition (one directory listing) per call
	auto &fs = FileSystem::GetFileSystem(client_context);
	if (scan_info->partition_keys.empty()) {
		if (next_partition > 0) {
			return false;
		}
		next_partition++;
		if (scan_info->root_location.empty()) {
			throw InvalidInputException("Hive table '%s' has no location", scan_info->Describe());
		}
		ListDataFiles(fs, scan_info->root_location, expanded_files);
		return true;
	}
	if (next_partition >= partition_indexes.size()) {
		return false;
	}
	auto partition_index = partition_indexes[next_partition++];
	auto &partition = scan_info->partitions[partition_index];
	if (partition.values.size() != scan_info->partition_keys.size()) {
		throw InvalidInputException("Partition [%s] of Hive table '%s' has %d values but the table has %d partition "
		                            "keys",
		                            StringUtil::Join(partition.values, ", "), scan_info->Describe(),
		                            partition.values.size(), scan_info->partition_keys.size());
	}
	vector<OpenFileInfo> partition_files;
	ListDataFiles(fs, partition.location, partition_files);
	lock_guard<mutex> guard(scan_info->file_partitions_lock);
	for (auto &file : partition_files) {
		if (!scan_info->file_partitions.emplace(file.path, partition_index).second) {
			// two partitions share a location, the file belongs to the first
			continue;
		}
		expanded_files.push_back(std::move(file));
	}
	return true;
}

FileExpandResult HiveMultiFileList::GetExpandResult() const {
	// Until everything is listed the answer is a guess, and listing now would defeat the point of listing lazily:
	// the schema comes from the table, so nothing at bind time needs a file
	lock_guard<mutex> lck(lock);
	if (!all_files_expanded) {
		return FileExpandResult::MULTIPLE_FILES;
	}
	if (expanded_files.size() > 1) {
		return FileExpandResult::MULTIPLE_FILES;
	}
	return expanded_files.size() == 1 ? FileExpandResult::SINGLE_FILE : FileExpandResult::NO_FILES;
}

vector<OpenFileInfo> HiveMultiFileList::GetDisplayFileList(optional_idx max_files) const {
	bool expanded;
	{
		lock_guard<mutex> lck(lock);
		expanded = all_files_expanded;
	}
	if (expanded) {
		// the base lists the files, which takes the lock: it must not be held here
		return LazyMultiFileList::GetDisplayFileList(max_files);
	}
	// not listed yet (e.g. EXPLAIN): show the partition directories instead of listing them
	vector<OpenFileInfo> result;
	if (scan_info->partition_keys.empty()) {
		result.emplace_back(scan_info->root_location);
		return result;
	}
	for (auto partition_index : partition_indexes) {
		if (max_files.IsValid() && result.size() >= max_files.GetIndex()) {
			break;
		}
		result.emplace_back(scan_info->partitions[partition_index].location);
	}
	return result;
}

unique_ptr<MultiFileList> HiveMultiFileList::Copy() const {
	return make_uniq<HiveMultiFileList>(client_context, scan_info, partition_indexes);
}

//===--------------------------------------------------------------------===//
// Binding
//===--------------------------------------------------------------------===//
static const TableFunction &GetListReadFunction(ClientContext &context, const string &function_name,
                                                const HiveScanInfo &scan_info) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &catalog_schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	auto catalog_entry = catalog_schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, Identifier(function_name));
	if (!catalog_entry) {
		throw MissingExtensionException("Reading Hive table '%s' requires %s, which is not available",
		                                scan_info.Describe(), function_name);
	}
	auto &function_set = catalog_entry->Cast<TableFunctionCatalogEntry>();
	return *function_set.functions.GetFunctionByArguments(context, {LogicalType::LIST(LogicalType::VARCHAR)});
}

TableFunction BindHiveScan(ClientContext &context, shared_ptr<HiveScanInfo> scan_info,
                           unique_ptr<FunctionData> &bind_data) {
	// the reader for the file format; the data columns (everything but the partition keys) are what the files hold
	child_list_t<Value> data_columns;
	for (idx_t i = 0; i < scan_info->names.size(); i++) {
		if (scan_info->GetPartitionKeyIndex(scan_info->names[i].GetIdentifierName()) == DConstants::INVALID_INDEX) {
			data_columns.emplace_back(scan_info->names[i], Value(scan_info->types[i].ToString()));
		}
	}
	named_parameter_map_t param_map;
	string function_name;
	switch (scan_info->file_format) {
	case HiveFileFormat::PARQUET:
		function_name = "read_parquet";
		break;
	case HiveFileFormat::CSV:
		// Hive CSV files carry no schema: the columns are given, by position
		function_name = "read_csv";
		param_map["columns"] = Value::STRUCT(data_columns);
		param_map["header"] = Value::BOOLEAN(scan_info->header);
		param_map["delim"] = Value(scan_info->delimiter);
		break;
	case HiveFileFormat::JSON:
		// one JSON object per line, keys matched to the columns by name
		ExtensionHelper::AutoLoadExtension(context, "json");
		function_name = "read_json";
		param_map["columns"] = Value::STRUCT(data_columns);
		param_map["format"] = Value("newline_delimited");
		break;
	case HiveFileFormat::AVRO:
		// the files carry their schema, columns are matched by name like parquet
		ExtensionHelper::AutoLoadExtension(context, "avro");
		function_name = "read_avro";
		break;
	}
	auto scan_function = GetListReadFunction(context, function_name, *scan_info);
	// with the HiveMultiFileReader: the table's schema and partition values, not the files'
	scan_function.get_multi_file_reader = HiveMultiFileReader::CreateInstance;

	vector<LogicalType> return_types;
	vector<Identifier> names;
	// the path argument is not used: CreateFileList builds the file list from the partitions
	TableFunctionRef empty_ref;
	vector<Value> inputs = {Value::LIST(LogicalType::VARCHAR, {Value(scan_info->root_location)})};
	// the JSON reader's multi-file wrapper reads its wrapped function from the bind input's info
	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, scan_function.function_info.get(),
	                                  nullptr, scan_function, empty_ref);
	HiveScanInfoScope scope(scan_info);
	bind_data = scan_function.bind(context, bind_input, return_types, names);
	return scan_function;
}

//===--------------------------------------------------------------------===//
// HiveMultiFileReader
//===--------------------------------------------------------------------===//
HiveMultiFileReader::HiveMultiFileReader(shared_ptr<HiveScanInfo> scan_info_p) : scan_info(std::move(scan_info_p)) {
}

unique_ptr<MultiFileReader> HiveMultiFileReader::CreateInstance(const TableFunction &table) {
	shared_ptr<HiveScanInfo> info;
	if (current_scan_info) {
		info = *current_scan_info;
	}
	auto result = make_uniq<HiveMultiFileReader>(std::move(info));
	result->function_name = table.name;
	return std::move(result);
}

unique_ptr<MultiFileReader> HiveMultiFileReader::Copy() const {
	auto result = make_uniq<HiveMultiFileReader>(scan_info);
	result->function_name = function_name;
	return std::move(result);
}

const HiveScanInfo &HiveMultiFileReader::ScanInfo() const {
	if (!scan_info) {
		throw InternalException("HiveMultiFileReader used without a HiveScanInfo (a hive scan can not be restored "
		                        "from a serialized plan)");
	}
	return *scan_info;
}

shared_ptr<MultiFileList> HiveMultiFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                              const FileGlobInput &glob_input) {
	// every partition, listed lazily; ComplexFilterPushdown narrows the partitions before anything is listed
	auto &info = ScanInfo();
	vector<idx_t> partition_indexes;
	for (idx_t i = 0; i < info.partitions.size(); i++) {
		partition_indexes.push_back(i);
	}
	return make_shared_ptr<HiveMultiFileList>(context, scan_info, std::move(partition_indexes));
}

bool HiveMultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                               vector<Identifier> &names, MultiFileReaderBindData &bind_data) {
	// The schema is the one Glue defines, not the schema of the first file: files are mapped to it by column name
	auto &info = ScanInfo();
	return_types = info.types;
	names = info.names;
	bind_data.schema.clear();
	for (idx_t i = 0; i < info.names.size(); i++) {
		// columns a file does not have are filled in per file in FinalizeBind
		bind_data.schema.push_back(MultiFileColumnDefinition::CreateFromNameAndType(info.names[i], info.types[i]));
	}
	bind_data.mapping = MultiFileColumnMappingMode::BY_NAME;
	return true;
}

void HiveMultiFileReader::BindOptions(MultiFileOptions &options, MultiFileList &files,
                                      vector<LogicalType> &return_types, vector<Identifier> &names,
                                      MultiFileReaderBindData &bind_data) {
	// partition values come from Glue, never from the directory names
	options.auto_detect_hive_partitioning = false;
	options.hive_partitioning = false;
	options.union_by_name = false;
	MultiFileReader::BindOptions(options, files, return_types, names, bind_data);
}

//===--------------------------------------------------------------------===//
// Partition pruning
//===--------------------------------------------------------------------===//
//! Replace references to partition columns of the scanned table by the partition's values
static void ReplacePartitionColumnRefs(ClientContext &context, unique_ptr<Expression> &expr, TableIndex table_index,
                                       const unordered_map<idx_t, idx_t> &projection_to_key, const HiveScanInfo &info,
                                       const GluePartitionInfo &partition) {
	if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expr->Cast<BoundColumnRefExpression>();
		if (colref.Binding().table_index != table_index) {
			return;
		}
		auto entry = projection_to_key.find(colref.Binding().column_index.GetIndex());
		if (entry == projection_to_key.end()) {
			return;
		}
		auto &key = info.partition_keys[entry->second];
		auto value = HivePartitioning::GetValue(context, key, partition.values[entry->second], colref.GetReturnType());
		expr = make_uniq<BoundConstantExpression>(std::move(value));
		return;
	}
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		ReplacePartitionColumnRefs(context, child, table_index, projection_to_key, info, partition);
	});
}

unique_ptr<MultiFileList> HiveMultiFileReader::ComplexFilterPushdown(ClientContext &context, MultiFileList &files,
                                                                     const MultiFileOptions &options,
                                                                     MultiFilePushdownInfo &pushdown_info,
                                                                     vector<unique_ptr<Expression>> &filters) {
	auto &info = ScanInfo();
	if (info.partition_keys.empty() || filters.empty()) {
		return nullptr;
	}
	// which projected columns are partition keys
	unordered_map<idx_t, idx_t> projection_to_key;
	for (idx_t i = 0; i < pushdown_info.column_ids.size(); i++) {
		auto column_id = pushdown_info.column_ids[i];
		if (IsVirtualColumn(column_id)) {
			continue;
		}
		auto key_index = info.GetPartitionKeyIndex(pushdown_info.column_names[column_id].GetIdentifierName());
		if (key_index != DConstants::INVALID_INDEX) {
			projection_to_key[i] = key_index;
		}
	}
	if (projection_to_key.empty()) {
		return nullptr;
	}

	// A filter that can be evaluated with the partition values alone decides whether the partition is read at all,
	// before its directory is listed. The filters themselves are kept: on the rows that remain they are cheap, the
	// partition columns are constants.
	auto &hive_list = files.Cast<HiveMultiFileList>();
	auto &candidates = hive_list.PartitionIndexes();
	vector<idx_t> kept;
	unordered_set<idx_t> pruning_filters;
	for (auto partition_index : candidates) {
		auto &partition = info.partitions[partition_index];
		bool keep = true;
		for (idx_t filter_index = 0; filter_index < filters.size(); filter_index++) {
			auto &filter = filters[filter_index];
			auto filter_copy = filter->Copy();
			ReplacePartitionColumnRefs(context, filter_copy, pushdown_info.table_index, projection_to_key, info,
			                           partition);
			Value result;
			if (!filter_copy->IsScalar() || !filter_copy->IsFoldable() ||
			    !ExpressionExecutor::TryEvaluateScalar(context, *filter_copy, result)) {
				// needs the data columns, can not decide here
				continue;
			}
			if (result.IsNull() || !result.GetValue<bool>()) {
				keep = false;
				if (pruning_filters.insert(filter_index).second) {
					if (!pushdown_info.extra_info.file_filters.empty()) {
						pushdown_info.extra_info.file_filters += " AND ";
					}
					pushdown_info.extra_info.file_filters += filter->ToString();
				}
				break;
			}
		}
		if (keep) {
			kept.push_back(partition_index);
		}
	}
	// reported as files in EXPLAIN, but these are partitions: nothing has been listed yet
	pushdown_info.extra_info.total_files = candidates.size();
	pushdown_info.extra_info.filtered_files = kept.size();
	if (kept.size() == candidates.size()) {
		return nullptr;
	}
	return make_uniq<HiveMultiFileList>(context, scan_info, std::move(kept));
}

//===--------------------------------------------------------------------===//
// Opening files
//===--------------------------------------------------------------------===//
shared_ptr<BaseFileReader> HiveMultiFileReader::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
                                                             const OpenFileInfo &file, idx_t file_idx,
                                                             const MultiFileBindData &bind_data) {
	auto &info = ScanInfo();
	if (info.file_format != HiveFileFormat::CSV) {
		return MultiFileReader::CreateReader(context, gstate, file, file_idx, bind_data);
	}
	// The CSV reader would open the file with the global columns (partition columns included) and sniff a schema
	// it was never given. A Hive CSV file holds exactly the data columns, in order, with the dialect the table
	// describes: open it with those and without sniffing.
	auto &csv_data = bind_data.bind_data->Cast<ReadCSVData>();
	auto &csv_gstate = gstate.Cast<CSVGlobalState>();
	auto options = csv_data.options;
	options.auto_detect = false;
	vector<Identifier> names;
	vector<LogicalType> types;
	for (idx_t i = 0; i < info.names.size(); i++) {
		if (info.GetPartitionKeyIndex(info.names[i].GetIdentifierName()) == DConstants::INVALID_INDEX) {
			names.push_back(info.names[i]);
			types.push_back(info.types[i]);
		}
	}
	return make_shared_ptr<CSVFileScan>(context, file, std::move(options), bind_data.file_options, names, types,
	                                    csv_data.csv_schema, csv_gstate.SingleThreadedRead(), nullptr, false);
}

//===--------------------------------------------------------------------===//
// Per file
//===--------------------------------------------------------------------===//
void HiveMultiFileReader::FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
                                       const MultiFileReaderBindData &options,
                                       const vector<MultiFileColumnDefinition> &global_columns,
                                       const vector<ColumnIndex> &global_column_ids, ClientContext &context,
                                       optional_ptr<MultiFileReaderGlobalState> global_state) {
	// The first constant registered for a column wins, so the partition values go in before the base runs: with a
	// union schema (the JSON reader's default) the base would otherwise fill every column a file lacks, partition
	// columns included, with NULL.
	auto &info = ScanInfo();
	case_insensitive_set_t local_names;
	for (auto &local_column : reader_data.reader->GetColumns()) {
		local_names.insert(local_column.name.GetIdentifierName());
	}
	optional_ptr<const GluePartitionInfo> partition;
	if (!info.partition_keys.empty()) {
		partition = &info.GetPartitionOfFile(reader_data.reader->GetFileName());
	}
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto &column_id = global_column_ids[i];
		if (column_id.IsVirtualColumn()) {
			continue;
		}
		auto &global_column = global_columns[column_id.GetPrimaryIndex()];
		auto &name = global_column.name.GetIdentifierName();
		auto key_index = info.GetPartitionKeyIndex(name);
		if (key_index != DConstants::INVALID_INDEX) {
			// a partition column is a constant: the value Glue stores for the file's partition
			auto &key = info.partition_keys[key_index];
			auto value = HivePartitioning::GetValue(context, key, partition->values[key_index], global_column.type);
			reader_data.constant_map.Add(MultiFileGlobalIndex(i), std::move(value));
			continue;
		}
		if (local_names.find(name) == local_names.end()) {
			// a data column the file does not have (added to the table after the file was written) reads as NULL
			auto &type = column_id.HasType() ? column_id.GetScanType() : global_column.type;
			reader_data.constant_map.Add(MultiFileGlobalIndex(i), Value(type));
		}
	}
	// the filename / file_index virtual columns
	MultiFileReader::FinalizeBind(reader_data, file_options, options, global_columns, global_column_ids, context,
	                              global_state);
}

} // namespace duckdb
