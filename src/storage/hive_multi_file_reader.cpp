#include "storage/hive_multi_file_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

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
	auto entry = file_partitions.find(path);
	if (entry == file_partitions.end()) {
		throw InternalException("Hive scan of '%s.%s': data file '%s' does not belong to any Glue partition",
		                        database_name, table_name, path);
	}
	return partitions[entry->second];
}

//===--------------------------------------------------------------------===//
// HiveMultiFileReader
//===--------------------------------------------------------------------===//
HiveMultiFileReader::HiveMultiFileReader(shared_ptr<TableFunctionInfo> function_info_p)
    : function_info(std::move(function_info_p)) {
}

unique_ptr<MultiFileReader> HiveMultiFileReader::CreateInstance(const TableFunction &table) {
	auto result = make_uniq<HiveMultiFileReader>(table.function_info);
	result->function_name = table.name;
	return std::move(result);
}

unique_ptr<MultiFileReader> HiveMultiFileReader::Copy() const {
	auto result = make_uniq<HiveMultiFileReader>(function_info);
	result->function_name = function_name;
	return std::move(result);
}

const HiveScanInfo &HiveMultiFileReader::ScanInfo() const {
	if (!function_info) {
		throw InternalException("HiveMultiFileReader used without a HiveScanInfo");
	}
	return function_info->Cast<HiveScanInfo>();
}

shared_ptr<MultiFileList> HiveMultiFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                              const FileGlobInput &glob_input) {
	// the files were listed from the Glue partitions when the scan was planned, the paths are not globbed
	return make_shared_ptr<SimpleMultiFileList>(ScanInfo().files);
}

bool HiveMultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                               vector<Identifier> &names, MultiFileReaderBindData &bind_data) {
	// The schema is the one Glue defines, not the schema of the first file: files are mapped to it by column name
	auto &info = ScanInfo();
	return_types = info.types;
	names = info.names;
	bind_data.schema.clear();
	for (idx_t i = 0; i < info.names.size(); i++) {
		auto column = MultiFileColumnDefinition::CreateFromNameAndType(info.names[i], info.types[i]);
		if (info.GetPartitionKeyIndex(info.names[i].GetIdentifierName()) == DConstants::INVALID_INDEX) {
			// a data column that a file does not have (added to the table after the file was written) reads as NULL
			column.default_expression = make_uniq<ConstantExpression>(Value(info.types[i]));
		}
		bind_data.schema.push_back(std::move(column));
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

	// A filter that can be evaluated with the partition values alone decides whether the partition is read at all.
	// The filters themselves are kept: on the files that remain they are cheap, the partition columns are constants.
	auto all_files = files.GetAllFiles();
	vector<OpenFileInfo> kept_files;
	unordered_map<idx_t, bool> partition_kept;
	unordered_set<idx_t> pruning_filters;
	for (auto &file : all_files) {
		auto partition_entry = info.file_partitions.find(file.path);
		if (partition_entry == info.file_partitions.end()) {
			kept_files.push_back(file);
			continue;
		}
		auto partition_index = partition_entry->second;
		auto cached = partition_kept.find(partition_index);
		if (cached == partition_kept.end()) {
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
			cached = partition_kept.emplace(partition_index, keep).first;
		}
		if (cached->second) {
			kept_files.push_back(file);
		}
	}
	pushdown_info.extra_info.total_files = all_files.size();
	pushdown_info.extra_info.filtered_files = kept_files.size();
	if (kept_files.size() == all_files.size()) {
		return nullptr;
	}
	return make_uniq<SimpleMultiFileList>(std::move(kept_files));
}

//===--------------------------------------------------------------------===//
// Per file
//===--------------------------------------------------------------------===//
void HiveMultiFileReader::FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
                                       const MultiFileReaderBindData &options,
                                       const vector<MultiFileColumnDefinition> &global_columns,
                                       const vector<ColumnIndex> &global_column_ids, ClientContext &context,
                                       optional_ptr<MultiFileReaderGlobalState> global_state) {
	MultiFileReader::FinalizeBind(reader_data, file_options, options, global_columns, global_column_ids, context,
	                              global_state);
	auto &info = ScanInfo();
	if (info.partition_keys.empty()) {
		return;
	}
	// the partition columns of this file are constants: the values Glue stores for the file's partition
	auto &partition = info.GetPartitionOfFile(reader_data.reader->GetFileName());
	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto &column_id = global_column_ids[i];
		if (column_id.IsVirtualColumn()) {
			continue;
		}
		auto &global_column = global_columns[column_id.GetPrimaryIndex()];
		auto key_index = info.GetPartitionKeyIndex(global_column.name.GetIdentifierName());
		if (key_index == DConstants::INVALID_INDEX) {
			continue;
		}
		auto &key = info.partition_keys[key_index];
		auto value = HivePartitioning::GetValue(context, key, partition.values[key_index], global_column.type);
		reader_data.constant_map.Add(MultiFileGlobalIndex(i), std::move(value));
	}
}

} // namespace duckdb
