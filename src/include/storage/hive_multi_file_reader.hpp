#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/function/table_function.hpp"

#include "glue_api.hpp"

namespace duckdb {

//! Everything a Hive table scan knows before any data file is opened: the table schema as Glue defines it, the
//! partitions Glue lists (values and locations) and the data files of every partition
struct HiveScanInfo : public TableFunctionInfo {
	//! Where the table comes from, for error messages: a Glue table or a hive_scan root
	string database_name;
	string table_name;
	//! The table location: the data files of an unpartitioned table live directly below it, and it is the parent of
	//! the <key>=<value> directories of partitions without an explicit location
	string root_location;
	//! The columns of the table: data columns first, partition keys last
	vector<Identifier> names;
	vector<LogicalType> types;
	//! The partition keys, in order
	vector<string> partition_keys;
	//! The partitions registered in Glue (empty for an unpartitioned table)
	vector<GluePartitionInfo> partitions;
	//! The data files to scan
	vector<OpenFileInfo> files;
	//! The partition (index into 'partitions') each data file belongs to
	unordered_map<string, idx_t> file_partitions;

	//! The index of a partition key by name, or DConstants::INVALID_INDEX
	idx_t GetPartitionKeyIndex(const string &name) const;
	//! The partition the file at 'path' belongs to
	const GluePartitionInfo &GetPartitionOfFile(const string &path) const;
	//! A description of the table for error messages
	string Describe() const;
	//! List the data files: those directly below the location of every partition, or directly below the root
	//! location for an unpartitioned table. Files named _* or .* are skipped.
	void CollectFiles(ClientContext &context);
};

//! Bind read_parquet over the files of 'scan_info' with the HiveMultiFileReader. Returns the bound table function
//! and fills in 'bind_data'; the scan produces exactly the columns of 'scan_info'.
TableFunction BindHiveScan(ClientContext &context, shared_ptr<HiveScanInfo> scan_info,
                           unique_ptr<FunctionData> &bind_data);

//! MultiFileReader for Hive tables registered in Glue. It reads the files Glue's partitions point to (whatever their
//! directory names), binds the schema Glue defines rather than the schema of the first file (a column missing from a
//! file reads as NULL, a differently typed column is cast) and fills the partition columns of every file with the
//! values Glue stores for its partition. Filters on partition columns prune whole partitions before any file is opened.
class HiveMultiFileReader : public MultiFileReader {
public:
	explicit HiveMultiFileReader(shared_ptr<HiveScanInfo> scan_info);

	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table);

	unique_ptr<MultiFileReader> Copy() const override;
	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override;
	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	          vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;
	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, MultiFileList &files,
	                                                const MultiFileOptions &options, MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) override;
	void FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
	                  const MultiFileReaderBindData &options, const vector<MultiFileColumnDefinition> &global_columns,
	                  const vector<ColumnIndex> &global_column_ids, ClientContext &context,
	                  optional_ptr<MultiFileReaderGlobalState> global_state) override;

private:
	const HiveScanInfo &ScanInfo() const;

private:
	shared_ptr<HiveScanInfo> scan_info;
};

} // namespace duckdb
