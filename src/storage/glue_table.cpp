#include "storage/glue_table.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "glue_functions.hpp"
#include "glue_types.hpp"
#include "storage/glue_catalog.hpp"
#include "storage/glue_schema_entry.hpp"
#include "storage/hive_multi_file_reader.hpp"

namespace duckdb {

GlueTable::GlueTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, GlueTableInfo table_info_p)
    : TableCatalogEntry(catalog, schema, info), table_info(std::move(table_info_p)), columns(info.columns.Copy()) {
	this->internal = false;
}

const ColumnList &GlueTable::GetColumns() const {
	return columns;
}

unique_ptr<BaseStatistics> GlueTable::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

TableStorageInfo GlueTable::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	return result;
}

GlueTableInfo GlueTable::RefreshTableInfo(ClientContext &context) const {
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	GlueTableInfo result;
	if (!GlueAPI::GetTable(context, glue_catalog, table_info.database_name, table_info.name, result)) {
		throw CatalogException("Glue table '%s.%s' no longer exists", table_info.database_name, table_info.name);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
TableFunction GlueTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	// Ask Glue what kind of table this is right before scanning: only Hive (Glue native) tables can be read
	auto latest_info = RefreshTableInfo(context);
	switch (latest_info.GetFormat()) {
	case GlueTableFormat::HIVE:
		return GetHiveScanFunction(context, bind_data, latest_info);
	default:
		throw NotImplementedException("Scan from table with type %s", latest_info.GetFormatName());
	}
}

//===--------------------------------------------------------------------===//
// Hive scan
//===--------------------------------------------------------------------===//
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

TableFunction GlueTable::GetHiveScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                             const GlueTableInfo &latest_info) {
	// The SerDe decides the file format. Only parquet is supported so far.
	auto serde = StringUtil::Lower(latest_info.serde_library);
	if (!StringUtil::Contains(serde, "parquet")) {
		throw NotImplementedException("Reading Hive table '%s.%s' with SerDe '%s' is not supported yet, only "
		                              "parquet tables (ParquetHiveSerDe) can be read",
		                              latest_info.database_name, latest_info.name, latest_info.serde_library);
	}

	// The scan produces the columns this entry was planned with: data columns first, partition keys last
	auto scan_info = make_shared_ptr<HiveScanInfo>();
	scan_info->database_name = latest_info.database_name;
	scan_info->table_name = latest_info.name;
	for (auto &column : GetColumns().Logical()) {
		scan_info->names.push_back(column.Name());
		scan_info->types.push_back(column.Type());
	}
	for (auto &key : table_info.partition_keys) {
		scan_info->partition_keys.push_back(key.name);
	}

	// The data files: those of every partition Glue lists (each partition has its own location, which need not be
	// <key>=<value> below the table location), or those below the table location for an unpartitioned table
	auto &fs = FileSystem::GetFileSystem(context);
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	if (scan_info->partition_keys.empty()) {
		if (latest_info.location.empty()) {
			throw InvalidInputException("Hive table '%s.%s' has no location in Glue", latest_info.database_name,
			                            latest_info.name);
		}
		ListDataFiles(fs, latest_info.location, scan_info->files);
	} else {
		scan_info->partitions =
		    GlueAPI::GetPartitions(context, glue_catalog, latest_info.database_name, latest_info.name);
		for (idx_t partition_index = 0; partition_index < scan_info->partitions.size(); partition_index++) {
			auto &partition = scan_info->partitions[partition_index];
			if (partition.values.size() != scan_info->partition_keys.size()) {
				throw InvalidInputException("Glue partition [%s] of Hive table '%s.%s' has %d values but the table "
				                            "has %d partition keys",
				                            StringUtil::Join(partition.values, ", "), latest_info.database_name,
				                            latest_info.name, partition.values.size(),
				                            scan_info->partition_keys.size());
			}
			vector<OpenFileInfo> partition_files;
			ListDataFiles(fs, partition.location, partition_files);
			for (auto &file : partition_files) {
				if (scan_info->file_partitions.find(file.path) != scan_info->file_partitions.end()) {
					// two partitions share a location, the file belongs to the first
					continue;
				}
				scan_info->file_partitions.emplace(file.path, partition_index);
				scan_info->files.push_back(std::move(file));
			}
		}
	}
	if (scan_info->files.empty()) {
		return MakeGlueEmptyScan(bind_data);
	}

	auto &db = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &catalog_schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	auto catalog_entry = catalog_schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, "read_parquet");
	if (!catalog_entry) {
		throw MissingExtensionException("Reading Hive table '%s.%s' requires the parquet extension",
		                                latest_info.database_name, latest_info.name);
	}
	auto &function_set = catalog_entry->Cast<TableFunctionCatalogEntry>();
	auto scan_function =
	    *function_set.functions.GetFunctionByArguments(context, {LogicalType::LIST(LogicalType::VARCHAR)});
	// read_parquet with the HiveMultiFileReader: Glue's schema and Glue's partition values
	scan_function.get_multi_file_reader = HiveMultiFileReader::CreateInstance;
	scan_function.function_info = scan_info;

	vector<Value> file_paths;
	for (auto &file : scan_info->files) {
		file_paths.emplace_back(file.path);
	}
	named_parameter_map_t param_map;
	vector<LogicalType> return_types;
	vector<Identifier> names;
	TableFunctionRef empty_ref;
	vector<Value> inputs = {Value::LIST(LogicalType::VARCHAR, std::move(file_paths))};
	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, scan_function,
	                                  empty_ref);
	bind_data = scan_function.bind(context, bind_input, return_types, names);
	return scan_function;
}

} // namespace duckdb
