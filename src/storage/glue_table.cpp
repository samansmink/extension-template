#include "storage/glue_table.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "glue_types.hpp"
#include "storage/glue_catalog.hpp"
#include "storage/glue_schema_entry.hpp"

namespace duckdb {

GlueTable::GlueTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, GlueTableInfo table_info_p)
    : TableCatalogEntry(catalog, schema, info), table_info(std::move(table_info_p)) {
	this->internal = false;
}

unique_ptr<BaseStatistics> GlueTable::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

TableStorageInfo GlueTable::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	return result;
}

virtual_column_map_t GlueTable::GetVirtualColumns() const {
	if (schema_resolved) {
		return virtual_columns;
	}
	return TableCatalogEntry::GetVirtualColumns();
}

vector<column_t> GlueTable::GetRowIdColumns() const {
	if (schema_resolved) {
		return row_id_columns;
	}
	return TableCatalogEntry::GetRowIdColumns();
}

void GlueTable::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
                                      ClientContext &context) {
	if (table_info.GetFormat() != GlueTableFormat::ICEBERG) {
		return;
	}
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(name));
	GetIcebergEntry(context, lookup).BindUpdateConstraints(binder, get, proj, update, context);
}

//===--------------------------------------------------------------------===//
// Child Iceberg entry
//===--------------------------------------------------------------------===//
TableCatalogEntry &GlueTable::LookupIcebergEntry(ClientContext &context, GlueCatalog &glue_catalog,
                                                 const Identifier &schema_name, const EntryLookupInfo &lookup) {
	auto &iceberg_catalog = glue_catalog.GetIcebergCatalog();
	auto &iceberg_schema = iceberg_catalog.GetSchema(context, schema_name);
	auto entry = iceberg_schema.LookupEntry(iceberg_catalog.GetCatalogTransaction(context), lookup);
	if (!entry) {
		throw CatalogException("Table \"%s.%s\" is registered in Glue but does not exist in the Iceberg catalog",
		                       schema_name.GetIdentifierName(), lookup.GetEntryName());
	}
	return entry->Cast<TableCatalogEntry>();
}

TableCatalogEntry &GlueTable::GetIcebergEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	if (table_info.GetFormat() != GlueTableFormat::ICEBERG) {
		throw InternalException("GetIcebergEntry called on Glue table '%s' with type %s", name.GetIdentifierName(),
		                        table_info.GetFormatName());
	}
	return LookupIcebergEntry(context, catalog.Cast<GlueCatalog>(), schema.name, lookup);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
TableFunction GlueTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(name));
	return GetScanFunction(context, bind_data, lookup);
}

TableFunction GlueTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                         const EntryLookupInfo &lookup) {
	// Ask Glue what kind of table this is right before scanning, the format decides who produces the scan
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	GlueTableInfo latest_info;
	if (!GlueAPI::GetTable(context, glue_catalog, table_info.database_name, table_info.name, latest_info)) {
		throw CatalogException("Glue table '%s.%s' no longer exists", table_info.database_name, table_info.name);
	}
	switch (latest_info.GetFormat()) {
	case GlueTableFormat::ICEBERG:
		// The iceberg extension scans its own entry (time travel through the lookup's AT clause included)
		return GetIcebergEntry(context, lookup).GetScanFunction(context, bind_data, lookup);
	case GlueTableFormat::HIVE:
		return GetHiveScanFunction(context, bind_data, latest_info);
	default:
		throw NotImplementedException("Scan from table with type %s", latest_info.GetFormatName());
	}
}

//===--------------------------------------------------------------------===//
// Hive scan
//===--------------------------------------------------------------------===//
TableFunction GlueTable::GetHiveScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                             const GlueTableInfo &latest_info) {
	// The SerDe decides the file format. Only parquet is supported so far.
	auto serde = StringUtil::Lower(latest_info.serde_library);
	if (!StringUtil::Contains(serde, "parquet")) {
		throw NotImplementedException("Reading Hive table '%s.%s' with SerDe '%s' is not supported yet, only "
		                              "parquet tables (ParquetHiveSerDe) can be read",
		                              latest_info.database_name, latest_info.name, latest_info.serde_library);
	}
	if (latest_info.location.empty()) {
		throw InvalidInputException("Hive table '%s.%s' has no location in Glue", latest_info.database_name,
		                            latest_info.name);
	}

	// read_parquet('<location>/<key=value>/.../*', hive_partitioning = true, hive_types = {...}).
	// Assumes the table location only holds the table's own data, laid out as <key=value> directories.
	auto location = latest_info.location;
	StringUtil::RTrim(location, "/");
	string glob = location;
	child_list_t<Value> hive_types;
	for (auto &key : latest_info.partition_keys) {
		glob += "/*";
		hive_types.emplace_back(key.name, Value(GlueTypes::ToLogicalType(key.type).ToString()));
	}
	glob += "/*";

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
	auto scan_function = *function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});

	named_parameter_map_t param_map;
	if (!hive_types.empty()) {
		param_map["hive_partitioning"] = Value::BOOLEAN(true);
		// the partition types come from Glue, do not sniff them from the values
		param_map["hive_types"] = Value::STRUCT(std::move(hive_types));
		param_map["hive_types_autocast"] = Value::BOOLEAN(false);
	}
	vector<LogicalType> return_types;
	vector<Identifier> names;
	TableFunctionRef empty_ref;
	vector<Value> inputs = {Value(glob)};
	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, scan_function,
	                                  empty_ref);
	bind_data = scan_function.bind(context, bind_input, return_types, names);

	VerifyScanColumns(latest_info, names, return_types);
	return scan_function;
}

void GlueTable::VerifyScanColumns(const GlueTableInfo &latest_info, const vector<Identifier> &scan_names,
                                  const vector<LogicalType> &scan_types) const {
	auto &table_columns = GetColumns();
	if (table_columns.PhysicalColumnCount() != scan_names.size()) {
		throw BinderException("Glue lists %d columns for table '%s.%s' but its data files have %d columns, the "
		                      "Glue table definition is out of sync with the data",
		                      table_columns.PhysicalColumnCount(), latest_info.database_name, latest_info.name,
		                      scan_names.size());
	}
	idx_t i = 0;
	for (auto &column : table_columns.Physical()) {
		auto &scan_name = scan_names[i].GetIdentifierName();
		auto &scan_type = scan_types[i];
		i++;
		if (!StringUtil::CIEquals(column.Name().GetIdentifierName(), scan_name)) {
			throw BinderException("Column %d of Glue table '%s.%s' is named '%s' in Glue but '%s' in the data "
			                      "files, the Glue table definition is out of sync with the data",
			                      i, latest_info.database_name, latest_info.name, column.Name().GetIdentifierName(),
			                      scan_name);
		}
		if (column.Type() != scan_type) {
			throw BinderException("Column '%s' of Glue table '%s.%s' has type %s in Glue but %s in the data files, "
			                      "the Glue table definition is out of sync with the data",
			                      scan_name, latest_info.database_name, latest_info.name, column.Type().ToString(),
			                      scan_type.ToString());
		}
	}
}

} // namespace duckdb
