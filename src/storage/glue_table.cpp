#include "storage/glue_table.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

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

void GlueTable::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
                                      ClientContext &context) {
}

GlueTableInfo GlueTable::RefreshTableInfo(ClientContext &context) const {
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	GlueTableInfo result;
	if (!GlueAPI::GetTable(context, glue_catalog, table_info.database_name, table_info.name, result)) {
		throw CatalogException("Glue table '%s.%s' no longer exists", table_info.database_name, table_info.name);
	}
	return result;
}

TableFunction GlueTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	// Ask Glue what kind of table this is right before scanning: for Iceberg tables the metadata location
	// changes with every commit, so the cached table info can not be used.
	auto latest_info = RefreshTableInfo(context);
	auto format = latest_info.GetFormat();
	switch (format) {
	case GlueTableFormat::ICEBERG:
		return GetIcebergScanFunction(context, bind_data, latest_info);
	default:
		throw NotImplementedException("Scan from table with type %s", latest_info.GetFormatName());
	}
}

TableFunction GlueTable::BindIcebergScan(ClientContext &context, const GlueTableInfo &table_info,
                                         unique_ptr<FunctionData> &bind_data, vector<Identifier> &names,
                                         vector<LogicalType> &types) {
	auto metadata_location = table_info.GetMetadataLocation();
	if (metadata_location.empty()) {
		throw InvalidInputException("Iceberg table '%s.%s' has no 'metadata_location' parameter in Glue",
		                            table_info.database_name, table_info.name);
	}

	// The iceberg extension provides the actual scan, look it up in the system catalog
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &catalog_schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	auto catalog_entry = catalog_schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, "iceberg_scan");
	if (!catalog_entry) {
		throw MissingExtensionException(
		    "Reading Iceberg table '%s.%s' requires the iceberg extension, LOAD it and try again",
		    table_info.database_name, table_info.name);
	}
	auto &iceberg_scan_function_set = catalog_entry->Cast<TableFunctionCatalogEntry>();
	auto iceberg_scan_function =
	    *iceberg_scan_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});

	named_parameter_map_t param_map;
	TableFunctionRef empty_ref;
	vector<Value> inputs = {Value(metadata_location)};
	TableFunctionBindInput bind_input(inputs, param_map, types, names, nullptr, nullptr, iceberg_scan_function,
	                                  empty_ref);
	bind_data = iceberg_scan_function.bind(context, bind_input, types, names);
	return iceberg_scan_function;
}

TableFunction GlueTable::GetIcebergScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                const GlueTableInfo &latest_info) {
	vector<LogicalType> return_types;
	vector<Identifier> names;
	auto iceberg_scan_function = BindIcebergScan(context, latest_info, bind_data, names, return_types);

	// The binder plans the scan with the columns of this catalog entry, while the data is produced with the
	// columns of the (current) Iceberg schema. Make sure the two agree before handing out the scan.
	VerifyScanColumns(latest_info, names, return_types);
	return iceberg_scan_function;
}

void GlueTable::VerifyScanColumns(const GlueTableInfo &latest_info, const vector<Identifier> &scan_names,
                                  const vector<LogicalType> &scan_types) const {
	auto &table_columns = GetColumns();
	if (table_columns.PhysicalColumnCount() != scan_names.size()) {
		throw BinderException("The catalog entry of table '%s.%s' has %d columns but its current Iceberg schema has "
		                      "%d columns, the table changed since it was loaded, re-attach the catalog to pick up "
		                      "the new schema",
		                      latest_info.database_name, latest_info.name, table_columns.PhysicalColumnCount(),
		                      scan_names.size());
	}
	idx_t i = 0;
	for (auto &column : table_columns.Physical()) {
		auto &scan_name = scan_names[i].GetIdentifierName();
		auto &scan_type = scan_types[i];
		i++;
		if (!StringUtil::CIEquals(column.Name().GetIdentifierName(), scan_name)) {
			throw BinderException(
			    "Column %d of Glue table '%s.%s' is named '%s' in the catalog entry but '%s' in the current Iceberg "
			    "schema, the table changed since it was loaded, re-attach the catalog to pick up the new schema",
			    i, latest_info.database_name, latest_info.name, column.Name().GetIdentifierName(), scan_name);
		}
		if (column.Type() != scan_type) {
			throw BinderException(
			    "Column '%s' of Glue table '%s.%s' has type %s in the catalog entry but %s in the current Iceberg "
			    "schema, the table changed since it was loaded, re-attach the catalog to pick up the new schema",
			    scan_name, latest_info.database_name, latest_info.name, column.Type().ToString(), scan_type.ToString());
		}
	}
}

} // namespace duckdb
