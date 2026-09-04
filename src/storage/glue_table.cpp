#include "storage/glue_table.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
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

TableFunction GlueTable::GetIcebergScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                const GlueTableInfo &latest_info) {
	auto metadata_location = latest_info.GetMetadataLocation();
	if (metadata_location.empty()) {
		throw InvalidInputException("Iceberg table '%s.%s' has no 'metadata_location' parameter in Glue",
		                            latest_info.database_name, latest_info.name);
	}

	// The iceberg extension provides the actual scan, look it up in the system catalog
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &catalog_schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	auto catalog_entry = catalog_schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, "iceberg_scan");
	if (!catalog_entry) {
		throw MissingExtensionException(
		    "Scanning Iceberg table '%s.%s' requires the iceberg extension, LOAD it and try again",
		    latest_info.database_name, latest_info.name);
	}
	auto &iceberg_scan_function_set = catalog_entry->Cast<TableFunctionCatalogEntry>();
	auto iceberg_scan_function =
	    *iceberg_scan_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});

	named_parameter_map_t param_map;
	vector<LogicalType> return_types;
	vector<Identifier> names;
	TableFunctionRef empty_ref;

	vector<Value> inputs = {Value(metadata_location)};
	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, iceberg_scan_function,
	                                  empty_ref);
	bind_data = iceberg_scan_function.bind(context, bind_input, return_types, names);
	return iceberg_scan_function;
}

} // namespace duckdb
