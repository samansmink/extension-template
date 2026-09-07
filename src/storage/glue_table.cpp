#include "storage/glue_table.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
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
	auto &iceberg_catalog = glue_catalog.GetIcebergCatalog(context);
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
	default:
		throw NotImplementedException("Scan from table with type %s", latest_info.GetFormatName());
	}
}

} // namespace duckdb
