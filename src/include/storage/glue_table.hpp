#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "glue_api.hpp"

namespace duckdb {

//! A table registered in the Glue Data Catalog. The table can be of any format (Iceberg, Delta, Hive, ...),
//! scanning is dispatched based on the format when the scan function is requested.
class GlueTable : public TableCatalogEntry {
public:
	GlueTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, GlueTableInfo table_info);

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

	//! Re-fetch the table definition from Glue. The Iceberg 'metadata_location' changes with every commit, so a
	//! scan can not rely on the (cached) table info the entry was created from.
	GlueTableInfo RefreshTableInfo(ClientContext &context) const;

private:
	TableFunction GetIcebergScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                                     const GlueTableInfo &latest_info);

public:
	//! The table definition as returned by Glue when the entry was created
	GlueTableInfo table_info;
};

} // namespace duckdb
