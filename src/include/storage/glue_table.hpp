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

	//! Bind the iceberg extension's 'iceberg_scan' on an Iceberg metadata file, returning the scan function and the
	//! columns it produces. Throws if the iceberg extension is not loaded.
	static TableFunction BindIcebergScan(ClientContext &context, const GlueTableInfo &table_info,
	                                     unique_ptr<FunctionData> &bind_data, vector<Identifier> &names,
	                                     vector<LogicalType> &types);

private:
	//! Throw if the columns Glue reports differ from the columns the scan produces
	void VerifyScanColumns(const GlueTableInfo &latest_info, const vector<Identifier> &scan_names,
	                       const vector<LogicalType> &scan_types) const;
	TableFunction GetIcebergScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                                     const GlueTableInfo &latest_info);

public:
	//! The table definition as returned by Glue when the entry was created
	GlueTableInfo table_info;
	//! Whether the columns of this entry were taken from the table format's own schema (Iceberg metadata) rather
	//! than from the (lossy) Glue column definitions
	bool schema_resolved = false;
};

} // namespace duckdb
