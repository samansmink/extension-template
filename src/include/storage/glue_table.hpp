#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "glue_api.hpp"

namespace duckdb {
class GlueCatalog;

//! A table registered in the Glue Data Catalog. Only Hive (Glue native) tables can be scanned and written; tables
//! of other formats (Iceberg, Delta, ...) are listed with the columns Glue reports but can not be read.
class GlueTable : public TableCatalogEntry {
public:
	GlueTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, GlueTableInfo table_info);

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	//! Re-fetch the table definition from Glue
	GlueTableInfo RefreshTableInfo(ClientContext &context) const;

private:
	//! Scan a Hive table with read_parquet over the table location, partition values from the directory names
	TableFunction GetHiveScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                                  const GlueTableInfo &latest_info);
	//! Throw if the columns the scan produces differ from the columns of this entry (which the query was planned
	//! with)
	void VerifyScanColumns(const GlueTableInfo &latest_info, const vector<Identifier> &scan_names,
	                       const vector<LogicalType> &scan_types) const;

public:
	//! The table definition as returned by Glue when the entry was created
	GlueTableInfo table_info;
};

} // namespace duckdb
