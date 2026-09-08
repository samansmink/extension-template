#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "duckdb/common/mutex.hpp"

#include "glue_api.hpp"

namespace duckdb {
class GlueCatalog;

//! A table registered in the Glue Data Catalog. The table can be of any format (Iceberg, Delta, Hive, ...).
//! Iceberg tables are proxied to the entry of the hidden child Iceberg catalog (see GlueCatalog::GetIcebergCatalog):
//! scans, virtual columns, row ids and DML all come from there, so the iceberg extension handles them natively.
class GlueTable : public TableCatalogEntry {
public:
	GlueTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, GlueTableInfo table_info);

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;
	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

	//! Look up the entry of this table in the child Iceberg catalog, throws if the table is not an Iceberg table
	TableCatalogEntry &GetIcebergEntry(ClientContext &context, const EntryLookupInfo &lookup);
	//! DML on a Delta table is executed by the delta extension: the table root is attached (once, hidden) as a
	//! single-table Delta catalog in child catalog mode and DML planning is forwarded to it. Throws if the table is
	//! not a Delta table.
	Catalog &GetDeltaCatalog(ClientContext &context);
	//! Detach the child Delta catalog, if any
	void DetachChildren(ClientContext &context);
	//! Hand the child Delta catalog over to a replacement entry (see GlueTableSet::ResolveEntry)
	void MoveChildrenTo(GlueTable &other);

private:
	//! Scan a Delta table with the delta extension's delta_scan over the table location
	TableFunction GetDeltaScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                                   const GlueTableInfo &latest_info);
	//! Scan a Hive table with read_parquet over the table location, partition values from the directory names
	TableFunction GetHiveScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                                  const GlueTableInfo &latest_info);
	//! Throw if the columns the scan produces differ from the columns of this entry (which the query was planned
	//! with)
	void VerifyScanColumns(const GlueTableInfo &latest_info, const vector<Identifier> &scan_names,
	                       const vector<LogicalType> &scan_types) const;

public:
	//! Look up a table in the child Iceberg catalog by name
	static TableCatalogEntry &LookupIcebergEntry(ClientContext &context, GlueCatalog &glue_catalog,
	                                             const Identifier &schema_name, const EntryLookupInfo &lookup);
	//! Bind the delta extension's delta_scan on the table location, returning the columns of the Delta log
	static TableFunction BindDeltaScan(ClientContext &context, const GlueTableInfo &table_info,
	                                   unique_ptr<FunctionData> &bind_data, vector<Identifier> &names,
	                                   vector<LogicalType> &types);

public:
	//! The table definition as returned by Glue when the entry was created
	GlueTableInfo table_info;
	//! Whether the columns (and virtual / row id columns) of this entry were taken from the child Iceberg entry
	//! rather than from the (lossy) Glue column definitions
	bool schema_resolved = false;
	//! Virtual and row id columns of the child Iceberg entry, copied when the schema was resolved
	virtual_column_map_t virtual_columns;
	vector<column_t> row_id_columns;

private:
	mutex delta_lock;
	//! The hidden child Delta catalog of this table (see GetDeltaCatalog)
	shared_ptr<AttachedDatabase> delta_database;
};

} // namespace duckdb
