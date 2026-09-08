#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

#include "glue_api.hpp"
#include "storage/glue_table.hpp"

namespace duckdb {
class GlueCatalog;
class GlueSchemaEntry;

//! The set of tables of a single Glue database, lazily loaded from Glue
class GlueTableSet {
public:
	explicit GlueTableSet(GlueSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	//! Insert a (new) table entry into the set, replacing any existing entry with the same name
	optional_ptr<CatalogEntry> CreateEntry(unique_ptr<GlueTable> entry);
	void RemoveEntry(const string &name);
	void ClearEntries();

	//! Build a table catalog entry from a Glue table definition, using the (lossy) Glue column definitions
	unique_ptr<GlueTable> CreateTableEntry(const GlueTableInfo &table);

private:
	void LoadEntries(ClientContext &context);
	static void SetTableTypeTag(GlueTable &entry);
	//! For open table formats the Glue column definitions are lossy (e.g. Iceberg 'timestamptz' is listed as
	//! 'timestamp'). Before an entry is used to plan a query, rebuild it with the columns of the table format's own
	//! schema (the child Iceberg catalog's entry). Entries produced by a listing (Scan) keep the Glue columns to
	//! avoid a round trip per table.
	GlueTable &ResolveEntry(ClientContext &context, GlueTable &entry);

private:
	GlueSchemaEntry &schema;
	GlueCatalog &catalog;
	mutex entry_lock;
	case_insensitive_map_t<unique_ptr<GlueTable>> entries;
	bool is_loaded = false;
};

} // namespace duckdb
