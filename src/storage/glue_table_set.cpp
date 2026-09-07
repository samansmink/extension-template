#include "storage/glue_table_set.hpp"

#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include "glue_types.hpp"
#include "storage/glue_catalog.hpp"
#include "storage/glue_schema_entry.hpp"

namespace duckdb {

GlueTableSet::GlueTableSet(GlueSchemaEntry &schema) : schema(schema), catalog(schema.catalog.Cast<GlueCatalog>()) {
}

unique_ptr<GlueTable> GlueTableSet::CreateTableEntry(const GlueTableInfo &table) {
	CreateTableInfo info(schema, Identifier(table.name));
	for (auto &column : table.columns) {
		info.columns.AddColumn(ColumnDefinition(Identifier(column.name), GlueTypes::ToLogicalType(column.type)));
	}
	// Hive style tables store their partition columns separately, they are regular columns for a scan.
	// Open table formats (Iceberg, Delta) keep the partitioning in their own metadata and have no partition keys.
	for (auto &column : table.partition_keys) {
		info.columns.AddColumn(ColumnDefinition(Identifier(column.name), GlueTypes::ToLogicalType(column.type)));
	}
	return make_uniq<GlueTable>(catalog, schema, info, table);
}

void GlueTableSet::LoadEntries(ClientContext &context) {
	if (is_loaded) {
		return;
	}
	auto tables = GlueAPI::GetTables(context, catalog, schema.database_info.name);
	for (auto &table : tables) {
		if (entries.find(table.name) != entries.end()) {
			// already loaded through a direct lookup
			continue;
		}
		entries.emplace(table.name, CreateTableEntry(table));
	}
	is_loaded = true;
}

GlueTable &GlueTableSet::ResolveEntry(ClientContext &context, GlueTable &entry) {
	if (entry.schema_resolved || entry.table_info.GetFormat() != GlueTableFormat::ICEBERG) {
		return entry;
	}
	// Take the columns from the iceberg extension's entry of this table, so that DuckDB plans scans and DML
	// with exactly the columns the iceberg extension produces and expects
	auto table = entry.table_info;
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(table.name)));
	auto &iceberg_entry = GlueTable::LookupIcebergEntry(context, catalog, schema.name, lookup);

	CreateTableInfo info(schema, Identifier(table.name));
	for (auto &column : iceberg_entry.GetColumns().Logical()) {
		info.columns.AddColumn(ColumnDefinition(column.Name(), column.Type()));
	}
	auto resolved = make_uniq<GlueTable>(catalog, schema, info, std::move(table));
	resolved->schema_resolved = true;
	resolved->virtual_columns = iceberg_entry.GetVirtualColumns();
	resolved->row_id_columns = iceberg_entry.GetRowIdColumns();
	auto &result = *resolved;
	auto name = entry.name.GetIdentifierName();
	entries.erase(name);
	entries.emplace(name, std::move(resolved));
	return result;
}

optional_ptr<CatalogEntry> GlueTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	auto &name = lookup.GetEntryName();
	lock_guard<mutex> guard(entry_lock);
	auto entry = entries.find(name);
	if (entry != entries.end()) {
		return &ResolveEntry(context, *entry->second);
	}
	// not cached, ask Glue for this table directly
	GlueTableInfo table;
	if (!GlueAPI::GetTable(context, catalog, schema.database_info.name, name, table)) {
		return nullptr;
	}
	auto result = entries.emplace(table.name, CreateTableEntry(table));
	return &ResolveEntry(context, *result.first->second);
}

void GlueTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> guard(entry_lock);
	LoadEntries(context);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> GlueTableSet::CreateEntry(unique_ptr<GlueTable> entry) {
	lock_guard<mutex> guard(entry_lock);
	auto name = entry->name.GetIdentifierName();
	entries.erase(name);
	auto result = entries.emplace(name, std::move(entry));
	return result.first->second.get();
}

void GlueTableSet::RemoveEntry(const string &name) {
	lock_guard<mutex> guard(entry_lock);
	entries.erase(name);
}

void GlueTableSet::ClearEntries() {
	lock_guard<mutex> guard(entry_lock);
	entries.clear();
	is_loaded = false;
}

} // namespace duckdb
