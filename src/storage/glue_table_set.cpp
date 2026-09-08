#include "storage/glue_table_set.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"

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
	auto entry = make_uniq<GlueTable>(catalog, schema, info, table);
	SetTableTypeTag(*entry);
	return entry;
}

void GlueTableSet::SetTableTypeTag(GlueTable &entry) {
	// exposed through duckdb_tables().tags['table_type'] (ICEBERG / DELTA / HIVE / UNKNOWN)
	entry.tags["table_type"] = GlueTableFormatToString(entry.table_info.GetFormat());
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
		unique_ptr<GlueTable> entry;
		try {
			entry = CreateTableEntry(table);
		} catch (std::exception &ex) {
			// A table whose Glue definition we can not turn into a DuckDB table (e.g. an unsupported column type)
			// must not break listing the other tables: leave it out and log why. Looking the table up by name
			// still reports the error to the user.
			ErrorData error(ex);
			DUCKDB_LOG_ERROR(context, "Glue table '%s.%s' is not listed: %s", schema.database_info.name, table.name,
			                 error.RawMessage());
			continue;
		}
		entries.emplace(table.name, std::move(entry));
	}
	is_loaded = true;
}

GlueTable &GlueTableSet::ResolveEntry(ClientContext &context, GlueTable &entry) {
	if (entry.schema_resolved) {
		return entry;
	}
	auto format = entry.table_info.GetFormat();
	if (format != GlueTableFormat::ICEBERG && format != GlueTableFormat::DELTA) {
		return entry;
	}
	// Open table formats carry their own schema, which is what the scan produces; the Glue columns are lossy
	// (e.g. Iceberg 'timestamptz' is listed as 'timestamp') or even placeholders (Spark registered Delta tables).
	// Rebuild the entry with the format's columns.
	auto table = entry.table_info;
	CreateTableInfo info(schema, Identifier(table.name));
	auto resolved_virtual_columns = entry.GetVirtualColumns();
	auto resolved_row_id_columns = entry.GetRowIdColumns();
	if (format == GlueTableFormat::ICEBERG) {
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(table.name)));
		auto &iceberg_entry = GlueTable::LookupIcebergEntry(context, catalog, schema.name, lookup);
		for (auto &column : iceberg_entry.GetColumns().Logical()) {
			info.columns.AddColumn(ColumnDefinition(column.Name(), column.Type()));
		}
		resolved_virtual_columns = iceberg_entry.GetVirtualColumns();
		resolved_row_id_columns = iceberg_entry.GetRowIdColumns();
	} else {
		unique_ptr<FunctionData> bind_data;
		vector<Identifier> names;
		vector<LogicalType> types;
		GlueTable::BindDeltaScan(context, table, bind_data, names, types);
		for (idx_t i = 0; i < names.size(); i++) {
			info.columns.AddColumn(ColumnDefinition(names[i], types[i]));
		}
	}
	auto resolved = make_uniq<GlueTable>(catalog, schema, info, std::move(table));
	SetTableTypeTag(*resolved);
	resolved->schema_resolved = true;
	resolved->virtual_columns = std::move(resolved_virtual_columns);
	resolved->row_id_columns = std::move(resolved_row_id_columns);
	// the replaced entry may already own a child catalog
	entry.MoveChildrenTo(*resolved);
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

void GlueTableSet::DetachChildren(ClientContext &context) {
	lock_guard<mutex> guard(entry_lock);
	for (auto &entry : entries) {
		entry.second->DetachChildren(context);
	}
}

void GlueTableSet::ClearEntries() {
	lock_guard<mutex> guard(entry_lock);
	entries.clear();
	is_loaded = false;
}

} // namespace duckdb
