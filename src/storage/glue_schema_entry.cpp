#include "storage/glue_schema_entry.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "storage/glue_catalog.hpp"

namespace duckdb {

GlueSchemaEntry::GlueSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, GlueDatabaseInfo database_info_p)
    : SchemaCatalogEntry(catalog, info), database_info(std::move(database_info_p)), tables(*this) {
}

GlueSchemaEntry::~GlueSchemaEntry() {
}

bool GlueSchemaEntry::CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return true;
	default:
		return false;
	}
}

//===--------------------------------------------------------------------===//
// Create / Drop / Alter
//===--------------------------------------------------------------------===//
optional_ptr<CatalogEntry> GlueSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	throw NotImplementedException("GlueSchemaEntry::CreateTable");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                           CreateFunctionInfo &info) {
	throw BinderException("Glue databases do not support creating functions");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                        TableCatalogEntry &table) {
	throw BinderException("Glue databases do not support creating indexes");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("Glue databases do not support creating views (yet)");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                           CreateSequenceInfo &info) {
	throw BinderException("Glue databases do not support creating sequences");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                CreateTableFunctionInfo &info) {
	throw BinderException("Glue databases do not support creating table functions");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                               CreateCopyFunctionInfo &info) {
	throw BinderException("Glue databases do not support creating copy functions");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                 CreatePragmaFunctionInfo &info) {
	throw BinderException("Glue databases do not support creating pragma functions");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                            CreateCollationInfo &info) {
	throw BinderException("Glue databases do not support creating collations");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("Glue databases do not support creating types");
}

void GlueSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("GlueSchemaEntry::Alter");
}

void GlueSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("GlueSchemaEntry::DropEntry");
}

//===--------------------------------------------------------------------===//
// Scan / Lookup
//===--------------------------------------------------------------------===//
void GlueSchemaEntry::Scan(ClientContext &context, CatalogType type,
                           const std::function<void(CatalogEntry &)> &callback) {
	if (!CatalogTypeIsSupported(type)) {
		return;
	}
	tables.Scan(context, callback);
}

void GlueSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported for Glue databases");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                        const EntryLookupInfo &lookup_info) {
	if (!CatalogTypeIsSupported(lookup_info.GetCatalogType())) {
		return nullptr;
	}
	auto &context = transaction.GetContext();
	return tables.GetEntry(context, lookup_info);
}

} // namespace duckdb
