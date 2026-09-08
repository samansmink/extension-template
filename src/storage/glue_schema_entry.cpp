#include "storage/glue_schema_entry.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "glue_types.hpp"
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
namespace {

enum class GlueCreateTableType { ICEBERG, HIVE };

//! Options accepted in CREATE TABLE ... WITH (...) for Glue tables
struct GlueCreateTableOptions {
	//! New tables default to the Iceberg format
	GlueCreateTableType type = GlueCreateTableType::ICEBERG;
	//! Optional explicit S3 location of the table
	string location;
};

GlueCreateTableOptions ParseCreateTableOptions(ClientContext &context, const CreateTableInfo &create_info) {
	GlueCreateTableOptions result;
	if (create_info.options.empty()) {
		return result;
	}
	auto binder = Binder::CreateBinder(context);
	TableFunctionBinder option_binder(*binder, context, "CREATE TABLE options");
	for (auto &option : create_info.options) {
		auto &key = option.first;
		auto expr_copy = option.second->Copy();
		auto bound_expr = option_binder.Bind(expr_copy);
		if (bound_expr->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		auto value = ExpressionExecutor::EvaluateScalar(context, *bound_expr, true);
		if (value.IsNull()) {
			throw BinderException("NULL is not a valid value for CREATE TABLE option '%s'", key);
		}
		auto string_value = value.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();

		if (StringUtil::CIEquals(key, "type")) {
			auto type = StringUtil::Upper(string_value);
			if (type == "ICEBERG") {
				result.type = GlueCreateTableType::ICEBERG;
			} else if (type == "HIVE") {
				result.type = GlueCreateTableType::HIVE;
			} else {
				throw BinderException("Unknown Glue table type '%s' for option 'type', expected 'ICEBERG' or 'HIVE'",
				                      string_value);
			}
		} else if (StringUtil::CIEquals(key, "location")) {
			result.location = string_value;
			StringUtil::RTrim(result.location, "/");
		} else {
			throw BinderException("Unknown CREATE TABLE option '%s' for Glue tables, supported options are 'type' "
			                      "(ICEBERG or HIVE) and 'location'",
			                      key);
		}
	}
	return result;
}

} // namespace

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &context = transaction.GetContext();
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	auto &base = info.Base();
	auto table_name = base.GetTableName().GetIdentifierName();

	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(table_name)));
	auto existing = tables.GetEntry(context, lookup);
	if (existing) {
		switch (base.on_conflict) {
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return nullptr;
		case OnCreateConflict::ERROR_ON_CONFLICT:
			throw CatalogException("Table with name \"%s\" already exists in Glue database \"%s\"", table_name,
			                       database_info.name);
		default:
			throw NotImplementedException(
			    "CREATE OR REPLACE TABLE is not supported for Glue catalogs, use separate DROP and CREATE statements");
		}
	}
	if (!base.constraints.empty()) {
		throw NotImplementedException("Constraints are not supported when creating tables in a Glue catalog");
	}
	if (!base.partition_keys.empty()) {
		throw NotImplementedException("PARTITIONED BY is not supported yet when creating tables in a Glue catalog");
	}
	auto options = ParseCreateTableOptions(context, base);

	GlueTableInfo table;
	table.name = table_name;
	table.database_name = database_info.name;
	table.location =
	    options.location.empty() ? glue_catalog.GetTableLocation(database_info, table_name) : options.location;
	for (auto &column : base.columns.Physical()) {
		GlueColumn glue_column;
		glue_column.name = column.Name().GetIdentifierName();
		glue_column.type = GlueTypes::FromLogicalType(column.Type());
		table.columns.push_back(std::move(glue_column));
	}
	switch (options.type) {
	case GlueCreateTableType::ICEBERG:
		GlueAPI::CreateIcebergTable(context, glue_catalog, table);
		break;
	case GlueCreateTableType::HIVE:
		GlueAPI::CreateHiveTable(context, glue_catalog, table);
		break;
	}

	// re-fetch so the entry carries the parameters (e.g. the Iceberg metadata location) Glue assigned
	GlueTableInfo created;
	if (!GlueAPI::GetTable(context, glue_catalog, database_info.name, table_name, created)) {
		throw CatalogException("Glue table \"%s.%s\" was created but could not be fetched afterwards",
		                       database_info.name, table_name);
	}
	return tables.CreateEntry(tables.CreateTableEntry(created));
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("Glue databases do not support creating functions");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                        TableCatalogEntry &table) {
	throw BinderException("Glue databases do not support creating indexes");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("Glue databases do not support creating views (yet)");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
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

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw BinderException("Glue databases do not support creating collations");
}

optional_ptr<CatalogEntry> GlueSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("Glue databases do not support creating types");
}

void GlueSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("GlueSchemaEntry::Alter");
}

void GlueSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (!CatalogTypeIsSupported(info.type)) {
		throw NotImplementedException("Glue databases only support dropping tables");
	}
	auto &glue_catalog = catalog.Cast<GlueCatalog>();
	auto table_name = info.GetQualifiedName().Name().GetIdentifierName();

	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(table_name)));
	auto existing = tables.GetEntry(context, lookup);
	if (!existing) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("Table with name \"%s\" does not exist in Glue database \"%s\"", table_name,
		                       database_info.name);
	}
	GlueAPI::DeleteTable(context, glue_catalog, database_info.name, table_name);
	tables.RemoveEntry(table_name);
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
