#include "storage/glue_catalog.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/storage/database_size.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

#include "glue_api.hpp"
#include "storage/glue_schema_entry.hpp"
#include "storage/glue_table.hpp"

namespace duckdb {

GlueCatalog::GlueCatalog(AttachedDatabase &db_p, AccessMode access_mode, GlueAttachOptions options_p)
    : Catalog(db_p), access_mode(access_mode), options(std::move(options_p)), schemas(*this) {
}

GlueCatalog::~GlueCatalog() {
}

unique_ptr<SecretEntry> GlueCatalog::GetStorageSecret(ClientContext &context, const string &secret_name) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto &secret_manager = context.db->GetSecretManager();

	case_insensitive_set_t accepted_secret_types {"s3", "aws"};

	if (!secret_name.empty()) {
		auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name);
		if (secret_entry) {
			auto secret_type = secret_entry->secret->GetType();
			if (accepted_secret_types.count(secret_type.GetIdentifierName())) {
				return secret_entry;
			}
			throw InvalidConfigurationException(
			    "Found a secret by the name of '%s', but it is not of an accepted type for a 'secret', "
			    "accepted types are: 's3' or 'aws', found '%s'",
			    secret_name, secret_type.GetIdentifierName());
		}
		throw InvalidConfigurationException(
		    "No secret by the name of '%s' could be found, consider changing the 'secret'", secret_name);
	}

	for (auto &type : accepted_secret_types) {
		//! Lookup the default secret for this type
		auto secret_entry = secret_manager.GetSecretByName(transaction, StringUtil::Format("__default_%s", type));
		if (secret_entry) {
			return secret_entry;
		}
		auto secret_match = secret_manager.LookupSecret(transaction, type + "://", type);
		if (secret_match.HasMatch()) {
			return std::move(secret_match.secret_entry);
		}
	}
	throw InvalidConfigurationException("Could not find a valid storage secret (s3 or aws) to connect to Glue with");
}

void GlueCatalog::Initialize(bool load_builtin) {
}

optional<Identifier> GlueCatalog::GetDefaultSchema() const {
	if (options.default_schema.empty()) {
		return nullopt;
	}
	return options.default_schema;
}

//===--------------------------------------------------------------------===//
// Schemas
//===--------------------------------------------------------------------===//
GlueSchemaSet &GlueCatalog::GetSchemas() {
	return schemas;
}

optional_ptr<CatalogEntry> GlueCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto &context = transaction.GetContext();
	auto schema_name = info.SchemaName().GetIdentifierName();

	auto existing = schemas.GetEntry(context, schema_name);
	if (existing) {
		switch (info.on_conflict) {
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return nullptr;
		case OnCreateConflict::ERROR_ON_CONFLICT:
			throw CatalogException("Schema with name \"%s\" already exists in Glue catalog \"%s\"", schema_name,
			                       GetName().GetIdentifierName());
		default:
			throw NotImplementedException("CREATE OR REPLACE SCHEMA is not supported for Glue catalogs");
		}
	}

	GlueDatabaseInfo database;
	database.name = schema_name;
	database.location_uri = GetDatabaseLocation(schema_name);
	GlueAPI::CreateDatabase(context, *this, database);

	// re-fetch so the cached entry reflects what Glue stored
	GlueDatabaseInfo created;
	if (!GlueAPI::GetDatabase(context, *this, schema_name, created)) {
		throw CatalogException("Glue database \"%s\" was created but could not be fetched afterwards", schema_name);
	}
	return schemas.CreateEntry(schemas.CreateSchemaEntry(created));
}

void GlueCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	auto schema_name = info.GetQualifiedName().Name().GetIdentifierName();
	auto existing = schemas.GetEntry(context, schema_name);
	if (!existing) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("Schema with name \"%s\" does not exist in Glue catalog \"%s\"", schema_name,
		                       GetName().GetIdentifierName());
	}
	// NOTE: Glue deletes every table in the database along with it, regardless of CASCADE
	GlueAPI::DeleteDatabase(context, *this, schema_name);
	schemas.RemoveEntry(schema_name);
}

string GlueCatalog::GetDatabaseLocation(const string &database_name) const {
	return options.default_location + "/" + database_name;
}

string GlueCatalog::GetTableLocation(const GlueDatabaseInfo &database, const string &table_name) const {
	if (!database.location_uri.empty()) {
		auto location = database.location_uri;
		StringUtil::RTrim(location, "/");
		return location + "/" + table_name;
	}
	return GetDatabaseLocation(database.name) + "/" + table_name;
}

void GlueCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<GlueSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> GlueCatalog::LookupSchema(CatalogTransaction transaction,
                                                           const EntryLookupInfo &schema_lookup,
                                                           OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	auto &context = transaction.GetContext();
	auto entry = schemas.GetEntry(context, schema_name);
	if (!entry) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw CatalogException(schema_lookup.GetErrorContext(), "Glue database with name \"%s\" does not exist",
		                       schema_name);
	}
	return &entry->Cast<SchemaCatalogEntry>();
}

//===--------------------------------------------------------------------===//
// Child Iceberg catalog
//===--------------------------------------------------------------------===//
Catalog &GlueCatalog::GetIcebergCatalog(ClientContext &context) {
	lock_guard<mutex> guard(child_lock);
	if (iceberg_database) {
		return iceberg_database->GetCatalog();
	}
	auto &db = DatabaseInstance::GetDatabase(context);
	if (!db.ExtensionIsLoaded("iceberg")) {
		ExtensionHelper::TryAutoLoadExtension(db, "iceberg");
	}
	if (!db.ExtensionIsLoaded("iceberg")) {
		throw MissingExtensionException(
		    "Reading and writing Iceberg tables through Glue catalog '%s' requires the iceberg extension, LOAD it "
		    "and try again",
		    GetName().GetIdentifierName());
	}

	// ATTACH '<catalog id>' AS __glue_internal_<uuid> (TYPE ICEBERG, ENDPOINT_TYPE 'GLUE', SECRET '<secret>')
	AttachInfo info;
	info.name = Identifier("__glue_internal_" + UUID::ToString(UUID::GenerateRandomUUID()));
	info.path = options.path;
	info.options = {{"type", Value("iceberg")}, {"endpoint_type", Value("glue")}};
	if (!options.secret_name.empty()) {
		info.options["secret"] = Value(options.secret_name);
	}
	AttachOptions attach_options(context.db->config.options);
	attach_options.access_mode = access_mode;
	attach_options.db_type = "iceberg";

	auto &db_manager = DatabaseManager::Get(context);
	iceberg_database = db_manager.AttachDatabase(context, info, attach_options);
	iceberg_database_name = info.name;
	return iceberg_database->GetCatalog();
}

void GlueCatalog::OnDetach(ClientContext &context) {
	lock_guard<mutex> guard(child_lock);
	if (!iceberg_database) {
		return;
	}
	iceberg_database.reset();
	DatabaseManager::Get(context).DetachDatabase(context, iceberg_database_name, OnEntryNotFound::RETURN_NULL);
}

Catalog &GlueCatalog::GetIcebergCatalogForDML(ClientContext &context, TableCatalogEntry &table) {
	auto &glue_table = table.Cast<GlueTable>();
	if (glue_table.table_info.GetFormat() != GlueTableFormat::ICEBERG) {
		throw NotImplementedException("Writing to Glue table '%s' with type %s is not supported",
		                              table.name.GetIdentifierName(), glue_table.table_info.GetFormatName());
	}
	return GetIcebergCatalog(context);
}

//===--------------------------------------------------------------------===//
// Planning
//===--------------------------------------------------------------------===//
PhysicalOperator &GlueCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                          optional_ptr<PhysicalOperator> plan) {
	auto &iceberg_catalog = GetIcebergCatalogForDML(context, op.table);
	return iceberg_catalog.PlanInsert(context, planner, op, plan);
}

PhysicalOperator &GlueCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("CREATE TABLE AS is not supported for Glue catalogs yet");
}

PhysicalOperator &GlueCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                          PhysicalOperator &plan) {
	return PlanDeleteOperation(context, planner, op, plan);
}

PhysicalOperator &GlueCatalog::PlanDeleteOperation(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalDelete &op, PhysicalOperator &plan) {
	auto &iceberg_catalog = GetIcebergCatalogForDML(context, op.table);
	return iceberg_catalog.PlanDelete(context, planner, op, plan);
}

PhysicalOperator &GlueCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                          PhysicalOperator &plan) {
	auto &iceberg_catalog = GetIcebergCatalogForDML(context, op.table);
	return iceberg_catalog.PlanUpdate(context, planner, op, plan);
}

PhysicalOperator &GlueCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalMergeInto &op, PhysicalOperator &plan) {
	auto &iceberg_catalog = GetIcebergCatalogForDML(context, op.table);
	return iceberg_catalog.PlanMergeInto(context, planner, op, plan);
}

unique_ptr<LogicalOperator> GlueCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                         TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("Indexes are not supported for Glue catalogs");
}

//===--------------------------------------------------------------------===//
// Misc
//===--------------------------------------------------------------------===//
DatabaseSize GlueCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	return size;
}

bool GlueCatalog::InMemory() {
	return false;
}

string GlueCatalog::GetDBPath() {
	return options.path;
}

} // namespace duckdb
