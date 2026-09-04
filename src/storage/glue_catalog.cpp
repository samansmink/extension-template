#include "storage/glue_catalog.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/storage/database_size.hpp"

#include "glue_api.hpp"
#include "storage/glue_schema_entry.hpp"

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
	throw NotImplementedException("GlueCatalog::CreateSchema");
}

void GlueCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("GlueCatalog::DropSchema");
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
// Planning
//===--------------------------------------------------------------------===//
PhysicalOperator &GlueCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                          optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("GlueCatalog::PlanInsert");
}

PhysicalOperator &GlueCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("GlueCatalog::PlanCreateTableAs");
}

PhysicalOperator &GlueCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                          PhysicalOperator &plan) {
	return PlanDeleteOperation(context, planner, op, plan);
}

PhysicalOperator &GlueCatalog::PlanDeleteOperation(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalDelete &op, PhysicalOperator &plan) {
	throw NotImplementedException("GlueCatalog::PlanDelete");
}

PhysicalOperator &GlueCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                          PhysicalOperator &plan) {
	throw NotImplementedException("GlueCatalog::PlanUpdate");
}

PhysicalOperator &GlueCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalMergeInto &op, PhysicalOperator &plan) {
	throw NotImplementedException("GlueCatalog::PlanMergeInto");
}

unique_ptr<LogicalOperator> GlueCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                         TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("GlueCatalog::BindCreateIndex");
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
