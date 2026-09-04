#include "glue_api.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "storage/glue_catalog.hpp"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/glue/GlueClient.h>
#include <aws/glue/GlueErrors.h>
#include <aws/glue/model/GetDatabaseRequest.h>
#include <aws/glue/model/GetDatabasesRequest.h>
#include <aws/glue/model/GetTableRequest.h>
#include <aws/glue/model/GetTablesRequest.h>
#include <aws/glue/model/CreateDatabaseRequest.h>
#include <aws/glue/model/CreateTableRequest.h>
#include <aws/glue/model/DeleteDatabaseRequest.h>
#include <aws/glue/model/DeleteTableRequest.h>

#include <sys/stat.h>

namespace duckdb {

//===--------------------------------------------------------------------===//
// GlueTableInfo
//===--------------------------------------------------------------------===//
string GlueTableFormatToString(GlueTableFormat format) {
	switch (format) {
	case GlueTableFormat::ICEBERG:
		return "ICEBERG";
	case GlueTableFormat::DELTA:
		return "DELTA";
	case GlueTableFormat::HUDI:
		return "HUDI";
	case GlueTableFormat::HIVE:
		return "HIVE";
	default:
		return "UNKNOWN";
	}
}

string GlueTableInfo::GetParameter(const string &key) const {
	for (auto &entry : parameters) {
		if (StringUtil::CIEquals(entry.first, key)) {
			return entry.second;
		}
	}
	return string();
}

GlueTableFormat GlueTableInfo::GetFormat() const {
	// Open table formats register themselves through the 'table_type' parameter
	auto table_type = StringUtil::Upper(GetParameter("table_type"));
	if (table_type == "ICEBERG") {
		return GlueTableFormat::ICEBERG;
	}
	if (table_type == "DELTA") {
		return GlueTableFormat::DELTA;
	}
	if (table_type == "HUDI") {
		return GlueTableFormat::HUDI;
	}
	// Spark registers Delta tables through the data source provider
	auto provider = StringUtil::Lower(GetParameter("spark.sql.sources.provider"));
	if (provider == "delta") {
		return GlueTableFormat::DELTA;
	}
	if (provider == "iceberg") {
		return GlueTableFormat::ICEBERG;
	}
	if (provider == "hudi") {
		return GlueTableFormat::HUDI;
	}
	if (!GetMetadataLocation().empty()) {
		return GlueTableFormat::ICEBERG;
	}
	if (!input_format.empty() || !location.empty()) {
		// Regular (Hive style) table with a storage descriptor
		return GlueTableFormat::HIVE;
	}
	return GlueTableFormat::UNKNOWN;
}

string GlueTableInfo::GetFormatName() const {
	auto format = GetFormat();
	if (format == GlueTableFormat::HIVE) {
		return StringUtil::Format("HIVE (input format '%s')", input_format);
	}
	if (format == GlueTableFormat::UNKNOWN && !glue_table_type.empty()) {
		return StringUtil::Format("UNKNOWN (glue table type '%s')", glue_table_type);
	}
	return GlueTableFormatToString(format);
}

string GlueTableInfo::GetMetadataLocation() const {
	return GetParameter("metadata_location");
}

//===--------------------------------------------------------------------===//
// Client creation
//===--------------------------------------------------------------------===//
namespace {

void InitAWSAPI() {
	static bool loaded = false;
	if (!loaded) {
		Aws::SDKOptions options;
		Aws::InitAPI(options); // Should only be called once.
		loaded = true;
	}
}

//! Grab the first path that exists, from a list of well-known CA bundle locations
string SelectCURLCertPath() {
	static const char *cert_file_locations[] = {// Arch, Debian-based, Gentoo
	                                            "/etc/ssl/certs/ca-certificates.crt",
	                                            // RedHat 7 based
	                                            "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
	                                            // Redhat 6 based
	                                            "/etc/pki/tls/certs/ca-bundle.crt",
	                                            // OpenSUSE
	                                            "/etc/ssl/ca-bundle.pem",
	                                            // Alpine
	                                            "/etc/ssl/cert.pem"};
	for (auto &ca_file : cert_file_locations) {
		struct stat buf;
		if (stat(ca_file, &buf) == 0) {
			return ca_file;
		}
	}
	return string();
}

const string &GetCURLCertPath() {
	static string cert_path = SelectCURLCertPath();
	return cert_path;
}

string ToStdString(const Aws::String &input) {
	return string(input.c_str(), input.size());
}

unordered_map<string, string> ToStdMap(const Aws::Map<Aws::String, Aws::String> &input) {
	unordered_map<string, string> result;
	for (auto &entry : input) {
		result.emplace(ToStdString(entry.first), ToStdString(entry.second));
	}
	return result;
}

vector<GlueColumn> ToColumns(const Aws::Vector<Aws::Glue::Model::Column> &input) {
	vector<GlueColumn> result;
	for (auto &column : input) {
		GlueColumn glue_column;
		glue_column.name = ToStdString(column.GetName());
		glue_column.type = ToStdString(column.GetType());
		glue_column.comment = ToStdString(column.GetComment());
		result.push_back(std::move(glue_column));
	}
	return result;
}

GlueDatabaseInfo ToDatabaseInfo(const Aws::Glue::Model::Database &database) {
	GlueDatabaseInfo result;
	result.name = ToStdString(database.GetName());
	result.description = ToStdString(database.GetDescription());
	result.location_uri = ToStdString(database.GetLocationUri());
	result.parameters = ToStdMap(database.GetParameters());
	return result;
}

GlueTableInfo ToTableInfo(const Aws::Glue::Model::Table &table) {
	GlueTableInfo result;
	result.name = ToStdString(table.GetName());
	result.database_name = ToStdString(table.GetDatabaseName());
	result.glue_table_type = ToStdString(table.GetTableType());
	auto &storage_descriptor = table.GetStorageDescriptor();
	result.location = ToStdString(storage_descriptor.GetLocation());
	result.input_format = ToStdString(storage_descriptor.GetInputFormat());
	result.output_format = ToStdString(storage_descriptor.GetOutputFormat());
	result.columns = ToColumns(storage_descriptor.GetColumns());
	result.partition_keys = ToColumns(table.GetPartitionKeys());
	result.parameters = ToStdMap(table.GetParameters());
	return result;
}

template <class OUTCOME>
[[noreturn]] void ThrowGlueError(const OUTCOME &outcome, const string &operation) {
	auto &error = outcome.GetError();
	throw IOException("Glue %s failed: %s (%s)", operation, ToStdString(error.GetMessage()),
	                  ToStdString(error.GetExceptionName()));
}

template <class OUTCOME>
bool IsEntityNotFound(const OUTCOME &outcome) {
	return outcome.GetError().GetErrorType() == Aws::Glue::GlueErrors::ENTITY_NOT_FOUND;
}

template <class REQUEST>
void SetCatalogId(REQUEST &request, const GlueCatalog &catalog) {
	if (!catalog.options.catalog_id.empty()) {
		request.SetCatalogId(catalog.options.catalog_id);
	}
}

} // namespace

std::shared_ptr<Aws::Glue::GlueClient> GlueAPI::GetClient(ClientContext &context, GlueCatalog &catalog) {
	InitAWSAPI();

	// Take the credentials from the DuckDB secret. The secret is looked up on every call so a refreshed
	// (credential_chain / sts) secret is picked up automatically.
	auto secret_entry = GlueCatalog::GetStorageSecret(context, catalog.options.secret_name);
	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
	auto key_id_val = kv_secret.TryGetValue("key_id");
	auto secret_val = kv_secret.TryGetValue("secret");
	auto session_token_val = kv_secret.TryGetValue("session_token");
	string key_id = key_id_val.IsNull() ? "" : key_id_val.GetValue<string>();
	string secret = secret_val.IsNull() ? "" : secret_val.GetValue<string>();
	string session_token = session_token_val.IsNull() ? "" : session_token_val.GetValue<string>();
	if (key_id.empty() || secret.empty()) {
		throw InvalidConfigurationException(
		    "Secret '%s' does not contain AWS credentials (key_id / secret), can not connect to Glue catalog '%s'",
		    secret_entry->secret->GetName().GetIdentifierName(), catalog.options.name);
	}

	auto &region = catalog.options.region;
	auto cache_key = key_id + "\x1f" + session_token + "\x1f" + region;

	lock_guard<mutex> guard(catalog.client_lock);
	if (catalog.glue_client && catalog.client_cache_key == cache_key) {
		return catalog.glue_client;
	}

	Aws::Glue::GlueClientConfiguration config;
	config.region = region;
	auto &cert_path = GetCURLCertPath();
	if (!cert_path.empty()) {
		config.caFile = cert_path;
	}
	auto &db_config = DBConfig::GetConfig(context);
	config.userAgent = db_config.UserAgent();

	Aws::Auth::AWSCredentials credentials(key_id, secret, session_token);
	auto provider = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(credentials);
	catalog.glue_client = std::make_shared<Aws::Glue::GlueClient>(provider, nullptr, config);
	catalog.client_cache_key = cache_key;
	return catalog.glue_client;
}

//===--------------------------------------------------------------------===//
// Read API
//===--------------------------------------------------------------------===//
void GlueAPI::VerifyConnection(ClientContext &context, GlueCatalog &catalog) {
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::GetDatabasesRequest request;
	SetCatalogId(request, catalog);
	request.SetMaxResults(1);
	auto outcome = client->GetDatabases(request);
	if (!outcome.IsSuccess()) {
		ThrowGlueError(outcome, StringUtil::Format("GetDatabases (catalog '%s', region '%s')", catalog.options.path,
		                                           catalog.options.region));
	}
}

vector<GlueDatabaseInfo> GlueAPI::GetDatabases(ClientContext &context, GlueCatalog &catalog) {
	auto client = GetClient(context, catalog);
	vector<GlueDatabaseInfo> result;
	Aws::String next_token;
	do {
		Aws::Glue::Model::GetDatabasesRequest request;
		SetCatalogId(request, catalog);
		if (!next_token.empty()) {
			request.SetNextToken(next_token);
		}
		auto outcome = client->GetDatabases(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, "GetDatabases");
		}
		auto &databases = outcome.GetResult();
		for (auto &database : databases.GetDatabaseList()) {
			result.push_back(ToDatabaseInfo(database));
		}
		next_token = databases.GetNextToken();
	} while (!next_token.empty());
	return result;
}

bool GlueAPI::GetDatabase(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                          GlueDatabaseInfo &result) {
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::GetDatabaseRequest request;
	SetCatalogId(request, catalog);
	request.SetName(database_name);
	auto outcome = client->GetDatabase(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			return false;
		}
		ThrowGlueError(outcome, StringUtil::Format("GetDatabase '%s'", database_name));
	}
	result = ToDatabaseInfo(outcome.GetResult().GetDatabase());
	return true;
}

vector<GlueTableInfo> GlueAPI::GetTables(ClientContext &context, GlueCatalog &catalog, const string &database_name) {
	auto client = GetClient(context, catalog);
	vector<GlueTableInfo> result;
	Aws::String next_token;
	do {
		Aws::Glue::Model::GetTablesRequest request;
		SetCatalogId(request, catalog);
		request.SetDatabaseName(database_name);
		if (!next_token.empty()) {
			request.SetNextToken(next_token);
		}
		auto outcome = client->GetTables(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, StringUtil::Format("GetTables (database '%s')", database_name));
		}
		auto &tables = outcome.GetResult();
		for (auto &table : tables.GetTableList()) {
			result.push_back(ToTableInfo(table));
		}
		next_token = tables.GetNextToken();
	} while (!next_token.empty());
	return result;
}

bool GlueAPI::GetTable(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                       const string &table_name, GlueTableInfo &result) {
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::GetTableRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetName(table_name);
	auto outcome = client->GetTable(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			return false;
		}
		ThrowGlueError(outcome, StringUtil::Format("GetTable '%s.%s'", database_name, table_name));
	}
	result = ToTableInfo(outcome.GetResult().GetTable());
	return true;
}

} // namespace duckdb

//===--------------------------------------------------------------------===//
// Write API
//===--------------------------------------------------------------------===//
namespace duckdb {

namespace {

template <class OUTCOME>
bool IsAlreadyExists(const OUTCOME &outcome) {
	return outcome.GetError().GetErrorType() == Aws::Glue::GlueErrors::ALREADY_EXISTS;
}

Aws::Map<Aws::String, Aws::String> ToAwsMap(const unordered_map<string, string> &input) {
	Aws::Map<Aws::String, Aws::String> result;
	for (auto &entry : input) {
		result.emplace(entry.first, entry.second);
	}
	return result;
}

Aws::Vector<Aws::Glue::Model::Column> ToAwsColumns(const vector<GlueColumn> &input) {
	Aws::Vector<Aws::Glue::Model::Column> result;
	for (auto &column : input) {
		Aws::Glue::Model::Column aws_column;
		aws_column.SetName(column.name);
		aws_column.SetType(column.type);
		if (!column.comment.empty()) {
			aws_column.SetComment(column.comment);
		}
		result.push_back(std::move(aws_column));
	}
	return result;
}

} // namespace

void GlueAPI::CreateDatabase(ClientContext &context, GlueCatalog &catalog, const GlueDatabaseInfo &database) {
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::DatabaseInput input;
	input.SetName(database.name);
	if (!database.description.empty()) {
		input.SetDescription(database.description);
	}
	if (!database.location_uri.empty()) {
		input.SetLocationUri(database.location_uri);
	}
	if (!database.parameters.empty()) {
		input.SetParameters(ToAwsMap(database.parameters));
	}
	Aws::Glue::Model::CreateDatabaseRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseInput(input);
	auto outcome = client->CreateDatabase(request);
	if (!outcome.IsSuccess()) {
		if (IsAlreadyExists(outcome)) {
			throw CatalogException("Glue database with name \"%s\" already exists", database.name);
		}
		ThrowGlueError(outcome, StringUtil::Format("CreateDatabase '%s'", database.name));
	}
}

void GlueAPI::DeleteDatabase(ClientContext &context, GlueCatalog &catalog, const string &database_name) {
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::DeleteDatabaseRequest request;
	SetCatalogId(request, catalog);
	request.SetName(database_name);
	auto outcome = client->DeleteDatabase(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			throw CatalogException("Glue database with name \"%s\" does not exist", database_name);
		}
		ThrowGlueError(outcome, StringUtil::Format("DeleteDatabase '%s'", database_name));
	}
}

void GlueAPI::CreateIcebergTable(ClientContext &context, GlueCatalog &catalog, const GlueTableInfo &table) {
	if (table.location.empty()) {
		throw InvalidInputException("Can not create Iceberg table '%s.%s' without a location", table.database_name,
		                            table.name);
	}
	auto client = GetClient(context, catalog);

	Aws::Glue::Model::StorageDescriptor storage_descriptor;
	storage_descriptor.SetLocation(table.location);
	storage_descriptor.SetColumns(ToAwsColumns(table.columns));

	Aws::Glue::Model::TableInput table_input;
	table_input.SetName(table.name);
	table_input.SetTableType("EXTERNAL_TABLE");
	table_input.SetStorageDescriptor(storage_descriptor);
	if (!table.partition_keys.empty()) {
		table_input.SetPartitionKeys(ToAwsColumns(table.partition_keys));
	}
	if (!table.parameters.empty()) {
		table_input.SetParameters(ToAwsMap(table.parameters));
	}

	// Let Glue create the Iceberg metadata (format version 2) at the table location
	Aws::Glue::Model::IcebergInput iceberg_input;
	iceberg_input.SetMetadataOperation(Aws::Glue::Model::MetadataOperation::CREATE);
	iceberg_input.SetVersion("2");
	Aws::Glue::Model::OpenTableFormatInput open_table_format_input;
	open_table_format_input.SetIcebergInput(iceberg_input);

	Aws::Glue::Model::CreateTableRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(table.database_name);
	request.SetTableInput(table_input);
	request.SetOpenTableFormatInput(open_table_format_input);
	auto outcome = client->CreateTable(request);
	if (!outcome.IsSuccess()) {
		if (IsAlreadyExists(outcome)) {
			throw CatalogException("Table with name \"%s\" already exists in Glue database \"%s\"", table.name,
			                       table.database_name);
		}
		ThrowGlueError(outcome, StringUtil::Format("CreateTable '%s.%s' (location '%s')", table.database_name,
		                                           table.name, table.location));
	}
}

void GlueAPI::DeleteTable(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                          const string &table_name) {
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::DeleteTableRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetName(table_name);
	auto outcome = client->DeleteTable(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			throw CatalogException("Table with name \"%s\" does not exist in Glue database \"%s\"", table_name,
			                       database_name);
		}
		ThrowGlueError(outcome, StringUtil::Format("DeleteTable '%s.%s'", database_name, table_name));
	}
}

} // namespace duckdb
