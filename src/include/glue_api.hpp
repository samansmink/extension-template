#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

#include <memory>

namespace Aws {
namespace Glue {
class GlueClient;
} // namespace Glue
} // namespace Aws

namespace duckdb {
class ClientContext;
class GlueCatalog;

//! The (open) table format a Glue table is stored in, derived from the table parameters
enum class GlueTableFormat : uint8_t { ICEBERG, DELTA, HUDI, HIVE, UNKNOWN };

string GlueTableFormatToString(GlueTableFormat format);

struct GlueColumn {
	string name;
	//! The Glue (Hive style) type string, e.g. 'int', 'decimal(10,2)', 'array<string>'
	string type;
	string comment;
};

//! A Glue "Database", exposed as a DuckDB schema
struct GlueDatabaseInfo {
	string name;
	string description;
	//! Optional S3 location tables of this database default to
	string location_uri;
	unordered_map<string, string> parameters;
};

//! A Glue "Table"
struct GlueTableInfo {
	string name;
	string database_name;
	//! The Glue TableType (EXTERNAL_TABLE, VIRTUAL_VIEW, ...), not the open table format
	string glue_table_type;
	//! StorageDescriptor.Location
	string location;
	string input_format;
	string output_format;
	vector<GlueColumn> columns;
	vector<GlueColumn> partition_keys;
	unordered_map<string, string> parameters;

public:
	//! Derive the open table format from the table parameters
	GlueTableFormat GetFormat() const;
	//! Human readable description of the table type, used in error messages
	string GetFormatName() const;
	//! The 'metadata_location' parameter of an Iceberg table (empty if not present)
	string GetMetadataLocation() const;
	//! Look up a table parameter (case-insensitive key), returns empty string if missing
	string GetParameter(const string &key) const;
};

//! Thin wrapper around the AWS SDK Glue client
class GlueAPI {
public:
	//! Verify that the catalog can be reached with the configured credentials
	static void VerifyConnection(ClientContext &context, GlueCatalog &catalog);

	//! List all databases of the catalog
	static vector<GlueDatabaseInfo> GetDatabases(ClientContext &context, GlueCatalog &catalog);
	//! Fetch a single database, returns false if it does not exist
	static bool GetDatabase(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                        GlueDatabaseInfo &result);
	//! List all tables of a database
	static vector<GlueTableInfo> GetTables(ClientContext &context, GlueCatalog &catalog, const string &database_name);
	//! Fetch a single table, returns false if it does not exist
	static bool GetTable(ClientContext &context, GlueCatalog &catalog, const string &database_name,
	                     const string &table_name, GlueTableInfo &result);

private:
	//! Get (or create) the Glue client for the catalog, using the credentials of the configured DuckDB secret
	static std::shared_ptr<Aws::Glue::GlueClient> GetClient(ClientContext &context, GlueCatalog &catalog);
};

} // namespace duckdb
