#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

//! Options parsed from ATTACH '<catalog id>' (TYPE GLUE, ...)
struct GlueAttachOptions {
	//! The name of the attached database
	string name;
	//! The raw ATTACH path (':' | '<account id>' | '<account id>:<catalog>' | '<catalog>/<nested>' ...)
	string path;
	//! The Glue CatalogId sent with every request, empty means the default catalog of the current account
	string catalog_id;
	//! AWS region the Glue Data Catalog lives in
	string region;
	//! Name of the DuckDB (s3 / aws) secret to take credentials from, empty selects the default secret
	string secret_name;
	//! Default S3 location used for new databases, and for tables created in databases that have no LocationUri
	string default_location = "s3://simple-s3-glue-database/glue-database-root";
	//! Optional default schema
	Identifier default_schema;
};

} // namespace duckdb
