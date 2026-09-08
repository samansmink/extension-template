#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! glue_get_table_response('<catalog>.<schema>.<table>'): the Glue GetTable response for a table of an attached
//! Glue catalog, for inspecting what Glue knows about a table. One row with the most useful fields as columns and
//! the complete Glue Table object as VARIANT.
TableFunction GetGlueGetTableResponseFunction();

} // namespace duckdb
