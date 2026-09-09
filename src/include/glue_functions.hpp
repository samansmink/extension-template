#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! glue_get_table_response('<catalog>.<schema>.<table>'): the Glue GetTable response for a table of an attached
//! Glue catalog, for inspecting what Glue knows about a table. One row with the most useful fields as columns and
//! the complete Glue Table object as VARIANT.
TableFunction GetGlueGetTableResponseFunction();

//! A scan that produces no rows, for tables without any data files (e.g. a freshly created Hive table). The
//! columns come from the catalog entry, which is what the binder plans with; 'bind_data' is filled in.
TableFunction MakeGlueEmptyScan(unique_ptr<FunctionData> &bind_data);

} // namespace duckdb
