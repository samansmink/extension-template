#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! glue_get_table_response('<catalog>.<schema>.<table>'): the Glue GetTable response for a table of an attached
//! Glue catalog, for inspecting what Glue knows about a table. One row with the most useful fields as columns and
//! the complete Glue Table object as VARIANT.
TableFunction GetGlueGetTableResponseFunction();

//! glue_partitions('<catalog>.<schema>.<table>'): the partitions of a Hive table as registered in Glue, one row
//! per partition with a typed column per partition key and the partition's location
TableFunction GetGluePartitionsFunction();
//! glue_add_partition(table, {key: value, ...}, location := '...', if_not_exists := false): ALTER TABLE ADD
//! PARTITION. Without 'location' the partition lives at <table location>/<key>=<value>/...
TableFunction GetGlueAddPartitionFunction();
//! glue_drop_partition(table, {key: value, ...}, if_exists := false): ALTER TABLE DROP PARTITION, the data files
//! are left in place
TableFunction GetGlueDropPartitionFunction();
//! glue_rename_partition(table, {key: value, ...}, {key: new_value, ...}): ALTER TABLE ... RENAME TO PARTITION,
//! changes the partition values and keeps the location
TableFunction GetGlueRenamePartitionFunction();

//! A scan that produces no rows, for tables without any data files (e.g. a freshly created Hive table). The
//! columns come from the catalog entry, which is what the binder plans with; 'bind_data' is filled in.
TableFunction MakeGlueEmptyScan(unique_ptr<FunctionData> &bind_data);

} // namespace duckdb
