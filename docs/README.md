# DuckDB Glue extension

Experimental extension that exposes an AWS Glue Data Catalog as a DuckDB catalog. It talks to Glue through the AWS
SDK Glue client and works with Hive (Glue native) tables stored as parquet on S3. Tables of other formats that Glue
registers (Iceberg, Delta, ...) are listed, with the columns Glue reports, but can not be read or written.

```sql
CREATE SECRET (TYPE S3, PROVIDER credential_chain, REGION 'eu-central-1');
ATTACH '984506134640' AS my_datalake (TYPE GLUE);
SHOW ALL TABLES;
SELECT * FROM my_datalake.default.some_table;
```

Attach options:

| option             | description                                                                 |
|--------------------|-----------------------------------------------------------------------------|
| `SECRET`           | name of the s3/aws secret to take credentials from (default: default secret) |
| `REGION`           | AWS region of the catalog (default: region of the secret)                    |
| `DEFAULT_LOCATION` | optional S3 prefix for new databases and for tables created without an explicit location (takes precedence over the Glue database LocationUri) |
| `DEFAULT_SCHEMA`   | Glue database to use as the default schema                                   |

## Reading

Hive tables stored as parquet (ParquetHiveSerDe) are scanned with `read_parquet` through a custom
`MultiFileReader` (`HiveMultiFileReader`) that follows Athena's read semantics:

- The data files are those directly below the location of every partition Glue lists (`GetPartitions`), or below
  the table location for an unpartitioned table. Partition locations need not follow the `<key>=<value>` layout.
  Files named `_*` or `.*` are skipped. A table without data files (just created) scans as empty.
- Partition column values are the values Glue stores for the partition, not the directory names, typed as Glue's
  partition keys. Filters on partition columns prune whole partitions before any file is opened (EXPLAIN shows
  `Scanning Files`).
- The schema is Glue's, data columns first and partition keys last, in `PARTITIONED BY` order. Files are matched
  by column name: a column a file does not have (added after the file was written) reads as NULL, a column with
  a different type in the file is cast, and file columns Glue does not list are ignored.

The SerDe of the Glue table decides the reader: ParquetHiveSerDe reads with `read_parquet`, LazySimpleSerDe and
OpenCSVSerde with `read_csv` (columns by position, no header unless `skip.header.line.count` is 1, delimiter
from `field.delim` / `separatorChar`, `,` otherwise) and JsonSerDe with `read_json` (one object per line, keys by
name). Other SerDes (ORC, Avro, ...) are not supported.

## Writing

- `CREATE SCHEMA` creates a Glue database with LocationUri `<DEFAULT_LOCATION>/<schema>`, or without a LocationUri
  when the catalog was attached without `DEFAULT_LOCATION`.
- `CREATE TABLE ... [PARTITIONED BY (col, ...)] [WITH (format = 'parquet' | 'csv' | 'json', location = '...',
  <property> = '...')]` creates a parquet (default), csv (LazySimpleSerDe, `,` delimited, no header) or json
  (JsonSerDe, one object per line)
  Hive table at `location`, else `<DEFAULT_LOCATION>/<database>/<table>`, else `<database LocationUri>/<table>`;
  without any of these the statement fails. Partition keys must be plain column names; they become Glue
  PartitionKeys and are listed last in the table's columns. Unknown `WITH` keys are stored as Glue table parameters.
- `INSERT INTO` and `CREATE TABLE ... AS` write files in the table's format into the table location (one file per partition
  touched, partition columns are not stored in the files) and register new partition directories in Glue with
  BatchCreatePartition. Because the partition keys are the last columns of the table, `INSERT ... VALUES` without a
  column list must list them last. `CREATE TABLE ... AS` creates the Glue table before the query runs; if the query
  fails the (empty) table stays.
- `ALTER TABLE ... ADD COLUMN` (appended last, no defaults), `DROP COLUMN` (not the last data column, not a
  partition key) and `ALTER COLUMN ... TYPE` update the Glue definition with UpdateTable. Existing parquet files
  keep their types, so only widening type changes are allowed: integer widening (TINYINT to BIGINT), FLOAT to
  DOUBLE, and anything to VARCHAR; partition keys can not be retyped.
- `DROP TABLE` and `DROP SCHEMA` delete the Glue entries but leave the data files in S3. Glue deletes all tables of
  a database when the database is dropped.

Glue has no transactions: DDL takes effect immediately, files are visible as soon as they are written, and nothing
is rolled back on failure. `DELETE`, `UPDATE` and `MERGE INTO` are not supported.

## Reading without a catalog: hive_scan

The same scan is available as a table function, for parquet Hive tables that are not (or not yet) registered in
Glue. The schema and the partitions are given as arguments; nothing is looked up in a catalog.

```sql
SELECT * FROM hive_scan('s3://bucket/warehouse/orders',
    schema := {id: 'INTEGER', amount: 'DOUBLE', dt: 'VARCHAR', country: 'VARCHAR'},
    partitions := [
        {dt: '2016-05-14', country: 'IN', location: NULL},
        {dt: '2016-05-16', country: 'IN', location: 's3://bucket/imports/INDIA_16_May_2016'}
    ]);
```

- `schema` (required): a struct of column name to DuckDB type name, data columns and partition columns.
- `format`: `'parquet'` (default), `'csv'` or `'json'`. For csv, `header := true` and `delim := '|'` describe the
  files; csv columns are matched by position, json keys by name.
- `partitions`: one struct per partition with a value for every partition key and an optional `location`; without
  a location the partition lives at `<root>/<key>=<value>/...`. The partition keys are the struct fields other than
  `location`, in that order, unless `partition_keys := [...]` names them. Without `partitions` the table is
  unpartitioned and the files directly below the root are read.
- A root or partition without data files scans as zero rows.

## Partitions

DuckDB has no `ALTER TABLE ... PARTITION` syntax, so the Athena partition statements are table functions. The
partition is given as a struct naming every partition key; values are stored as strings in Glue, in partition key
order.

| function | Athena statement |
|----------|------------------|
| `glue_partitions('cat.db.t')` | `SHOW PARTITIONS`: one row per registered partition, a typed column per partition key plus `location` |
| `CALL glue_add_partition('cat.db.t', {dt: '2016-05-14', country: 'IN'}, location := 's3://...', if_not_exists := false)` | `ALTER TABLE ADD [IF NOT EXISTS] PARTITION (...) [LOCATION ...]`; without `location` the partition lives at `<table location>/dt=2016-05-14/country=IN` |
| `CALL glue_drop_partition('cat.db.t', {dt: '2016-05-14', country: 'IN'}, if_exists := false)` | `ALTER TABLE DROP [IF EXISTS] PARTITION (...)`; the data files stay in S3 |
| `CALL glue_rename_partition('cat.db.t', {dt: '2016-05-14', country: 'IN'}, {dt: '2016-05-15', country: 'IN'})` | `ALTER TABLE PARTITION (...) RENAME TO PARTITION (...)`; changes the values, keeps the location |

## Inspecting tables

Every table entry carries its format in `duckdb_tables().tags['table_type']` (`HIVE`, `ICEBERG`, `DELTA` or
`UNKNOWN`). The full Glue definition of a table is available through a table function:

```sql
SELECT * FROM glue_get_table_response('my_datalake.default.some_table');
SELECT response.StorageDescriptor.Location FROM glue_get_table_response('my_datalake.default.some_table');
```

It returns one row with the classification, the Glue table type, location, SerDe, columns, partition keys and
parameters as columns, plus the complete Glue `Table` object as a VARIANT in `response`.

## HTTP transport and logging

The AWS SDK's Glue calls are routed through DuckDB's HTTP layer (httpfs), so they honor DuckDB's proxy and
certificate settings and appear in the HTTP log:

```sql
CALL enable_logging('HTTP', storage='memory');
-- ... run queries ...
SELECT request.type, request.url, request.headers['x-amz-target'], response.status FROM duckdb_logs_parsed('HTTP');
```

`SET glue_network_calls_via_duckdb = false` switches back to the SDK's own HTTP client.

## Building

```sh
VCPKG_TOOLCHAIN_PATH='<path to vcpkg>/scripts/buildsystems/vcpkg.cmake' GEN=ninja make relassert
ASAN_OPTIONS=detect_container_overflow=0 ./build/relassert/test/unittest test/sql/test_glue_attach.test
```
