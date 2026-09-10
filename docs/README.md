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

Hive tables stored as parquet (ParquetHiveSerDe) are scanned with `read_parquet` over the table location, one
directory level per partition key. Partition values come from the `<key>=<value>` directory names and partition
types from Glue's partition keys, so they are not sniffed from the values. The location is assumed to hold only the
table's own data. A table without data files (just created) scans as empty. Other SerDes (CSV, JSON, ORC, Avro) are
not supported yet.

Columns are taken from the Glue definition, data columns first and partition keys last, in `PARTITIONED BY` order.
A scan verifies that the parquet files agree with those columns (names and types) and fails with a message naming
the difference otherwise.

## Writing

- `CREATE SCHEMA` creates a Glue database with LocationUri `<DEFAULT_LOCATION>/<schema>`, or without a LocationUri
  when the catalog was attached without `DEFAULT_LOCATION`.
- `CREATE TABLE ... [PARTITIONED BY (col, ...)] [WITH (location = '...', <property> = '...')]` creates a parquet
  Hive table at `location`, else `<DEFAULT_LOCATION>/<database>/<table>`, else `<database LocationUri>/<table>`;
  without any of these the statement fails. Partition keys must be plain column names; they become Glue
  PartitionKeys and are listed last in the table's columns. Unknown `WITH` keys are stored as Glue table parameters.
- `INSERT INTO` and `CREATE TABLE ... AS` write parquet files into the table location (one file per partition
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
