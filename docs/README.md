# DuckDB Glue extension

Prototype extension that exposes an AWS Glue Data Catalog as a DuckDB catalog. It talks to Glue through the AWS SDK
Glue client, so it can list every table type Glue registers (Iceberg, Delta, Hive, ...). Iceberg tables are scanned through the iceberg extension, Delta tables through the delta extension's
`delta_scan` (with the columns taken from the Delta log), and Hive tables stored as parquet are scanned with
`read_parquet` over the table location, with partition values from the directory names and partition types from
Glue's partition keys. The iceberg and
avro extensions are linked into the build through `extension_config.cmake`, pinned to the commits the duckdb
submodule uses.

```sql
CREATE SECRET (TYPE S3, PROVIDER credential_chain, REGION 'us-east-1');
ATTACH '840140254803' AS my_datalake (TYPE GLUE);
SHOW ALL TABLES;
```

Attach options:

| option             | description                                                                 |
|--------------------|-----------------------------------------------------------------------------|
| `SECRET`           | name of the s3/aws secret to take credentials from (default: default secret) |
| `REGION`           | AWS region of the catalog (default: region of the secret)                    |
| `DEFAULT_LOCATION` | S3 prefix for new databases and for tables in databases without a LocationUri (default `s3://simple-s3-glue-database/glue-database-root`) |
| `DEFAULT_SCHEMA`   | Glue database to use as the default schema                                   |

## Inspecting tables

Every Glue table entry carries its format in `duckdb_tables().tags['table_type']` (`ICEBERG`, `DELTA`, `HIVE` or
`UNKNOWN`). The full Glue definition of a table is available through a table function:

```sql
SELECT * FROM glue_get_table_response('my_datalake.default.test_table');
SELECT response.Parameters.metadata_location FROM glue_get_table_response('my_datalake.default.test_table');
```

It returns one row with the classification, the Glue table type, location, SerDe, columns, partition keys and
parameters as columns, plus the complete Glue `Table` object as a VARIANT in `response`.

Delta tables: `INSERT` is executed by the delta extension. The table root is attached lazily as a hidden
single-table Delta catalog (`__glue_delta_<uuid>`, child catalog mode) and DML planning is forwarded to it; the
delta extension commits the Delta log at the end of the statement, Glue is not involved. Limitation of the delta
extension: an INSERT whose rows span several partitions fails, insert one partition per statement.

Hive tables support `ALTER TABLE ... ADD COLUMN`, `DROP COLUMN` and `ALTER COLUMN ... TYPE` (widening changes
only: integer widening, FLOAT to DOUBLE, anything to VARCHAR, since existing parquet files keep their types).
Partition keys can not be dropped or retyped. The change is written to Glue with UpdateTable, keeping the rest of
the definition; Iceberg and Delta tables keep their schema in their own metadata and reject ALTER TABLE.

## HTTP transport and logging

The AWS SDK's Glue calls are routed through DuckDB's HTTP layer (httpfs), so they honor DuckDB's proxy and
certificate settings and appear in the HTTP log:

```sql
CALL enable_logging('HTTP', storage='memory');
-- ... run queries ...
SELECT request.type, request.url, request.headers['x-amz-target'], response.status FROM duckdb_logs_parsed('HTTP');
```

`SET glue_network_calls_via_duckdb = false` switches back to the SDK's own HTTP client.

## Writing

INSERT, DELETE, UPDATE and MERGE INTO on Iceberg tables are executed by the iceberg extension: when attached, the Glue
catalog attaches itself a second time as a hidden Iceberg catalog (`__glue_internal_<uuid>`, using Glue's Iceberg
REST endpoint and the same secret) and forwards scans and DML planning to that catalog's table entries. The hidden
catalog is attached with `HIDDEN` visibility, so it does not show up in `SHOW DATABASES`, `SHOW ALL TABLES` or
`information_schema`, and it is detached together with the Glue catalog.

`CREATE SCHEMA` creates a Glue database at `<DEFAULT_LOCATION>/<schema>`. `CREATE TABLE` creates an Iceberg table
(format version 2) at `<database LocationUri>/<table>`; Glue writes the initial metadata file. `DROP TABLE` and
`DROP SCHEMA` delete the Glue entries but leave the data files in S3. Note that Glue deletes all tables of a database
when the database is dropped.

## Building

```sh
VCPKG_TOOLCHAIN_PATH='<path to vcpkg>/scripts/buildsystems/vcpkg.cmake' GEN=ninja make relassert
./build/relassert/test/unittest test/sql/test_glue_attach.test
```
