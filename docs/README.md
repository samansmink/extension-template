# DuckDB Glue extension

Prototype extension that exposes an AWS Glue Data Catalog as a DuckDB catalog. It talks to Glue through the AWS SDK
Glue client, so it can list every table type Glue registers (Iceberg, Delta, Hive, ...). Scanning is delegated to
the extension that understands the table format (currently only Iceberg through `iceberg_scan`). The iceberg and
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

## Writing

INSERT, DELETE, UPDATE and MERGE INTO on Iceberg tables are executed by the iceberg extension: when attached, the Glue
catalog attaches itself a second time as a hidden Iceberg catalog (`__glue_internal_<uuid>`, using Glue's Iceberg
REST endpoint and the same secret) and forwards scans and DML planning to that catalog's table entries. The hidden
catalog is visible in `SHOW DATABASES` and is detached together with the Glue catalog.

`CREATE SCHEMA` creates a Glue database at `<DEFAULT_LOCATION>/<schema>`. `CREATE TABLE` creates an Iceberg table
(format version 2) at `<database LocationUri>/<table>`; Glue writes the initial metadata file. `DROP TABLE` and
`DROP SCHEMA` delete the Glue entries but leave the data files in S3. Note that Glue deletes all tables of a database
when the database is dropped.

## Building

```sh
VCPKG_TOOLCHAIN_PATH='<path to vcpkg>/scripts/buildsystems/vcpkg.cmake' GEN=ninja make relassert
./build/relassert/test/unittest test/sql/test_glue_attach.test
```
