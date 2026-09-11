PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=glue
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Extensions needed for testing: the S3 secret type lives in httpfs, the credential_chain provider in aws
CORE_EXTENSIONS='httpfs;parquet;aws;json'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile
