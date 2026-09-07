# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(glue
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Iceberg tables registered in Glue are scanned through the iceberg extension, which needs avro for the manifests.
# The hashes (and patches) are the ones the duckdb submodule pins in .github/config/extensions/{avro,iceberg}.cmake,
# so they are known to build against this duckdb commit.
if (NOT EMSCRIPTEN AND NOT MINGW)
  duckdb_extension_load(avro
        GIT_URL https://github.com/duckdb/duckdb-avro
        GIT_TAG 70f1766d8deb91cbf55e452d474755effc14a35b
        SUBMODULES "third_party/avro-c"
        APPLY_PATCHES
    )
  # Built from the local checkout in ./duckdb-iceberg (at bf975d6f with duckdb's merge-into patch applied by hand,
  # APPLY_PATCHES only works for GIT_URL loads). Switch back to the GIT_URL/GIT_TAG form for a reproducible build.
  duckdb_extension_load(iceberg
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/duckdb-iceberg
        #GIT_URL https://github.com/duckdb/duckdb-iceberg
        #GIT_TAG bf975d6febdd1842294e80a4887fc52ae8623d5b
        #APPLY_PATCHES
    )
endif()
