option(BUILD_COMPLETE_EXTENSION_SET
       "Whether we need to actually build the complete set" TRUE)
if(DEFINED ENV{BUILD_COMPLETE_EXTENSION_SET})
  set(BUILD_COMPLETE_EXTENSION_SET "$ENV{BUILD_COMPLETE_EXTENSION_SET}")
endif()

option(WASM_ENABLED "Are DuckDB-Wasm extensions build enabled" FALSE)
if(DEFINED ENV{WASM_EXTENSIONS})
  set(WASM_ENABLED "$ENV{WASM_EXTENSIONS}")
endif()
option(MUSL_ENABLED "Are Musl extensions build enabled" FALSE)
if(DEFINED ENV{DUCKDB_PLATFORM})
  if("$ENV{DUCKDB_PLATFORM}" STREQUAL "linux_amd64_musl"
     OR "$ENV{DUCKDB_PLATFORM}" STREQUAL "linux_arm64_musl")
    set(MUSL_ENABLED ON)
  endif()
endif()
