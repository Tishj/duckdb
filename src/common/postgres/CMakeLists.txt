add_library_unity(duckdb_common_postgres OBJECT postgres_alias.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_postgres>
    PARENT_SCOPE)
