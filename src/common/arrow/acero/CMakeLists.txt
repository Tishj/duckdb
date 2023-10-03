add_library_unity(duckdb_common_acero OBJECT declaration.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_acero>
    PARENT_SCOPE)
