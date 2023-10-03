add_library_unity(duckdb_common_arrow_acero OBJECT declaration.cpp
                  arrow_test_factory.cpp arrow_stream_factory.cpp dataset.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_arrow_acero>
    PARENT_SCOPE)
