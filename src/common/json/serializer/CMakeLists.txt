add_library_unity(duckdb_json_serializer OBJECT json_serializer.cpp
                  json_deserializer.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_json_serializer>
    PARENT_SCOPE)
