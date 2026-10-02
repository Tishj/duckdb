if(NOT DEFINED EXTENSION_CONFIG_BASE_DIR)
    if(DEFINED ENV{EXTENSION_CONFIG_BASE_DIR} AND NOT "$ENV{EXTENSION_CONFIG_BASE_DIR}" STREQUAL "")
        set(EXTENSION_CONFIG_BASE_DIR "$ENV{EXTENSION_CONFIG_BASE_DIR}")
    else()
        set(EXTENSION_CONFIG_BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/.github/config/extensions")
    endif()
endif()
get_filename_component(EXTENSION_CONFIG_BASE_DIR "${EXTENSION_CONFIG_BASE_DIR}" ABSOLUTE BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
if(NOT IS_DIRECTORY "${EXTENSION_CONFIG_BASE_DIR}")
    message(FATAL_ERROR "Extension config directory does not exist: ${EXTENSION_CONFIG_BASE_DIR}")
endif()

if(DEFINED CORE_EXTENSIONS)
    message(DEPRECATION "CORE_EXTENSIONS is deprecated. Use BUILD_EXTENSIONS instead.")
    if(NOT DEFINED BUILD_EXTENSIONS)
        set(BUILD_EXTENSIONS ${CORE_EXTENSIONS})
    else()
        list(APPEND BUILD_EXTENSIONS ${CORE_EXTENSIONS})
    endif()
endif()

# Explicit project configurations take precedence over named defaults.
foreach(DUCKDB_EXTENSION_CONFIG IN LISTS DUCKDB_EXTENSION_CONFIGS)
    if (NOT "${DUCKDB_EXTENSION_CONFIG}" STREQUAL "")
        include("${DUCKDB_EXTENSION_CONFIG}")
    endif()
endforeach()

# Load extensions passed through cmake config var
foreach(EXT IN LISTS BUILD_EXTENSIONS)
    if("${EXT}" STREQUAL "jemalloc")
        message(WARNING "The 'jemalloc' allocator is no longer provided as an extension, use 'ENABLE_JEMALLOC=ON' to include jemalloc instead")
        set(ENABLE_JEMALLOC ON CACHE BOOL "Use jemalloc as the memory allocator for DuckDB" FORCE)
        # Backward-compat shim: downstream consumers call target_link_libraries(... ${ext}_extension).
        # We provide an empty INTERFACE target to make sure that doesn't fail.
        if(NOT TARGET jemalloc_extension)
            add_library(jemalloc_extension INTERFACE)
        endif()
        continue()
    endif()

    if(NOT "${EXT}" STREQUAL "")
        if (EXISTS "${EXTENSION_CONFIG_BASE_DIR}/${EXT}.cmake")
            # out-of-tree extension: load cmake file
            include("${EXTENSION_CONFIG_BASE_DIR}/${EXT}.cmake")
        else()
            # in-tree or non-existent extension: load it
            duckdb_extension_load(${EXT})
        endif()
        if(LINK_CORE_EXTENSIONS)
            duckdb_extension_statically_link(${EXT})
        endif()
    endif()
endforeach()

# Check if jemalloc is ignored, and if so disable it
list (FIND SKIP_EXTENSIONS "jemalloc" _index)
if (${_index} GREATER -1)
    message(WARNING "The 'jemalloc' allocator is no longer provided as an extension, use 'ENABLE_JEMALLOC=OFF' to disable jemalloc instead")
    set(ENABLE_JEMALLOC OFF CACHE BOOL "Use jemalloc as the memory allocator for DuckDB" FORCE)
endif()



# Local extension config
if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/extension/extension_config_local.cmake)
    include(${CMAKE_CURRENT_SOURCE_DIR}/extension/extension_config_local.cmake)
endif()

# Load base extension config
include(${CMAKE_CURRENT_SOURCE_DIR}/extension/extension_config.cmake)
