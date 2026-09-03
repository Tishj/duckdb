//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/catalog_entry_scan_level.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! The level to which entries returned by a catalog scan must be expanded.
enum class CatalogEntryScanLevel : uint8_t {
	SCHEMA = 0,
	TABLE = 1,
	COLUMN = 2,
};

} // namespace duckdb
