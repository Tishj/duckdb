#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {
namespace spark {

// This class is nothing more than a python handle to group duckdb spark modules

class DuckDBSpark {
public:
	static void Initialize(py::handle &m);
};

} // namespace spark
} // namespace duckdb
