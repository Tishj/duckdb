#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"

namespace duckdb {

class DuckDBPyCompute {
public:
	DuckDBPyCompute() = delete;

	static py::object SortIndices(const py::object &array, py::object &sort_keys, const string &null_placement, const py::object &options, const py::object &memory_pool);
public:
	static void Initialize(py::module_ &m);
};

} // namespace duckdb
