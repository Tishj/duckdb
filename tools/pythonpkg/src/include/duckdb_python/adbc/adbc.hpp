#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace duckdb {

namespace adbc {

class PyADBCModule {
public:
	static void Initialize(py::module_ &parent);
};

} // namespace adbc

} // namespace duckdb
