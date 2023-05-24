#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/pybind11/registered_py_object.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/arrow/arrow_array_stream.hpp"

namespace duckdb {

namespace adbc {

class PyADBCConnection : public DuckDBPyConnection {
public:
	explicit PyADBCConnection() {
	}
	virtual ~PyADBCConnection() {
	}

public:
	static void Initialize(py::module_ &parent);
	static shared_ptr<PyADBCConnection> Connect(const string &database, bool read_only, const py::dict &config);

public:
	string Method();
};

} // namespace adbc

} // namespace duckdb
