#include "duckdb_python/adbc/adbc.hpp"
#include "duckdb_python/adbc/connection.hpp"
#include "duckdb_python/pytype.hpp"

namespace duckdb {

namespace adbc {

void PyADBCModule::Initialize(py::module_ &parent) {
	auto m =
	    parent.def_submodule("adbc", "This module contains classes and methods related to the DuckDB ADBC Connector");
	PyADBCConnection::Initialize(m);

	// TODO: the sqlite version of this returns an 'AdbcDatabase' object
	// from 'adbc_driver_sqlite.connect'

	// It defines a 'dbapi' submodule, which also contains 'connect'
	// when that 'connect' method is called, it instead creates a 'Connection' object

	m.def("connect", &PyADBCConnection::Connect,
	      "Create a DuckDB database instance. Can take a database file name to read/write persistent data and a "
	      "read_only flag if no changes are desired",
	      py::arg("database") = ":memory:", py::arg("read_only") = false, py::arg_v("config", py::dict(), "None"));
}

} // namespace adbc

} // namespace duckdb
