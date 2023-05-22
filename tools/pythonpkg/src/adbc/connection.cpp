#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/adbc/connection.hpp"
#include "duckdb_python/python_conversion.hpp"

namespace duckdb {

namespace adbc {

void PyADBCConnection::Initialize(py::module_ &m) {
	auto connection_module = py::class_<PyADBCConnection, shared_ptr<PyADBCConnection>, DuckDBPyConnection>(
	    m, "Connection", py::module_local());
	connection_module.def("clone", &PyADBCConnection::Clone, "Clone the current ADBC Connection");
}

shared_ptr<PyADBCConnection> FetchOrCreateInstance(const string &database, DBConfig &config) {
	auto res = make_shared<PyADBCConnection>();
	res->database = DuckDBPyConnection::instance_cache.GetInstance(database, config);
	if (!res->database) {
		//! No cached database, we must create a new instance
		DuckDBPyConnection::CreateNewInstance(*res, database, config);
		return res;
	}
	res->connection = make_uniq<Connection>(*res->database);
	return res;
}

shared_ptr<PyADBCConnection> PyADBCConnection::Connect(const string &database, bool read_only,
                                                       const py::dict &config_options) {
	auto config_dict = TransformPyConfigDict(config_options);

	DBConfig config(config_dict, read_only);
	auto res = FetchOrCreateInstance(database, config);
	auto &client_context = *res->connection->context;
	SetDefaultConfigArguments(client_context);
	return res;
}

shared_ptr<PyADBCConnection> PyADBCConnection::Clone() {
	return shared_from_base<PyADBCConnection>();
}

} // namespace adbc

} // namespace duckdb
