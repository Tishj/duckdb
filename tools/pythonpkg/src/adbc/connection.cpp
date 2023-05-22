#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/adbc/connection.hpp"
#include "duckdb_python/python_conversion.hpp"

namespace duckdb {

namespace adbc {

void PyADBCConnection::Initialize(py::module_ &m) {
	auto connection_module = py::class_<PyADBCConnection, shared_ptr<PyADBCConnection>, DuckDBPyConnection>(
	    m, "Connection", py::module_local());
	connection_module.def("adbc_clone", &PyADBCConnection::Clone,
	                      "Create a new Connection sharing the same underlying database.");
	connection_module.def("adbc_get_info", &PyADBCConnection::GetMetadata,
	                      "Get metadata about the database and driver.");
	connection_module.def("adbc_get_objects", &PyADBCConnection::GetObjects,
	                      "List catalogs, schemas, tables, etc. in the database.");
	connection_module.def("adbc_get_table_schema", &PyADBCConnection::GetTableSchema,
	                      "Get the Arrow schema of a table by name.");
	connection_module.def("adbc_get_table_types", &PyADBCConnection::GetTableTypes,
	                      "List the types of tables that the server knows about.");

	// >>> conn.adbc_connection
	// <adbc_driver_manager._lib.AdbcConnection object at 0x1037fbfa0>

	// >>> conn.adbc_database
	// <adbc_driver_manager._lib.AdbcDatabase object at 0x100f69210>

	// Cursor methods:

	connection_module.def("adbc_execute_partitions", &PyADBCConnection::ExecutePartitions,
	                      "Execute a query and get the partitions of a distributed result set.");
	connection_module.def("adbc_ingest", &PyADBCConnection::Ingest, "Ingest Arrow data into a database table.");
	connection_module.def("adbc_prepare", &PyADBCConnection::Prepare, "Prepare a query without executing it.");
	connection_module.def("adbc_read_partition", &PyADBCConnection::ReadPartition,
	                      "Read a partition of a distributed result set.");

	// >>> cursor.adbc_statement
	// <adbc_driver_manager._lib.AdbcStatement object at 0x103887b20>
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

py::dict PyADBCConnection::GetMetadata() {
	return py::dict();
}

pyarrow::RecordBatchReader PyADBCConnection::GetObjects() {
	return py::none();
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
