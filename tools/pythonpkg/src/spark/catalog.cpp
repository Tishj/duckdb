#include "duckdb_python/spark/catalog.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/string.hpp"

#include "duckdb_python/pyconnection.hpp"
#include "duckdb_python/spark/data_frame.hpp"

namespace duckdb {
namespace spark {

void Catalog::Initialize(py::handle &m) {
	auto catalog_module = py::class_<Catalog, shared_ptr<Catalog>>(m, "catalog", py::module_local());
	catalog_module.def("cacheTable", &Catalog::CacheTable, "Caches the specified table in-memory.",
	                   py::arg("tableName"));
	catalog_module.def("clearCache", &Catalog::ClearCache, "Removes all cached tables from the in-memory cache.");
	DefineMethod({"createExternalTable", "createTable"}, catalog_module, &Catalog::CreateTable,
	             "Creates a table based on the dataset in a data source.", py::arg("tableName"),
	             py::arg("path") = py::str(), py::arg("source") = py::str(), py::arg("schema") = py::none(),
	             py::arg("description") = py::str());
	catalog_module.def("currentDatabase", &Catalog::CurrentDatabase);
	catalog_module.def("databaseExists", &Catalog::DatabaseExists, py::arg("dbName"));
	catalog_module.def("dropGlobalTempView", &Catalog::DropGlobalTempView, py::arg("viewName"));
	catalog_module.def("dropTempView", &Catalog::DropLocalTempView, py::arg("viewName"));
	catalog_module.def("functionExists", &Catalog::FunctionExists, py::arg("functionName"),
	                   py::arg("dbName") = py::str());
	catalog_module.def("isCached", &Catalog::IsCached, py::arg("tableName"));
	catalog_module.def("listColumns", &Catalog::ListColumns, py::arg("tableName"), py::arg("dbName") = py::str());
	catalog_module.def("listDatabases", &Catalog::ListDatabases);
	catalog_module.def("listFunctions", &Catalog::ListFunctions, py::arg("dbName") = py::str());
	catalog_module.def("listTables", &Catalog::ListTables, py::arg("dbName") = py::str());
	catalog_module.def("recoverPartitions", &Catalog::RecoverPartitions, py::arg("tableName"));
	catalog_module.def("refreshByPath", &Catalog::RefreshByPath, py::arg("path"));
	catalog_module.def("refreshTable", &Catalog::RefreshTable, py::arg("tableName"));
	catalog_module.def("setCurrentDatabase", &Catalog::SetCurrentDatabase, py::arg("dbName"));
	catalog_module.def("tableExists", &Catalog::TableExists, py::arg("tableName"), py::arg("dbName") = py::str());
	catalog_module.def("uncacheTable", &Catalog::UncacheTable, py::arg("tableName"));
}

Catalog::Catalog(shared_ptr<DuckDBPyConnection> connection) : connection(std::move(connection)) {
}

void Catalog::CacheTable(const string &table_name) {
	// TODO: implement this
}

void Catalog::ClearCache() {
	// TODO: implement this
}

bool Catalog::IsCached(const string &table_name) {
	// TODO: implement this
	return false;
}

void Catalog::UncacheTable(const string &table_name) {
	// TODO: implement this
	return;
}

shared_ptr<DataFrame> Catalog::CreateTable(const string &table_name, const string &path, const string &source,
                                           const py::object &schema, const string &description) {
	// TODO: implement this
	// https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.createTable.html#pyspark.sql.Catalog.createTable
	//  This should just adapt the arguments and create a DuckDBPyRelation, wrapping it in a duckdb::spark::DataFrame
	return nullptr;
}

shared_ptr<DuckDBPyConnection> Catalog::CurrentDatabase() {
	return connection;
}

void Catalog::SetCurrentDatabase(const string &db_name) {
	// TODO: implement this
}

bool Catalog::DatabaseExists(const string &db_name) {
	// TODO: implement this
	// https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.databaseExists.html#pyspark.sql.Catalog.databaseExists
	return false;
}

void Catalog::DropGlobalTempView(const string &name) {
	// TODO: implement this
	// https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.dropGlobalTempView.html#pyspark.sql.Catalog.dropGlobalTempView
}

void Catalog::DropLocalTempView(const string &name) {
	// TODO: implement this
}

bool Catalog::FunctionExists(const string &name, const string &db_name) {
	// TODO: implement this
	return false;
}

py::list Catalog::ListColumns(const string &table_name, const string &db_name) {
	// TODO: implement this
	return py::list();
}

py::list Catalog::ListDatabases() {
	// TODO: implement this
	return py::list();
}

py::list Catalog::ListTables(const string &db_name) {
	// TODO: implement this
	return py::list();
}

py::list Catalog::ListFunctions(const string &db_name) {
	// TODO: implement this
	return py::list();
}

void Catalog::RecoverPartitions(const string &table_name) {
	// TODO: implement this
	return;
}

void Catalog::RefreshByPath(const string &path) {
	// TODO: implement this
	return;
}

void Catalog::RefreshTable(const string &table_name) {
	// TODO: implement this
	return;
}

bool Catalog::TableExists(const string &table_name, const string &db_name) {
	// TODO: implement this
	return false;
}

} // namespace spark
} // namespace duckdb
