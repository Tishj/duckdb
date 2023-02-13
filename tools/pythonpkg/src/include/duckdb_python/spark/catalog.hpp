#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {
namespace spark {

class DuckDBPyConnection;
class DataFrame;

class Catalog {
public:
	static void Initialize(py::handle &m);
	//! -- Cache related methods --
	void CacheTable(const string &table_name);
	void ClearCache();
	bool IsCached(const string &table_name);
	void UncacheTable(const string &table_name);

	shared_ptr<DataFrame> CreateTable(const string &table_name, const string &path = string(),
	                                  const string &source = string(), const py::object &schema = py::none(),
	                                  const string &description = string()
	                                  // this is missing 'options'
	                                  // it's `**options: str` - not sure how that translates yet
	);
	shared_ptr<DuckDBPyConnection> CurrentDatabase();

	void DropGlobalTempView(const string &name);
	void DropLocalTempView(const string &name);
	bool FunctionExists(const string &name, const string &db_name = string());
	bool DatabaseExists(const string &name);
	py::list ListColumns(const string &table_name, const string &db_name = string());
	py::list ListDatabases();
	py::list ListTables(const string &db_name = string());
	py::list ListFunctions(const string &db_name = string());

	void RecoverPartitions(const string &table_name);
	void RefreshByPath(const string &path);
	void RefreshTable(const string &table_name);
	//! deprecated
	// void RegisterFunction(const string &name, const py::object &f, const py::object &return_type = py::none());

	void SetCurrentDatabase(const string &db_name);
	bool TableExists(const string &table_name, const string &db_name = string());

private:
	shared_ptr<DuckDBPyConnection> connection;
};

} // namespace spark
} // namespace duckdb
