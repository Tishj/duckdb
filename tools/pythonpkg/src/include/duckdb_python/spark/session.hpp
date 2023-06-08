#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb_python/spark/session/builder.hpp"

#include <memory>

namespace duckdb {

class DuckDBPyConnection;

namespace spark {

class Catalog;

class SparkSession {
public:
	static void Initialize(py::handle &m);
	SparkSession(const string &name, const unordered_map<string, string> &configuration, bool hive_support_enabled,
	             const string &cluster_url);
	shared_ptr<Catalog> GetCatalog();

private:
private:
	string name;
	unordered_map<string, string> configuration;
	bool hive_support_enabled;
	string cluster_url;

	shared_ptr<DuckDBPyConnection> connection;
	shared_ptr<Catalog> catalog;
};

} // namespace spark
} // namespace duckdb
