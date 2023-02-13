#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb_python/spark/session/builder.hpp"

#include <memory>

namespace duckdb {
namespace spark {

class Catalog;
class DuckDBPyConnection;

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
