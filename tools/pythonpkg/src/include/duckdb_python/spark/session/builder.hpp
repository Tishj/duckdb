#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"

#include <memory>

namespace duckdb {
namespace spark {

class SparkSession;

class SessionBuilder : public std::enable_shared_from_this<SessionBuilder> {
public:
	static void Initialize(py::handle &m);
	shared_ptr<SessionBuilder> AppName(const string &name);
	shared_ptr<SessionBuilder> AddConfigOption(const py::object &key, const py::object &value,
	                                           const py::object &config);
	shared_ptr<SessionBuilder> EnableHiveSupport();
	shared_ptr<SparkSession> GetOrCreate();
	shared_ptr<SessionBuilder> Master(const string &name);

private:
private:
	// Name of the to-be-constructed SparkSession
	string name;
	// The key-value configuration options
	unordered_map<string, string> config_map;
	// Whether hive support is enabled or not
	bool hive_support_enabled = false;
	// The URL to the Spark cluster to connect to
	string cluster_url;
};

} // namespace spark
} // namespace duckdb
