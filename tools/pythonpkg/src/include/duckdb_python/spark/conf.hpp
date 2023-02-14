#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb_python/spark/session/builder.hpp"

#include <memory>

namespace duckdb {

class DuckDBPyConnection;

namespace spark {

class Catalog;

class SparkConf : public std::enable_shared_from_this<SparkConf> {
public:
	static void Initialize(py::handle &m);
	static shared_ptr<SparkConf> CreateSparkConf(bool load_defaults, const py::object &jvm, const py::object &jconf);
	SparkConf(bool load_defaults, const py::object &jvm = py::none(), const py::object &jconf = py::none());

public:
	bool Contains(const string &key);
	// 'default_value' is object so we can differentiate between "" and unset
	py::object Get(const string &key, const py::object &default_value = py::none());
	py::list GetAll();
	shared_ptr<SparkConf> Set(const string &key, const string &value);
	shared_ptr<SparkConf> SetAll(const py::list &pairs);
	shared_ptr<SparkConf> SetAppName(const string &value);
	shared_ptr<SparkConf> SetExecutorEnv(const string &key = string(), const string &value = string(),
	                                     const py::object &pairs = py::none());
	shared_ptr<SparkConf> SetIfMissing(const string &key, const string &value);
	shared_ptr<SparkConf> SetMaster(const string &value);
	shared_ptr<SparkConf> SetSparkHome(const string &value);
	string ToDebugString();

private:
private:
	bool load_defaults;
};

} // namespace spark
} // namespace duckdb
