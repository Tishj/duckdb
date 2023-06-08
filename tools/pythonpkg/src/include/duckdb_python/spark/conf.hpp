#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string.hpp"

#include <memory>

namespace duckdb {

class DuckDBPyConnection;

namespace spark {

class Catalog;

class SparkConf : public std::enable_shared_from_this<SparkConf> {
public:
	static void Initialize(py::handle &m);
	SparkConf(bool load_defaults, const py::object &jvm = py::none(), const py::object &jconf = py::none());

public:
	bool Contains(const string &key);
	// 'default_value' is object so we can differentiate between "" and unset
	py::object Get(const string &key, const py::object &default_value = py::none());
	py::list GetAll();
	shared_ptr<SparkConf> Set(const string &key, const string &value);
	shared_ptr<SparkConf> SetAll(const py::list &pairs);
	shared_ptr<SparkConf> SetAppName(const string &value);
	shared_ptr<SparkConf> SetExecutorEnv(const py::object &key = py::none(), const py::object &value = py::none(),
	                                     const py::object &pairs = py::none());
	shared_ptr<SparkConf> SetIfMissing(const string &key, const string &value);
	shared_ptr<SparkConf> SetMaster(const string &value);
	shared_ptr<SparkConf> SetSparkHome(const string &value);
	string ToDebugString();

private:
	string application_name;
	string master_url;
	string spark_home;
	case_insensitive_map_t<string> executor_env;
	case_insensitive_map_t<string> regular_env;

private:
	bool load_defaults;
};

} // namespace spark
} // namespace duckdb
