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

class SparkConf {
public:
	static void Initialize(py::handle &m);
	static shared_ptr<SparkConf> CreateSparkConf(bool load_defaults, const py::object &jvm, const py::object &jconf);
	SparkConf(bool load_defaults, const py::object &jvm = py::none(), const py::object &jconf = py::none());

private:
private:
	bool load_defaults;
};

} // namespace spark
} // namespace duckdb
