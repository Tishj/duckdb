#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/string.hpp"

#include <memory>

namespace duckdb {

class DuckDBPyConnection;

namespace spark {

class Catalog;

class SparkContext : public std::enable_shared_from_this<SparkContext> {
public:
	static void Initialize(py::handle &m);
	SparkContext(const string &master, const string &app_name, const string &spark_home, const py::object &py_files,
	             const py::object &environment, const py::object &batch_size, const py::object &serializer,
	             const py::object &conf, const py::object &gateway, const py::object &jsc,
	             const py::object &profiler_cls, const py::object &udf_profiler_cls);

public:
private:
private:
	string master;
};

} // namespace spark
} // namespace duckdb
