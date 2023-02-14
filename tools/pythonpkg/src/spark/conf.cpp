#include "duckdb_python/spark/conf.hpp"

#include "duckdb_python/spark/catalog.hpp"
#include "duckdb_python/spark/session/builder.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/string.hpp"

#include "duckdb_python/pyconnection.hpp"

namespace duckdb {
namespace spark {

shared_ptr<SparkConf> SparkConf::CreateSparkConf(bool load_defaults, const py::object &jvm, const py::object &jconf) {
	return make_shared<SparkConf>(load_defaults, jvm, jconf);
}

void SparkConf::Initialize(py::handle &m) {
	auto spark_conf = py::class_<SparkConf, shared_ptr<SparkConf>>(m, "SparkConf", py::module_local());
	spark_conf.def(py::init([](bool load_defaults, const py::object &jvm, const py::object &jconf) {
		               return SparkConf::CreateSparkConf(load_defaults, jvm, jconf);
	               }),
	               py::arg("loadDefaults") = true, py::arg("_jvm") = py::none(), py::arg("_jconf") = py::none());
}

SparkConf::SparkConf(bool load_defaults, const py::object &jvm, const py::object &jconf)
    : load_defaults(load_defaults) {
}

} // namespace spark
} // namespace duckdb
