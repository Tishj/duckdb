#include "duckdb_python/spark/conf.hpp"

#include "duckdb_python/spark/catalog.hpp"
#include "duckdb_python/spark/session/builder.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/string.hpp"

#include "duckdb_python/pyconnection.hpp"

namespace duckdb {
namespace spark {

void SparkConf::Initialize(py::handle &m) {
	auto spark_conf = py::class_<SparkConf, shared_ptr<SparkConf>>(m, "SparkConf", py::module_local());

	// Constructor
	spark_conf.def(py::init([](bool load_defaults, const py::object &jvm, const py::object &jconf) {
		               return make_shared<SparkConf>(load_defaults, jvm, jconf);
	               }),
	               py::arg("loadDefaults") = true, py::arg("_jvm") = py::none(), py::arg("_jconf") = py::none());

	spark_conf.def("contains", &SparkConf::Contains, "Does this configuration contain a given key?", py::arg("key"));
	spark_conf.def("get", &SparkConf::Get, "Get the configured value for some key, or return a default otherwise.",
	               py::arg("key"), py::arg("default_value") = py::none());
	spark_conf.def("getAll", &SparkConf::GetAll, "Get all values as a list of key-value pairs.");
	spark_conf.def("set", &SparkConf::Set, "Set a configuration property.", py::arg("key"), py::arg("value"));
	spark_conf.def("setAll", &SparkConf::SetAll, "Set multiple parameters, passed as a list of key-value pairs.",
	               py::arg("pairs"));
	spark_conf.def("setAppName", &SparkConf::SetAppName, "Set application name.", py::arg("value"));
	spark_conf.def("setExecutorEnv", &SparkConf::SetExecutorEnv,
	               "Set an environment variable to be passed to executors.", py::arg("key") = string(),
	               py::arg("value") = string(), py::arg("pairs") = py::none());
	spark_conf.def("setIfMissing", &SparkConf::SetIfMissing, "Set a configuration property, if not already set.",
	               py::arg("key"), py::arg("value"));
	spark_conf.def("setMaster", &SparkConf::SetMaster, "Set master URL to connect to.", py::arg("value"));
	spark_conf.def("setSparkHome", &SparkConf::SetSparkHome, "Set path where Spark is installed on worker nodes.",
	               py::arg("value"));
	spark_conf.def("toDebugString", &SparkConf::ToDebugString,
	               "Returns a printable version of the configuration, as a list of key=value pairs, one per line.");
}

SparkConf::SparkConf(bool load_defaults, const py::object &jvm, const py::object &jconf)
    : load_defaults(load_defaults) {
}

// Member methods

bool SparkConf::Contains(const string &key) {
	return false;
}

py::object SparkConf::Get(const string &key, const py::object &default_value) {
	return py::none();
}

py::list SparkConf::GetAll() {
	return py::list();
}

shared_ptr<SparkConf> SparkConf::Set(const string &key, const string &value) {
	return shared_from_this();
}

shared_ptr<SparkConf> SparkConf::SetAll(const py::list &pairs) {
	return shared_from_this();
}

shared_ptr<SparkConf> SparkConf::SetAppName(const string &value) {
	return shared_from_this();
}

shared_ptr<SparkConf> SparkConf::SetExecutorEnv(const string &key, const string &value, const py::object &pairs) {
	return shared_from_this();
}

shared_ptr<SparkConf> SparkConf::SetIfMissing(const string &key, const string &value) {
	return shared_from_this();
}

shared_ptr<SparkConf> SparkConf::SetMaster(const string &value) {
	return shared_from_this();
}

shared_ptr<SparkConf> SparkConf::SetSparkHome(const string &value) {
	return shared_from_this();
}

string SparkConf::ToDebugString() {
	return "";
}

} // namespace spark
} // namespace duckdb
