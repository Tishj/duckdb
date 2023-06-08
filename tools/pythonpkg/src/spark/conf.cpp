#include "duckdb_python/spark/conf.hpp"

#include "duckdb_python/spark/catalog.hpp"
#include "duckdb_python/spark/session/builder.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/string.hpp"

#include "duckdb_python/pyconnection/pyconnection.hpp"

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
	return regular_env.count(key);
}

py::object SparkConf::Get(const string &key, const py::object &default_value) {
	auto entry = regular_env.find(key);
	if (entry == regular_env.end()) {
		return default_value;
	}
	return py::str(entry->second);
}

py::list SparkConf::GetAll() {
	py::list result(regular_env.size());
	idx_t i = 0;
	for (auto &pair : regular_env) {
		auto &key = pair.first;
		auto &value = pair.second;

		py::tuple key_val(2);
		key_val[0] = key;
		key_val[1] = value;
		result[i] = std::move(key_val);
		i++;
	}
	return result;
}

shared_ptr<SparkConf> SparkConf::Set(const string &key, const string &value) {
	regular_env[key] = value;
}

static void SetEnvironmentVariables(case_insensitive_map_t<string> &env, const py::list &pairs) {
	for (auto &pair : pairs) {
		if (!py::isinstance<py::tuple>(pair)) {
			string actual_type = py::str(pair.get_type());
			throw InvalidInputException("Entries of the 'pairs' list should be tuples, not %s", actual_type);
		}
		py::tuple key_value = py::cast<py::tuple>(pair);
		if (key_value.size() != 2) {
			throw InvalidInputException(
			    "Expected to find a tuple containing a key and a value, but this tuple has %d members",
			    key_value.size());
		}
		string key = py::str(key_value[0]);
		string value = py::str(key_value[1]);
		env[key] = value;
	}
}

shared_ptr<SparkConf> SparkConf::SetAll(const py::list &pairs) {
	SetEnvironmentVariables(regular_env, pairs);
	return shared_from_this();
}

shared_ptr<SparkConf> SparkConf::SetAppName(const string &value) {
	application_name = value;
	return shared_from_this();
}

shared_ptr<SparkConf> SparkConf::SetExecutorEnv(const py::object &key, const py::object &value,
                                                const py::object &pairs) {
	bool key_is_none = py::none().is(key);
	bool value_is_none = py::none().is(value);
	bool pairs_is_none = py::none().is(pairs);
	if (!key_is_none && !value_is_none) {
		if (!pairs_is_none) {
			throw InvalidInputException("Either provide a key and a value or a list of pairs, not both");
		}
		string key_str = py::str(key);
		string value_str = py::str(value);
		executor_env[key_str] = value_str;
	} else if (!pairs_is_none) {
		if (!key_is_none || !value_is_none) {
			throw InvalidInputException("Either provide a key and a value or a list of pairs, not both");
		}
		auto pairs_list = py::cast<py::list>(pairs);
		SetEnvironmentVariables(executor_env, pairs_list);
	}
	return shared_from_this();
}

shared_ptr<SparkConf> SparkConf::SetIfMissing(const string &key, const string &value) {
	if (regular_env.count(key)) {
		return shared_from_this();
	}
	regular_env[key] = value;
	return shared_from_this();
}

shared_ptr<SparkConf> SparkConf::SetMaster(const string &value) {
	master_url = value;
	return shared_from_this();
}

shared_ptr<SparkConf> SparkConf::SetSparkHome(const string &value) {
	spark_home = value;
	return shared_from_this();
}

string SparkConf::ToDebugString() {
	// TODO: implement this
	return "";
}

} // namespace spark
} // namespace duckdb
