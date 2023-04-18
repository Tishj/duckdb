#include "duckdb_python/spark/session.hpp"
#include "duckdb_python/spark/session/builder.hpp"
#include "duckdb/common/exception.hpp"

#include "duckdb/common/common.hpp"

namespace duckdb {
namespace spark {

void SessionBuilder::Initialize(py::handle &m) {
	auto builder_module = py::class_<SessionBuilder, shared_ptr<SessionBuilder>>(m, "Builder", py::module_local());

	// constructor
	builder_module.def(py::init([]() { return make_shared<SessionBuilder>(); }));
	builder_module.def("appName", &SessionBuilder::AppName,
	                   "Sets a name for the application, which will be shown in the Spark web UI.", py::arg("name"));
	builder_module.def("enableHiveSupport", &SessionBuilder::EnableHiveSupport,
	                   "Enables Hive support, including connectivity to a persistent Hive metastore, support for Hive "
	                   "SerDes, and Hive user-defined functions.");
	builder_module.def("getOrCreate", &SessionBuilder::GetOrCreate,
	                   "Gets an existing SparkSession or, if there is no existing one, creates a new one based on the "
	                   "options set in this builder.");
	builder_module.def("master", &SessionBuilder::Master,
	                   "Sets the Spark master URL to connect to, such as “local” to run locally, “local[4]” to run "
	                   "locally with 4 cores, or “spark://master:7077” to run on a Spark standalone cluster.",
	                   py::arg("master"));
	builder_module.def("config", &SessionBuilder::AddConfigOption, "Sets a config option.", py::arg("key") = py::none(),
	                   py::arg("value") = py::none(), py::arg("conf") = py::none());
}

shared_ptr<SessionBuilder> SessionBuilder::AppName(const string &name) {
	this->name = name;
	return shared_from_this();
}

shared_ptr<SessionBuilder> SessionBuilder::AddConfigOption(const py::object &key_p, const py::object &value_p,
                                                           const py::object &config_p) {
	bool key_value_supplied = !py::none().is(key_p) && !py::none().is(value_p);
	bool config_supplied = !py::none().is(config_p);

	if (!key_value_supplied && !config_supplied) {
		throw InvalidInputException("Either supply a 'conf' or both a 'key' and a 'value' option");
	}

	if (key_value_supplied) {
		if (!py::isinstance<py::str>(key_p)) {
			string actual_type = py::str(key_p.get_type());
			throw InvalidInputException("The provided 'key' has to be of type str, not of type '%s'", actual_type);
		}
		string key = py::str(key_p);
		if (!py::isinstance<py::str>(value_p)) {
			string actual_type = py::str(value_p.get_type());
			throw InvalidInputException("The provided 'value' has to be of type str, not of type '%s'", actual_type);
		}
		string value = py::str(value_p);
		config_map[key] = std::move(value);
	} else {
		// TODO: Get the internal config map from the SparkConf object
	}
	return shared_from_this();
}

shared_ptr<SessionBuilder> SessionBuilder::EnableHiveSupport() {
	hive_support_enabled = true;
	return shared_from_this();
}

shared_ptr<SessionBuilder> SessionBuilder::Master(const string &name) {
	cluster_url = name;
	return shared_from_this();
}

shared_ptr<SparkSession> SessionBuilder::GetOrCreate() {
	// TODO: add a cache to store SparkSession instances by their builder config
	return make_shared<SparkSession>(name, config_map, hive_support_enabled, cluster_url);
}

} // namespace spark
} // namespace duckdb
