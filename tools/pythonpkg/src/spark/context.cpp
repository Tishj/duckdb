#include "duckdb_python/spark/context.hpp"

#include "duckdb_python/spark/catalog.hpp"
#include "duckdb_python/spark/session/builder.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/string.hpp"

#include "duckdb_python/pyconnection/pyconnection.hpp"

namespace duckdb {
namespace spark {

void SparkContext::Initialize(py::handle &m) {
	auto spark_context = py::class_<SparkContext, shared_ptr<SparkContext>>(m, "SparkContext", py::module_local());

	// Constructor
	spark_context.def(
	    py::init([](const string &master, const string &app_name, const string &spark_home, const py::object &py_files,
	                const py::object &environment, const py::object &batch_size, const py::object &serializer,
	                const py::object &conf, const py::object &gateway, const py::object &jsc,
	                const py::object &profiler_cls, const py::object &udf_profiler_cls) {
		    return make_shared<SparkContext>(master, app_name, spark_home, py_files, environment, batch_size,
		                                     serializer, conf, gateway, jsc, profiler_cls, udf_profiler_cls);
	    }),
	    py::arg("master") = string(), py::arg("appName") = string(), py::arg("sparkHome") = string(),
	    py::arg("pyFiles") = py::none(), py::arg("environment") = py::none(), py::arg("batchSize") = py::none(),
	    py::arg("serializer") = py::none(), py::arg("conf") = py::none(), py::arg("gateway") = py::none(),
	    py::arg("jsc") = py::none(), py::arg("profiler_cls") = py::none(), py::arg("udf_profiler_cls") = py::none());

	// spark_context.def("contains", &SparkContext::Contains, py::arg("key"));
	// spark_context.def("get", &SparkContext::Get, py::arg("key"), py::arg("default_value") = py::none());
	// spark_context.def("getAll", &SparkContext::GetAll);
	// spark_context.def("set", &SparkContext::Set, py::arg("key"), py::arg("value"));
	// spark_context.def("setAll", &SparkContext::SetAll, py::arg("pairs"));
	// spark_context.def("setAll", &SparkContext::SetAll, py::arg("pairs"));
	// spark_context.def("setAppName", &SparkContext::SetAppName, py::arg("value"));
	// spark_context.def("setExecutorEnv", &SparkContext::SetExecutorEnv, py::arg("key") = string(),
	//                   py::arg("value") = string(), py::arg("pairs") = py::none());
	// spark_context.def("setIfMissing", &SparkContext::SetIfMissing, py::arg("key"), py::arg("value"));
	// spark_context.def("setMaster", &SparkContext::SetMaster, py::arg("value"));
	// spark_context.def("setSparkHome", &SparkContext::SetSparkHome, py::arg("value"));
	// spark_context.def("toDebugString", &SparkContext::ToDebugString);
}

SparkContext::SparkContext(const string &master, const string &app_name, const string &spark_home,
                           const py::object &py_files, const py::object &environment, const py::object &batch_size,
                           const py::object &serializer, const py::object &conf, const py::object &gateway,
                           const py::object &jsc, const py::object &profiler_cls, const py::object &udf_profiler_cls)
    : master(master) {
}

// Member methods

} // namespace spark
} // namespace duckdb
