#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb_python/spark/duckdb_spark.hpp"
#include "duckdb_python/spark/session.hpp"

namespace py = pybind11;

namespace duckdb {
namespace spark {

void DuckDBSpark::Initialize(py::handle &m) {
	auto spark_module = py::class_<DuckDBSpark, shared_ptr<DuckDBSpark>>(m, "spark", py::module_local());

	SparkSession::Initialize(spark_module);
}

} // namespace spark
} // namespace duckdb
