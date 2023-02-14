#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb_python/spark/duckdb_spark.hpp"

#include "duckdb_python/spark/session.hpp"
#include "duckdb_python/spark/catalog.hpp"

namespace py = pybind11;

namespace duckdb {
namespace spark {

void DuckDBSpark::Initialize(py::handle &m) {
	auto spark_module =
	    ((py::module_ &)m).def_submodule("spark", "DuckDB.spark is a local staging testing ground for PySpark.");

	SparkSession::Initialize(spark_module);
	Catalog::Initialize(spark_module);
}

} // namespace spark
} // namespace duckdb
