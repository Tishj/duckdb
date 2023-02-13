#include "duckdb_python/spark/session.hpp"

namespace duckdb {
namespace spark {

void SparkSession::Initialize(py::handle &m) {
	auto spark_session = py::class_<SparkSession, shared_ptr<SparkSession>>(m, "SparkSession", py::module_local());
}

} // namespace spark
} // namespace duckdb
