#include "duckdb_python/spark/session.hpp"
#include "duckdb_python/spark/session/builder.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {
namespace spark {

void SparkSession::Initialize(py::handle &m) {
	auto spark_session = py::class_<SparkSession, shared_ptr<SparkSession>>(m, "SparkSession", py::module_local());
	SessionBuilder::Initialize(spark_session);
}

SparkSession::SparkSession(const string &name, const unordered_map<string, string> &configuration,
                           bool hive_support_enabled, const string &cluster_url)
    : name(name), configuration(configuration), hive_support_enabled(hive_support_enabled), cluster_url(cluster_url) {
}

} // namespace spark
} // namespace duckdb
