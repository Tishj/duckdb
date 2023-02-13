#include "duckdb_python/spark/session.hpp"

#include "duckdb_python/spark/catalog.hpp"
#include "duckdb_python/spark/session/builder.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/string.hpp"

#include "duckdb_python/pyconnection.hpp"

namespace duckdb {
namespace spark {

void SparkSession::Initialize(py::handle &m) {
	auto spark_session = py::class_<SparkSession, shared_ptr<SparkSession>>(m, "SparkSession", py::module_local());
	SessionBuilder::Initialize(spark_session);

	spark_session.def("catalog", &SparkSession::GetCatalog,
	                  "Interface through which the user may create, drop, alter or query underlying databases, tables, "
	                  "functions, etc.");
}

// TODO: as part of the Session we likely want to start a DuckDBPyConnection
SparkSession::SparkSession(const string &name, const unordered_map<string, string> &configuration,
                           bool hive_support_enabled, const string &cluster_url)
    : name(name), configuration(configuration), hive_support_enabled(hive_support_enabled), cluster_url(cluster_url) {
	// TODO: Transform the configuration into a py::dict of config options
	connection = duckdb::DuckDBPyConnection::Connect(name, false, py::none());
	catalog = make_shared<Catalog>(connection);
}

// This also inherits the DuckDBPyConnection from the Session
shared_ptr<Catalog> SparkSession::GetCatalog() {
	return catalog;
}

} // namespace spark
} // namespace duckdb
