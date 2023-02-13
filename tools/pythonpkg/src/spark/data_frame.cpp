#include "duckdb_python/spark/data_frame.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/string.hpp"

#include "duckdb_python/pyconnection.hpp"

namespace duckdb {
namespace spark {

void DataFrame::Initialize(py::handle &m) {
	auto data_frame = py::class_<DataFrame, shared_ptr<DataFrame>>(m, "DataFrame", py::module_local());
}

DataFrame::DataFrame(shared_ptr<DuckDBPyRelation> relation) : relation(std::move(relation)) {
}

} // namespace spark
} // namespace duckdb
