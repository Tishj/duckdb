#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb_python/pyrelation.hpp"

namespace duckdb {
namespace spark {

// This is a wrapper/adapter for the DuckDBPyRelation class
class DataFrame {
public:
	DataFrame(shared_ptr<DuckDBPyRelation> relation);

private:
	shared_ptr<DuckDBPyRelation> relation;
};

} // namespace spark
} // namespace duckdb
