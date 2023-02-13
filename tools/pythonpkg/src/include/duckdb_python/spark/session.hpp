#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {
namespace spark {

class SparkSession {
public:
	static void Initialize(py::handle &m);
	class Builder {};

private:
};

} // namespace spark
} // namespace duckdb
