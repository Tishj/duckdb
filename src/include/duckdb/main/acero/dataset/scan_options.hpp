#pragma once

#include "duckdb/main/acero/compute/expression.hpp"

namespace duckdb {
namespace arrow {
namespace dataset {

struct ScanOptions {

	cp::Expression projection;
};

} // namespace dataset
} // namespace arrow
} // namespace duckdb
