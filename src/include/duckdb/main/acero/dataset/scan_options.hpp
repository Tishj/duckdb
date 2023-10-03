#pragma once

#include "duckdb/main/acero/compute/expression.hpp"

namespace duckdb {
namespace arrow {
namespace dataset {

struct ScanOptions {
	//  /// A row filter (which will be pushed down to partitioning/reading if supported).
	//  cp::Expression filter = cp::literal(true);
	/// A projection expression (which can add/remove/rename columns).
	cp::Expression projection;
};

} // namespace dataset
} // namespace arrow
} // namespace duckdb
