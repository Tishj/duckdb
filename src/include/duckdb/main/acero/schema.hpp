#pragma once

#include "duckdb/common/arrow/arrow_wrapper.hpp"

namespace duckdb {
namespace arrow {

struct Schema {
	ArrowSchemaWrapper wrapped;
};

} // namespace arrow
} // namespace duckdb
