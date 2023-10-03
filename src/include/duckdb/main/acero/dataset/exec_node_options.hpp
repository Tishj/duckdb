#pragma once

#include "duckdb/main/acero/options.hpp"

namespace duckdb {
namespace arrow {
namespace dataset {

struct ExecNodeOptions {
public:
	virtual ~ExecNodeOptions() = default;
};

} // namespace dataset
} // namespace arrow
} // namespace duckdb
