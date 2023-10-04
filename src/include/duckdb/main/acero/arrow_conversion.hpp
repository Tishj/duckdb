#pragma once

#include "duckdb/main/acero/dataset/table.hpp"

namespace duckdb {
namespace ac {

struct ArrowConversion {
public:
	static shared_ptr<arrow::Table> ConvertToTable(unique_ptr<QueryResult> result);
};

} // namespace ac
} // namespace duckdb
