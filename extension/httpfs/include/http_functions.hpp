//===----------------------------------------------------------------------===//
//                         DuckDB
//
// inet_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace duckdb {

struct HTTPFunctions {
	static void GetQueryParamFunction(DataChunk &args, ExpressionState &state, Vector &result);
};

} // namespace duckdb
