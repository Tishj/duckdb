//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/range.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct CheckpointFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct GlobTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RangeTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RepeatTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct UnnestTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RangeInOutTableFunction {
	static void RegisterFunction(TableFunctionSet &set);
};

struct GenerateSeriesInOutTableFunction {
	static void RegisterFunction(TableFunctionSet &set);
};

} // namespace duckdb
