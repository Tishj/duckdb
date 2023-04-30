#include "duckdb/function/table/range.hpp"
#include "duckdb/function/table/summary.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

void RangeTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet range("range");
	RangeInOutTableFunction::RegisterFunction(range);
	set.AddFunction(range);

	TableFunctionSet generate_series("generate_series");
	GenerateSeriesInOutTableFunction::RegisterFunction(generate_series);
	set.AddFunction(generate_series);
}

void BuiltinFunctions::RegisterTableFunctions() {
	CheckpointFunction::RegisterFunction(*this);
	GlobTableFunction::RegisterFunction(*this);
	RangeTableFunction::RegisterFunction(*this);
	RepeatTableFunction::RegisterFunction(*this);
	SummaryTableFunction::RegisterFunction(*this);
	UnnestTableFunction::RegisterFunction(*this);
}

} // namespace duckdb
