#include "duckdb/function/table/range.hpp"
#include "duckdb/function/table/summary.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

struct RangeInOutFunctionState : public GlobalTableFunctionState {
	RangeInOutFunctionState() : current_idx(0) {
	}

	int64_t current_idx;
};

static unique_ptr<GlobalTableFunctionState> RangeFunctionInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_unique<RangeInOutFunctionState>();
}

template <bool GENERATE_SERIES>
static unique_ptr<FunctionData> RangeFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	if (!GENERATE_SERIES) {
		names.emplace_back("range");
	} else {
		names.emplace_back("generate_series");
	}
	return_types.emplace_back(LogicalType::BIGINT);
	return make_unique<TableFunctionData>();
}

static OperatorResultType RangeFunction(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                        DataChunk &output) {
	auto &state = (RangeInOutFunctionState &)*data_p.global_state;

	D_ASSERT(input.ColumnCount() >= 1);
	// auto increment = bind_data.increment;
	// auto end = bind_data.end;
	// hugeint_t current_value = bind_data.start + increment * state.current_idx;
	// int64_t current_value_i64;
	// if (!Hugeint::TryCast<int64_t>(current_value, current_value_i64)) {
	//	return;
	// }
	// int64_t offset = increment < 0 ? 1 : -1;
	// idx_t remaining = MinValue<idx_t>(Hugeint::Cast<idx_t>((end - current_value + (increment + offset)) / increment),
	//                                   STANDARD_VECTOR_SIZE);
	//// set the result vector as a sequence vector
	// output.data[0].Sequence(current_value_i64, Hugeint::Cast<int64_t>(increment), remaining);
	//// increment the index pointer by the remaining count
	// state.current_idx += remaining;
	// output.SetCardinality(remaining);

	return OperatorResultType::NEED_MORE_INPUT;
}

void RangeInOutTableFunction::RegisterFunction(TableFunctionSet &set) {
	TableFunction range_function({LogicalType::TABLE}, nullptr, RangeFunctionBind<false>, RangeFunctionInit);
	range_function.in_out_function = RangeFunction;
	set.AddFunction(range_function);
}

} // namespace duckdb
