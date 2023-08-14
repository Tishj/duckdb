#include "duckdb/function/table/range.hpp"
#include "duckdb/function/table/summary.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table/range/range_executor.hpp"
#include "duckdb/function/table/range/range_int_executor.hpp"
#include "duckdb/function/table/range/range_timestamp_executor.hpp"
#include "duckdb/function/table/range/range_function_bind_data.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

namespace range {

template <bool GENERATE_SERIES>
struct RangeInOutTimestampFunctionState : public GlobalTableFunctionState {
	RangeInOutTimestampFunctionState(ClientContext &context, vector<unique_ptr<Expression>> args_list_p)
	    : executor(context, std::move(args_list_p)) {
	}
	RangeTimestampExecutor<GENERATE_SERIES> executor;
};

template <bool GENERATE_SERIES>
struct RangeInOutNumericFunctionState : public GlobalTableFunctionState {
	RangeInOutNumericFunctionState(ClientContext &context, vector<unique_ptr<Expression>> args_list_p)
	    : executor(context, std::move(args_list_p)) {
	}
	RangeIntExecutor<GENERATE_SERIES> executor;
};

template <bool GENERATE_SERIES>
static unique_ptr<GlobalTableFunctionState> RangeFunctionNumericInit(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto bind_data = input.bind_data->Cast<RangeFunctionBindData>();

	vector<unique_ptr<Expression>> args_list;
	// initialize the global state's args_list with bound expressions referencing the input columns
	for (idx_t i = 0; i < bind_data.input_types.size(); i++) {
		auto expr = make_uniq_base<Expression, BoundReferenceExpression>(bind_data.input_types[i], i);
		args_list.push_back(std::move(expr));
	}
	return make_uniq<RangeInOutNumericFunctionState<GENERATE_SERIES>>(context, std::move(args_list));
}

template <bool GENERATE_SERIES>
static unique_ptr<GlobalTableFunctionState> RangeFunctionTimestampInit(ClientContext &context,
                                                                       TableFunctionInitInput &input) {
	auto bind_data = input.bind_data->Cast<RangeFunctionBindData>();

	vector<unique_ptr<Expression>> args_list;
	// initialize the global state's args_list with bound expressions referencing the input columns
	for (idx_t i = 0; i < bind_data.input_types.size(); i++) {
		auto expr = make_uniq_base<Expression, BoundReferenceExpression>(bind_data.input_types[i], i);
		args_list.push_back(std::move(expr));
	}
	return make_uniq<RangeInOutTimestampFunctionState<GENERATE_SERIES>>(context, std::move(args_list));
}

template <bool GENERATE_SERIES>
static unique_ptr<FunctionData> RangeFunctionBindInternal(const LogicalType &return_type,
                                                          vector<LogicalType> &return_types, vector<string> &names,
                                                          TableFunctionBindInput &input) {
	if (!GENERATE_SERIES) {
		names.emplace_back("range");
	} else {
		names.emplace_back("generate_series");
	}
	return_types.emplace_back(return_type);
	return make_uniq<RangeFunctionBindData>(input.input_table_types);
}

template <bool GENERATE_SERIES>
static unique_ptr<FunctionData> RangeIntFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	return RangeFunctionBindInternal<GENERATE_SERIES>(LogicalType::BIGINT, return_types, names, input);
}

template <bool GENERATE_SERIES>
static unique_ptr<FunctionData> RangeTimestampFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	return RangeFunctionBindInternal<GENERATE_SERIES>(LogicalType::TIMESTAMP, return_types, names, input);
}

template <class EXECUTOR>
static OperatorResultType RangeFunctionInternal(ExecutionContext &context, EXECUTOR &executor, DataChunk &input,
                                                DataChunk &output) {
	idx_t total_written_tuples = 0;
	idx_t written_tuples = 0;
	OperatorResultType result;

	bool is_null = false;
	// Either we reach the end of the input chunk, or we have written an entire chunk of output
	do {
		if (total_written_tuples == STANDARD_VECTOR_SIZE) {
			break;
		}
		written_tuples = executor.Execute(context, input, output, total_written_tuples, is_null);
		if (is_null && executor.ForwardInput(input.size()) == OperatorResultType::NEED_MORE_INPUT) {
			return OperatorResultType::NEED_MORE_INPUT;
		}
		total_written_tuples += written_tuples;
	} while ((result = executor.Update(written_tuples, input.size())) != OperatorResultType::NEED_MORE_INPUT);

	output.SetCardinality(total_written_tuples);
	return result;
}

template <bool GENERATE_SERIES>
static OperatorResultType RangeFunctionNumeric(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                               DataChunk &output) {

	auto &state = (RangeInOutNumericFunctionState<GENERATE_SERIES> &)*data_p.global_state;

	auto &executor = state.executor;
	return RangeFunctionInternal<RangeIntExecutor<GENERATE_SERIES>>(context, executor, input, output);
}

template <bool GENERATE_SERIES>
static OperatorResultType RangeFunctionTimestamp(ExecutionContext &context, TableFunctionInput &data_p,
                                                 DataChunk &input, DataChunk &output) {
	auto &state = (RangeInOutTimestampFunctionState<GENERATE_SERIES> &)*data_p.global_state;

	auto &executor = state.executor;
	return RangeFunctionInternal<RangeTimestampExecutor<GENERATE_SERIES>>(context, executor, input, output);
}

} // namespace range

template <bool GENERATE_SERIES>
static void RegisterFunctionInternal(TableFunctionSet &set) {
	// range(BIGINT);
	TableFunction range_function({LogicalType::BIGINT}, nullptr, range::RangeIntFunctionBind<GENERATE_SERIES>,
	                             range::RangeFunctionNumericInit<GENERATE_SERIES>);
	range_function.in_out_function = range::RangeFunctionNumeric<GENERATE_SERIES>;
	set.AddFunction(range_function);

	// range(BIGINT, BIGINT);
	range_function.arguments = {LogicalType::BIGINT, LogicalType::BIGINT};
	set.AddFunction(range_function);

	// range(BIGINT, BIGINT, BIGINT);
	range_function.arguments = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
	set.AddFunction(range_function);

	// range(TIMESTAMP, TIMESTAMP, INTERVAL);
	range_function.arguments = {LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::INTERVAL};
	range_function.init_global = range::RangeFunctionTimestampInit<GENERATE_SERIES>;
	range_function.bind = range::RangeTimestampFunctionBind<GENERATE_SERIES>;
	range_function.in_out_function = range::RangeFunctionTimestamp<GENERATE_SERIES>;
	set.AddFunction(range_function);
}

void RangeInOutTableFunction::RegisterFunction(TableFunctionSet &set) {
	RegisterFunctionInternal<false>(set);
}

void GenerateSeriesInOutTableFunction::RegisterFunction(TableFunctionSet &set) {
	RegisterFunctionInternal<true>(set);
}

} // namespace duckdb
