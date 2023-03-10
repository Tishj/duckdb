#include "duckdb/function/table/range.hpp"
#include "duckdb/function/table/summary.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table/range/range_inout_execution.hpp"

namespace duckdb {

namespace range {

OperatorResultType RangeIntExecutor::Update(idx_t written_tuples, idx_t input_size) {
	if (written_tuples == 0) {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
	if (range_idx + written_tuples == settings.size) {
		input_idx++;
		range_idx = 0;
	}
	if (input_idx == input_size) {
		input_idx = 0;
		return OperatorResultType::NEED_MORE_INPUT;
	}
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

idx_t RangeIntExecutor::Execute(ExecutionContext &context, DataChunk &input, DataChunk &output, idx_t total_written) {
	auto &settings = GetCurrentSettings(input);
	auto &increment = settings.increment;
	auto &start = settings.start;

	auto remaining = GetRemaining(settings, total_written);

	range_t current_value = start + increment * range_idx;
	int64_t current_value_i64;
	if (!Hugeint::TryCast<int64_t>(current_value, current_value_i64)) {
		throw InvalidInputException("Range value exceeds the capacity of BIGINT");
	}
	auto increment_i64 = Hugeint::Cast<int64_t>(increment);
	if (remaining == STANDARD_VECTOR_SIZE) {
		// We can write a sequence vector to be efficient, the entire output is populated by one range
		output.data[0].Sequence(current_value_i64, Hugeint::Cast<int64_t>(increment), remaining);
	} else {
		// FIXME: might be faster to also return a sequence vector and just return HAVE MORE OUTPUT?
		UnifiedVectorFormat output_data;
		output.data[0].ToUnifiedFormat(remaining, output_data);
		auto result_data = (int64_t *)(output_data.data);
		for (idx_t i = 0; i < remaining; i++) {
			auto idx = output_data.sel->get_index(i + total_written);
			result_data[idx] = current_value_i64 + (increment_i64 * i);
		}
	}
	return remaining;
}

struct RangeInOutFunctionState : public GlobalTableFunctionState {
	//! The executor created for the given input types
	//! can only be set once we have entered execution
	unique_ptr<RangeExecutor> executor;

	RangeExecutor &GetExecutor(DataChunk &input) {
		if (!executor) {
			executor = MakeExecutor(input);
		}
		return *executor;
	}

private:
	unique_ptr<RangeExecutor> MakeExecutor(DataChunk &input) {
		if (input.data[0].GetType() == LogicalType::INTEGER) {
			return make_unique<RangeIntExecutor>();
		} else {
			throw NotImplementedException("Range is not implemented as a table in-out function for this type!");
		}
	}
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

	auto &executor = state.GetExecutor(input);
	idx_t total_written_tuples = 0;
	idx_t written_tuples = 0;
	OperatorResultType result;

	// Either we reach the end of the input chunk, or we have written an entire chunk of output
	while ((result = executor.Update(written_tuples, input.size())) != OperatorResultType::NEED_MORE_INPUT &&
	       total_written_tuples < STANDARD_VECTOR_SIZE) {

		written_tuples = executor.Execute(context, input, output, total_written_tuples);
		total_written_tuples += written_tuples;
	}

	output.SetCardinality(total_written_tuples);
	return result;
}

} // namespace range

void RangeInOutTableFunction::RegisterFunction(TableFunctionSet &set) {
	TableFunction range_function({LogicalType::TABLE}, nullptr, range::RangeFunctionBind<false>,
	                             range::RangeFunctionInit);
	range_function.in_out_function = range::RangeFunction;
	set.AddFunction(range_function);
}

} // namespace duckdb
