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

namespace duckdb {

namespace range {

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
	unique_ptr<RangeExecutor> MakeNumericExecutor(const LogicalType &type) {
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
			return make_unique<RangeIntExecutor<int8_t>>();
		case LogicalTypeId::SMALLINT:
			return make_unique<RangeIntExecutor<int16_t>>();
		case LogicalTypeId::INTEGER:
			return make_unique<RangeIntExecutor<int32_t>>();
		case LogicalTypeId::BIGINT:
			return make_unique<RangeIntExecutor<int64_t>>();
		case LogicalTypeId::FLOAT:
			return make_unique<RangeIntExecutor<float>>();
		case LogicalTypeId::DOUBLE:
			return make_unique<RangeIntExecutor<double>>();
		case LogicalTypeId::UTINYINT:
			return make_unique<RangeIntExecutor<uint8_t>>();
		case LogicalTypeId::USMALLINT:
			return make_unique<RangeIntExecutor<uint16_t>>();
		case LogicalTypeId::UINTEGER:
			return make_unique<RangeIntExecutor<uint32_t>>();
		case LogicalTypeId::UBIGINT:
			return make_unique<RangeIntExecutor<uint64_t>>();
		default:
			// Explicitly ignored:
			// hugeint
			// decimal
			throw NotImplementedException("Range is not implemented as a table in-out function for '%s'",
			                              type.ToString());
		}
	}

	unique_ptr<RangeExecutor> MakeExecutor(DataChunk &input) {
		auto type = input.data[0].GetType();
		if (input.ColumnCount() > 3) {
			throw InvalidInputException("Range takes up to 3 arguments, not %d", input.ColumnCount());
		}
		if (type.IsNumeric()) {
			for (idx_t i = 1; i < input.ColumnCount(); i++) {
				if (input.data[i].GetType() != type) {
					throw InvalidInputException(
					    "All of the arguments passed to the numeric RANGE function have to be of the same type");
				}
			}
			return MakeNumericExecutor(type);
		} else if (input.ColumnCount() == 3 && type.id() == LogicalTypeId::TIMESTAMP) {
			if (input.data[1].GetType() != type) {
				throw InvalidInputException("Provided start and end column types don't match!");
			}
			if (input.data[2].GetType() != LogicalTypeId::INTERVAL) {
				throw InvalidInputException("Increment column has to be of type INTERVAL!");
			}
			return make_unique<RangeTimestampExecutor<false>>();
		} else {
			throw NotImplementedException("Range is not implemented as a table in-out function for '%s'",
			                              type.ToString());
		}
	}
};

static unique_ptr<GlobalTableFunctionState> RangeFunctionInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_unique<RangeInOutFunctionState>();
}

template <bool GENERATE_SERIES>
static unique_ptr<FunctionData> RangeFunctionBindInternal(LogicalType return_type, vector<LogicalType> &return_types,
                                                          vector<string> &names) {
	if (!GENERATE_SERIES) {
		names.emplace_back("range");
	} else {
		names.emplace_back("generate_series");
	}
	return_types.emplace_back(return_type);
	return make_unique<TableFunctionData>();
}

template <bool GENERATE_SERIES>
static unique_ptr<FunctionData> RangeIntFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	return RangeFunctionBindInternal<GENERATE_SERIES>(LogicalType::BIGINT, return_types, names);
}

template <bool GENERATE_SERIES>
static unique_ptr<FunctionData> RangeTimestampFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	return RangeFunctionBindInternal<GENERATE_SERIES>(LogicalType::TIMESTAMP, return_types, names);
}

static OperatorResultType RangeFunction(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                        DataChunk &output) {
	auto &state = (RangeInOutFunctionState &)*data_p.global_state;

	auto &executor = state.GetExecutor(input);
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

} // namespace range

void RangeInOutTableFunction::RegisterFunction(TableFunctionSet &set) {

	// range(BIGINT);
	TableFunction range_function({LogicalType::BIGINT}, nullptr, range::RangeIntFunctionBind<false>,
	                             range::RangeFunctionInit);
	range_function.in_out_function = range::RangeFunction;
	set.AddFunction(range_function);

	// range(BIGINT, BIGINT, BIGINT);
	range_function.arguments = {LogicalType::BIGINT, LogicalType::BIGINT};
	set.AddFunction(range_function);

	// range(BIGINT, BIGINT, BIGINT);
	range_function.arguments = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
	set.AddFunction(range_function);

	// range(TIMESTAMP, TIMESTAMP, INTERVAL);
	range_function.arguments = {LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::INTERVAL};
	range_function.bind = range::RangeTimestampFunctionBind<false>;
	set.AddFunction(range_function);
}

} // namespace duckdb
