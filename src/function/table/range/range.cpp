#include "duckdb/function/table/range.hpp"
#include "duckdb/function/table/summary.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

struct RangeInOutFunctionState : public GlobalTableFunctionState {
	using range_t = int32_t;
	using increment_t = int32_t;

	RangeInOutFunctionState() : new_row(true), input_idx(0), range_idx(0) {
	}

	struct RangeSettings {
		range_t start;
		range_t end;
		increment_t increment;
		//! The size of the range for the current input
		idx_t size;
	};

	enum class SettingType { START, END, INCREMENT };

	RangeSettings settings;

	template <SettingType SETTING, class TYPE>
	struct SettingContainer {
	public:
		SettingContainer() : initialized(false) {
		}
		UnifiedVectorFormat format;
		bool initialized = false;

	public:
		void Set(idx_t count, Vector &vec) {
			vec.ToUnifiedFormat(count, format);
			initialized = true;
		}
		TYPE Get(idx_t idx) {
			if (initialized) {
				idx = format.sel->get_index(idx);
				return ((TYPE *)format.data)[idx];
			}
			if (SETTING == SettingType::START) {
				return 0;
			}
			if (SETTING == SettingType::INCREMENT) {
				return 1;
			}
			throw InternalException("'end' of a range should always be given");
		}
	};

	SettingContainer<SettingType::START, range_t> start_data;
	SettingContainer<SettingType::END, range_t> end_data;
	SettingContainer<SettingType::INCREMENT, increment_t> increment_data;

public:
	RangeSettings &GetCurrentSettings(DataChunk &chunk) {
		if (input_idx == 0) {
			// Initialize the vector formats for the entire chunk
			if (chunk.ColumnCount() == 1) {
				end_data.Set(chunk.size(), chunk.data[0]);
			} else if (chunk.ColumnCount() == 2) {
				start_data.Set(chunk.size(), chunk.data[0]);
				end_data.Set(chunk.size(), chunk.data[1]);
			} else if (chunk.ColumnCount() == 3) {
				start_data.Set(chunk.size(), chunk.data[0]);
				end_data.Set(chunk.size(), chunk.data[1]);
				increment_data.Set(chunk.size(), chunk.data[2]);
			} else {
				throw InvalidInputException("'range' expects 1, 2 or 3 arguments!");
			}
		}
		if (range_idx == 0) {
			// New range starts, need to refresh the current settings
			settings.start = start_data.Get(input_idx);
			settings.end = end_data.Get(input_idx);
			settings.increment = increment_data.Get(input_idx);

			int64_t offset = settings.increment < 0 ? 1 : -1;
			settings.size = Hugeint::Cast<idx_t>((settings.end + (settings.increment + offset)) / settings.increment);
		}
		return settings;
	}

	OperatorResultType Update(idx_t written_tuples, idx_t input_size) {
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

	idx_t GetRemaining(const RangeSettings &settings, idx_t written_tuples) {
		D_ASSERT(range_idx < settings.size);
		D_ASSERT(STANDARD_VECTOR_SIZE > written_tuples);
		return MinValue<idx_t>(settings.size - range_idx, STANDARD_VECTOR_SIZE - written_tuples);
	}

	bool new_row;
	idx_t input_idx;
	idx_t range_idx;
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

	using range_t = RangeInOutFunctionState::range_t;
	idx_t total_written_tuples = 0;
	idx_t written_tuples = 0;
	OperatorResultType result;

	// Either we reach the end of the input chunk, or we have written an entire chunk of output
	while ((result = state.Update(written_tuples, input.size())) != OperatorResultType::NEED_MORE_INPUT &&
	       total_written_tuples < STANDARD_VECTOR_SIZE) {
		auto &settings = state.GetCurrentSettings(input);
		auto &increment = settings.increment;
		auto &start = settings.start;

		auto remaining = state.GetRemaining(settings, total_written_tuples);

		range_t current_value = start + increment * state.range_idx;
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
				auto idx = output_data.sel->get_index(i + total_written_tuples);
				result_data[idx] = current_value_i64 + (increment_i64 * i);
			}
		}
		written_tuples = remaining;
		total_written_tuples += written_tuples;
	}

	output.SetCardinality(total_written_tuples);
	return result;
}

void RangeInOutTableFunction::RegisterFunction(TableFunctionSet &set) {
	TableFunction range_function({LogicalType::TABLE}, nullptr, RangeFunctionBind<false>, RangeFunctionInit);
	range_function.in_out_function = RangeFunction;
	set.AddFunction(range_function);
}

} // namespace duckdb
