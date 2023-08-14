#pragma once

#include "duckdb/function/table/range/range_executor.hpp"

namespace duckdb {

namespace range {

template <bool GENERATE_SERIES>
class RangeIntExecutor : public RangeExecutor {
	using IntRange = RangeSettings<hugeint_t, hugeint_t, hugeint_t>;

public:
	RangeIntExecutor(ClientContext &context, vector<unique_ptr<Expression>> args_list_p)
	    : RangeExecutor(context, std::move(args_list_p)), settings(), start_data(0, GENERATE_SERIES ? 1 : 0),
	      end_data(0, 0), increment_data(1, 1) {
	}

	~RangeIntExecutor() {
	}

public:
	OperatorResultType ForwardInput(idx_t input_size) {
		D_ASSERT(input_idx != input_size);
		input_idx++;
		range_idx = 0;
		if (input_idx == input_size) {
			input_idx = 0;
			return OperatorResultType::NEED_MORE_INPUT;
		}
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}

	OperatorResultType Update(idx_t written_tuples, idx_t input_size) {
		range_idx += written_tuples;
		if (range_idx != settings.size) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		return ForwardInput(input_size);
	}

	idx_t Execute(ExecutionContext &context, DataChunk &input, DataChunk &output, idx_t total_written, bool &is_null) {
		auto &settings = GetCurrentSettings(input);
		if (settings.null) {
			is_null = true;
			return 0;
		}
		auto &increment = settings.increment;
		auto &start = settings.start;

		auto remaining = GetRemaining(settings, total_written);

		hugeint_t current_value = start + increment * range_idx;
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

private:
	void VerifySettings(IntRange &settings) {
		if (settings.null) {
			return;
		}
		if (settings.increment == 0) {
			throw BinderException("interval cannot be 0!");
		}
		if (settings.start > settings.end && settings.increment > 0) {
			throw BinderException(
			    "start is bigger than end, but increment is positive: cannot generate infinite series");
		} else if (settings.start < settings.end && settings.increment < 0) {
			throw BinderException(
			    "start is smaller than end, but increment is negative: cannot generate infinite series");
		}
	}

	IntRange &GetCurrentSettings(DataChunk &chunk) {
		if (input_idx == 0) {
			args_data.Reset();
			expr_executor.Execute(chunk, args_data);

			// Initialize the vector formats for the entire chunk
			if (args_data.ColumnCount() == 1) {
				end_data.Set(args_data.size(), args_data.data[0]);
			} else if (args_data.ColumnCount() == 2) {
				start_data.Set(args_data.size(), args_data.data[0]);
				end_data.Set(args_data.size(), args_data.data[1]);
			} else if (args_data.ColumnCount() == 3) {
				start_data.Set(args_data.size(), args_data.data[0]);
				end_data.Set(args_data.size(), args_data.data[1]);
				increment_data.Set(args_data.size(), args_data.data[2]);
			} else {
				throw InvalidInputException("'range' expects 1, 2 or 3 arguments!");
			}
		}
		if (range_idx == 0) {
			// New range starts, need to refresh the current settings
			settings.null = false;
			settings.start = start_data.Get(input_idx, settings.null);
			settings.end = end_data.Get(input_idx, settings.null);
			settings.increment = increment_data.Get(input_idx, settings.null);

			if (GENERATE_SERIES) {
				if (settings.increment < 0) {
					settings.end = settings.end - 1;
				} else {
					settings.end = settings.end + 1;
				}
			}

			VerifySettings(settings);
			if (settings.null) {
				return settings;
			}

			int64_t offset = settings.increment < 0 ? 1 : -1;
			settings.size = Hugeint::Cast<idx_t>((settings.end - settings.start + (settings.increment + offset)) /
			                                     settings.increment);
		}
		return settings;
	}

	idx_t GetRemaining(const IntRange &settings, idx_t written_tuples) {
		D_ASSERT(range_idx <= settings.size);
		D_ASSERT(STANDARD_VECTOR_SIZE > written_tuples);
		return MinValue<idx_t>(settings.size - range_idx, STANDARD_VECTOR_SIZE - written_tuples);
	}

private:
	IntRange settings;
	SettingContainer<int64_t> start_data;
	SettingContainer<int64_t> end_data;
	SettingContainer<int64_t> increment_data;
};

} // namespace range

} // namespace duckdb
