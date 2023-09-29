#pragma once

#include "duckdb/function/table/range/range_executor.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/operator/add.hpp"

namespace duckdb {

namespace range {

struct TimeRange {
	timestamp_t start;
	timestamp_t end;
	interval_t increment;
	bool greater_than_check;
	idx_t size;
	timestamp_t current;
	bool null;
};

template <bool GENERATE_SERIES = false>
class RangeTimestampExecutor : public RangeExecutor {
	using range_t = timestamp_t;
	using increment_t = interval_t;

public:
	RangeTimestampExecutor(ClientContext &context, vector<unique_ptr<Expression>> args_list_p)
	    : RangeExecutor(context, std::move(args_list_p)), settings(), start_data(range_t(), range_t()),
	      end_data(range_t(), range_t()), increment_data(increment_t(), increment_t()) {
	}
	virtual ~RangeTimestampExecutor() {
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

		auto remaining = GetRemaining(settings, total_written);

		auto data = FlatVector::GetData<timestamp_t>(output.data[0]);
		auto current = settings.current;
		for (idx_t i = 0; i < remaining; i++) {
			data[i] = current;
			current = AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(current, increment);
		}
		settings.current = current;
		return remaining;
	}

private:
	void VerifySettings(TimeRange &settings) {
		if (settings.null) {
			return;
		}
		// Infinities either cause errors or infinite loops, so just ban them
		if (!Timestamp::IsFinite(settings.start) || !Timestamp::IsFinite(settings.end)) {
			throw BinderException("RANGE with infinite bounds is not supported");
		}

		if (settings.increment.months == 0 && settings.increment.days == 0 && settings.increment.micros == 0) {
			throw BinderException("interval cannot be 0!");
		}
		// all elements should point in the same direction
		if (settings.increment.months > 0 || settings.increment.days > 0 || settings.increment.micros > 0) {
			if (settings.increment.months < 0 || settings.increment.days < 0 || settings.increment.micros < 0) {
				throw BinderException("RANGE with composite interval that has mixed signs is not supported");
			}
			settings.greater_than_check = true;
			if (settings.start > settings.end) {
				throw BinderException(
				    "start is bigger than end, but increment is positive: cannot generate infinite series");
			}
		} else {
			settings.greater_than_check = false;
			if (settings.start < settings.end) {
				throw BinderException(
				    "start is smaller than end, but increment is negative: cannot generate infinite series");
			}
		}
	}

	template <bool GREATER_THAN>
	inline bool Finished(const timestamp_t &current_value, const timestamp_t &end) const {
		if (GREATER_THAN) {
			if (GENERATE_SERIES) {
				return current_value > end;
			} else {
				return current_value >= end;
			}
		} else {
			if (GENERATE_SERIES) {
				return current_value < end;
			} else {
				return current_value <= end;
			}
		}
	}

	template <bool GREATER_THAN>
	idx_t GetRangeSize(const TimeRange &settings) const {
		timestamp_t current = settings.start;
		idx_t size = 0;
		for (; !size || !Finished<GREATER_THAN>(current, settings.end); size++) {
			current = AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(current, settings.increment);
		}
		return size;
	}

	TimeRange &GetCurrentSettings(DataChunk &chunk) {
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
			settings.current = settings.start;

			VerifySettings(settings);
			if (settings.null) {
				return settings;
			}

			if (settings.greater_than_check) {
				settings.size = GetRangeSize<true>(settings);
			} else {
				settings.size = GetRangeSize<false>(settings);
			}
		}
		return settings;
	}

	idx_t GetRemaining(const TimeRange &settings, idx_t written_tuples) {
		D_ASSERT(range_idx < settings.size);
		D_ASSERT(STANDARD_VECTOR_SIZE > written_tuples);
		return MinValue<idx_t>(settings.size - range_idx, STANDARD_VECTOR_SIZE - written_tuples);
	}

private:
	TimeRange settings;
	SettingContainer<range_t> start_data;
	SettingContainer<range_t> end_data;
	SettingContainer<increment_t> increment_data;
};

} // namespace range

} // namespace duckdb
