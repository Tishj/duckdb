//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/range/range_inout_execution.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

namespace range {

template <class START_TYPE, class END_TYPE, class INCREMENT_TYPE>
struct RangeSettings {
	START_TYPE start;
	END_TYPE end;
	INCREMENT_TYPE increment;
	//! The size of the range for the current input
	idx_t size;
};

template <class TYPE>
struct SettingContainer {
public:
	SettingContainer(TYPE default_value) : initialized(false), default_value(std::move(default_value)) {
	}
	UnifiedVectorFormat format;
	bool initialized = false;
	TYPE default_value;

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
		return default_value;
	}
};

class RangeExecutor {
public:
	RangeExecutor() : new_row(false), input_idx(0), range_idx(0) {
	}
	virtual ~RangeExecutor() {
	}

public:
	virtual OperatorResultType Update(idx_t written_tuples, idx_t input_size) = 0;
	virtual idx_t Execute(ExecutionContext &context, DataChunk &input, DataChunk &output, idx_t total_written) = 0;

protected:
	bool new_row;
	idx_t input_idx;
	idx_t range_idx;
};

} // namespace range

} // namespace duckdb
