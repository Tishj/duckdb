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
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

namespace range {

template <class START_TYPE, class END_TYPE, class INCREMENT_TYPE>
struct RangeSettings {
	START_TYPE start;
	END_TYPE end;
	INCREMENT_TYPE increment;
	//! The size of the range for the current input
	idx_t size;
	//! Whether the input was NULL
	bool null;
};

template <class TYPE>
struct SettingContainer {
public:
	SettingContainer(TYPE default_value, TYPE null_value)
	    : initialized(false), default_value(std::move(default_value)), null_value(std::move(null_value)) {
	}
	UnifiedVectorFormat format;
	bool initialized = false;
	TYPE default_value;
	TYPE null_value;

public:
	void Set(idx_t count, Vector &vec) {
		vec.ToUnifiedFormat(count, format);
		initialized = true;
	}
	TYPE Get(idx_t idx, bool &null) {
		if (initialized) {
			idx = format.sel->get_index(idx);
			if (!format.validity.RowIsValid(idx)) {
				null = true;
				return null_value;
			}
			return ((TYPE *)format.data)[idx];
		}
		return default_value;
	}
};

class RangeExecutor {
protected:
	RangeExecutor(ClientContext &context, vector<unique_ptr<Expression>> args_list_p)
	    : new_row(false), input_idx(0), range_idx(0), expr_executor(context), args_list(std::move(args_list_p)) {

		// we add each expression in the args_list to the expression executor
		vector<LogicalType> args_data_types;
		for (auto &exp : args_list) {
			D_ASSERT(exp->type == ExpressionType::BOUND_REF);
			args_data_types.push_back(exp->return_type);
			expr_executor.AddExpression(*exp);
		}

		auto &allocator = Allocator::Get(context);
		args_data.Initialize(allocator, args_data_types);
	}
	~RangeExecutor() {
	}

protected:
	bool new_row;
	idx_t input_idx;
	idx_t range_idx;

	//! The executor used to create the args_data
	ExpressionExecutor expr_executor;
	//! The arguments extracted from the input chunk
	DataChunk args_data;
	//! BoundReferenceExpressions referencing the input chunk
	vector<unique_ptr<Expression>> args_list;
};

} // namespace range

} // namespace duckdb
