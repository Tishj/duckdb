//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/list_aggregate_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

template <class FUNCTION_FUNCTOR, bool IS_AGGR = false>
void ListAggregatesFunctionStripped(Vector &lists, idx_t count, BoundAggregateExpression &aggr, Vector &result);
void ListUniqueFunction(DataChunk &args, ExpressionState &state, Vector &result);

struct UniqueFunctor {
	template <class OP, class T, class MAP_TYPE = unordered_map<T, idx_t>>
	static void ListExecuteFunction(Vector &result, Vector &state_vector, idx_t count) {

		VectorData sdata;
		state_vector.Orrify(count, sdata);
		auto states = (HistogramAggState<T, MAP_TYPE> **)sdata.data;

		auto result_data = FlatVector::GetData<uint64_t>(result);

		for (idx_t i = 0; i < count; i++) {

			auto state = states[sdata.sel->get_index(i)];

			if (!state->hist) {
				result_data[i] = 0;
				continue;
			}

			result_data[i] = state->hist->size();
		}
		result.Verify(count);
	}
};

struct ListAggregatesBindData : public FunctionData {
	ListAggregatesBindData(const LogicalType &stype_p, unique_ptr<Expression> aggr_expr_p);
	~ListAggregatesBindData() override;

	LogicalType stype;
	unique_ptr<Expression> aggr_expr;

	unique_ptr<FunctionData> Copy() const override {
		return make_unique<ListAggregatesBindData>(stype, aggr_expr->Copy());
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = (const ListAggregatesBindData &)other_p;
		return stype == other.stype && aggr_expr->Equals(other.aggr_expr.get());
	}
};

template <bool IS_AGGR = false>
unique_ptr<FunctionData> ListAggregatesBindFunction(ClientContext &context, ScalarFunction &bound_function,
                                                    const LogicalType &list_child_type,
                                                    AggregateFunction &aggr_function) {

	// create the child expression and its type
	vector<unique_ptr<Expression>> children;
	auto expr = make_unique<BoundConstantExpression>(Value(list_child_type));
	children.push_back(move(expr));

	auto bound_aggr_function = AggregateFunction::BindAggregateFunction(context, aggr_function, move(children));
	bound_function.arguments[0] = LogicalType::LIST(bound_aggr_function->function.arguments[0]);

	if (IS_AGGR) {
		bound_function.return_type = bound_aggr_function->function.return_type;
	}

	return make_unique<ListAggregatesBindData>(bound_function.return_type, move(bound_aggr_function));
}

} // namespace duckdb
