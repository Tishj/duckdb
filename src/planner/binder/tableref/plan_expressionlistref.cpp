#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_expressionlistref.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundExpressionListRef &ref) {
	auto root = make_unique_base<LogicalOperator, LogicalDummyScan>(0);
	// values list, first plan any subqueries in the list
	for (auto &expr_list : ref.values) {
		for (auto &expr : expr_list) {
			PlanSubqueries(&expr, &root);
		}
	}
	// now create a LogicalExpressionGet from the set of expressions
	// fetch the types
	bool child_is_comparison_mark_join = false;
	if (root->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto& comparison_join = (LogicalJoin&)*root;
		//FIXME: probably also applies to other JoinOperators?
		if (comparison_join.join_type == JoinType::MARK) {
			child_is_comparison_mark_join = true;
		}
	}

	vector<LogicalType> types;
	for (auto &expr : ref.values[0]) {
		types.push_back(expr->return_type);
		if (!child_is_comparison_mark_join) {
			continue;
		}
		//! This needs to happen recursively for all children of expressions as well..
		//! The issue is that the bound reference needs to be changed to refer to the added BOOLEAN vector
		//! Which gets added when the comparison join is of type MARK
		if (expr->type == ExpressionType::BOUND_REF) {
			auto& bound_ref = (BoundReferenceExpression&)*expr;
			bound_ref.index++; //! Mark join adds an extra Vector
		}
	}
	auto expr_get = make_unique<LogicalExpressionGet>(ref.bind_index, types, move(ref.values));
	expr_get->AddChild(move(root));
	return move(expr_get);
}

} // namespace duckdb
