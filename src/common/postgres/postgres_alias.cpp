#include "duckdb/common/postgres/postgres_alias.hpp"
#include "duckdb/common/exception.hpp"

#include "duckdb/parser/expression/list.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

string PostgresMode::GetAlias(BaseExpression &expr) {
	if (!expr.alias.empty()) {
		return expr.alias;
	}

	switch (expr.GetExpressionType()) {
	case ExpressionType::BOUND_AGGREGATE: {
		auto &function = expr.Cast<BoundAggregateExpression>();
		return function.function.name;
	}
	case ExpressionType::BOUND_FUNCTION: {
		auto &function = expr.Cast<BoundFunctionExpression>();
		if (function.is_operator) {
			return "?column?";
		}
		return function.function.name;
	}
	case ExpressionType::FUNCTION: {
		auto &function = expr.Cast<FunctionExpression>();
		if (function.is_operator) {
			return "?column?";
		}
		return function.function_name;
	}
	case ExpressionType::VALUE_CONSTANT:
	case ExpressionType::COMPARE_BETWEEN: {
		return "?column?";
	}
	case ExpressionType::CASE_EXPR: {
		return "case";
	}
	case ExpressionType::OPERATOR_CAST: {
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_CAST) {
			auto &cast = expr.Cast<BoundCastExpression>();
			return GetAlias(*cast.child);
		}
		auto &cast = expr.Cast<CastExpression>();
		return GetAlias(*cast.child);
	}
	case ExpressionType::COLLATE: {
		auto &collate = expr.Cast<CollateExpression>();
		return GetAlias(*collate.child);
	}
	case ExpressionType::COLUMN_REF: {
		auto &column = expr.Cast<ColumnRefExpression>();
		return column.GetColumnName();
	}
	case ExpressionType::POSITIONAL_REFERENCE:
	case ExpressionType::LAMBDA_REF: {
		// This doesn't exist in Postgres
		return expr.GetName();
	}
	default: {
		switch (expr.GetExpressionClass()) {
		case ExpressionClass::OPERATOR:
		case ExpressionClass::CONJUNCTION:
		case ExpressionClass::WINDOW:
		case ExpressionClass::COMPARISON: {
			return "?column?";
		}
		case ExpressionClass::SUBQUERY:
		case ExpressionClass::BOUND_SUBQUERY: {
			// FIXME: Postgres actually resolves this to the name of the referenced column
			// see: `select (select col from (values ('test', 'hello')) t(col, bol));`
			return "?column?";
		}
		default:
			break;
		}
	}
	}
	throw NotImplementedException("Could not get the Postgres alias for expression %s of class %s and type %s",
	                              expr.ToString(), EnumUtil::ToString(expr.GetExpressionClass()),
	                              EnumUtil::ToString(expr.GetExpressionType()));
}

} // namespace duckdb
