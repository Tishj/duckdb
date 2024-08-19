#include "duckdb/common/postgres/postgres_alias.hpp"
#include "duckdb/common/exception.hpp"

#include "duckdb/parser/expression/list.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

string PostgresMode::GetAlias(BaseExpression &expr) {
	D_ASSERT(expr.alias.empty());

	switch (expr.GetExpressionType()) {
	case ExpressionType::FUNCTION: {
		auto &function = expr.Cast<FunctionExpression>();
		return function.function_name;
	}
	case ExpressionType::VALUE_CONSTANT:
	case ExpressionType::SUBQUERY:
	case ExpressionType::COMPARE_BETWEEN: {
		return "?column?";
	}
	case ExpressionType::CASE_EXPR: {
		return "case";
	}
	case ExpressionType::OPERATOR_CAST: {
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
		default:
			break;
		}
		throw NotImplementedException("Could not get the Postgres alias for expression %s of class %s and type %s",
		                              expr.ToString(), EnumUtil::ToString(expr.GetExpressionClass()),
		                              EnumUtil::ToString(expr.GetExpressionType()));
	}
	}
}

} // namespace duckdb
