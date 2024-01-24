#pragma once

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/acero/dataset/date32_scalar.hpp"

namespace duckdb {
namespace cp {

struct Expression {
	friend Expression field_ref(std::string field_name);
	friend Expression greater_equal(Expression lhs, Expression rhs);
	friend Expression less(Expression lhs, Expression rhs);
	friend Expression less_equal(Expression lhs, Expression rhs);
	friend Expression literal(Value value);
	friend Expression call(std::string name, std::vector<Expression> inputs);

public:
	Expression() {
	}
	Expression(const Expression &other) {
		*this = other;
	}
	unique_ptr<ParsedExpression> InternalExpression() const {
		return expression->Copy();
	}

	Expression &operator=(const Expression &other) {
		if (other.expression) {
			expression = other.expression->Copy();
		} else {
			expression = nullptr;
		}
		return *this;
	}

private:
	static Expression Function(const string &name, vector<Expression> inputs) {
		vector<unique_ptr<ParsedExpression>> children;
		for (auto &input : inputs) {
			children.push_back(std::move(input.expression));
		}
		auto expr = Expression();
		expr.expression = make_uniq<FunctionExpression>(name, std::move(children));
		return expr;
	}
	static Expression ColumnRef(const string &name) {
		auto expr = Expression();
		expr.expression = make_uniq<ColumnRefExpression>(name);
		return expr;
	}
	static Expression Constant(Value value) {
		auto expr = Expression();
		expr.expression = make_uniq<ConstantExpression>(value);
		return expr;
	}

	static Expression Comparison(ExpressionType type, Expression lhs, Expression rhs) {
		auto expr = Expression();
		expr.expression = make_uniq<ComparisonExpression>(type, std::move(lhs.expression), std::move(rhs.expression));
		return expr;
	}

private:
	unique_ptr<ParsedExpression> expression;
};

Expression project(std::vector<bool> unused_one, std::vector<bool> unused_two) {
	return Expression();
}

Expression call(std::string function_name, std::vector<Expression> inputs) {
	return Expression::Function(function_name, std::move(inputs));
}

Expression field_ref(std::string field_name) {
	return Expression::ColumnRef(field_name);
}

Expression greater_equal(Expression lhs, Expression rhs) {
	return Expression::Comparison(ExpressionType::COMPARE_GREATERTHANOREQUALTO, std::move(lhs), std::move(rhs));
}

Expression less(Expression lhs, Expression rhs) {
	return Expression::Comparison(ExpressionType::COMPARE_LESSTHAN, std::move(lhs), std::move(rhs));
}

Expression less_equal(Expression lhs, Expression rhs) {
	return Expression::Comparison(ExpressionType::COMPARE_LESSTHANOREQUALTO, std::move(lhs), std::move(rhs));
}

Expression literal(Value value) {
	return Expression::Constant(value);
}

Expression literal(arrow::Date32Scalar scalar) {
	return literal(scalar.GetDuckValue());
}

template <typename Arg>
Expression literal(Arg &&arg) {
	return literal(Value(std::forward<Arg>(arg)));
}

} // namespace cp
} // namespace duckdb
