#pragma once

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {
namespace cp {

struct Expression {
	friend Expression field_ref(std::string field_name);
	friend Expression literal(int value);
	friend Expression call(std::string name, std::vector<Expression> inputs);

public:
	Expression() {
	}
	Expression(const Expression &other) {
		expression = other.expression->Copy();
	}
	Expression &operator=(const Expression &other) {
		expression = other.expression->Copy();
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
	static Expression Constant(int value) {
		auto expr = Expression();
		expr.expression = make_uniq<ConstantExpression>(value);
		return expr;
	}

private:
	unique_ptr<ParsedExpression> expression;
};

} // namespace cp
} // namespace duckdb
