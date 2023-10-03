#pragma once

#include "duckdb/main/acero/compute/expression.hpp"

namespace duckdb {
namespace cp {

static Expression project(std::vector<bool> unused_one, std::vector<bool> unused_two) {
	return Expression();
}

static Expression call(std::string function_name, std::vector<Expression> inputs) {
	return Expression::Function(function_name, std::move(inputs));
}

static Expression field_ref(std::string field_name) {
	return Expression::ColumnRef(field_name);
}

static Expression literal(int value) {
	return Expression::Constant(value);
}

} // namespace cp
} // namespace duckdb
