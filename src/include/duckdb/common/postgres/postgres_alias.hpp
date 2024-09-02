#pragma once

#include "duckdb/parser/base_expression.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class PostgresMode {
public:
	PostgresMode() = delete;

public:
	static string GetAlias(BaseExpression &expr);
};

} // namespace duckdb
