#include "duckdb/core_functions/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

static void AliasFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();

	auto name = state.expr.alias.empty() ? func_expr.children[0]->GetName() : state.expr.alias;
	auto &executor = *state.root.executor;
	if (executor.HasContext()) {
		auto &dbconfig = DBConfig::GetConfig(executor.GetContext());
		if (dbconfig.options.postgres_mode && state.expr.alias.empty()) {
			name = "?column?";
		}
	}

	Value v(name);
	result.Reference(v);
}

ScalarFunction AliasFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::VARCHAR, AliasFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb
