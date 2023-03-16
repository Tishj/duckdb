//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/table_function_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/planner/tableref/bound_subqueryref.hpp"

namespace duckdb {

enum class LazyEvaluatedArgumentType {
	SUBQUERY,
	REGULAR,
	CONSUMED,
	SPECIAL
};

// Because of an optional subquery, we cant populate our arguments vector instantly
struct LazyEvaluatedArgument {
	LazyEvaluatedArgument(LazyEvaluatedArgumentType argument_type, LogicalType type) : argument_type(argument_type), type(type) {}
	LazyEvaluatedArgumentType argument_type;
	LogicalType type;
};

//! The Table function binder can bind standard table function parameters (i.e. non-table-in-out functions)
class TableFunctionBinder : public ExpressionBinder {
public:
	TableFunctionBinder(Binder &binder, ClientContext &context);
	void BundleParametersIntoSubquery(QueryNode &initial_node);
	string BindArguments(Binder &binder, TableFunctionCatalogEntry &function, vector<unique_ptr<ParsedExpression>> expressions);
	vector<LogicalType> GetArguments();
	vector<Value> GetParameters();
	named_parameter_map_t GetNamedParameters();
	unique_ptr<BoundSubqueryRef> GetSubquery();

protected:
	BindResult BindColumnReference(ColumnRefExpression &expr, idx_t depth);
	BindResult BindExpression(unique_ptr<ParsedExpression> *expr, idx_t depth, bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;
private:
	bool ConvertExpressions(bool can_have_subquery, string &error, vector<unique_ptr<ParsedExpression>> &expressions);
private:
	vector<unique_ptr<ParsedExpression>> children;
	vector<LazyEvaluatedArgument> lazy_arguments;
	named_parameter_map_t named_parameters;
	vector<LogicalType> arguments;
	vector<Value> parameters;
	unique_ptr<BoundSubqueryRef> subquery;
};

} // namespace duckdb
