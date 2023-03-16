#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/table_binding.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

TableFunctionBinder::TableFunctionBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult TableFunctionBinder::BindColumnReference(ColumnRefExpression &expr, idx_t depth) {

	// if this is a lambda parameters, then we temporarily add a BoundLambdaRef,
	// which we capture and remove later
	if (lambda_bindings) {
		auto &colref = (ColumnRefExpression &)expr;
		for (idx_t i = 0; i < lambda_bindings->size(); i++) {
			if (colref.GetColumnName() == (*lambda_bindings)[i].dummy_name) {
				return (*lambda_bindings)[i].Bind(colref, i, depth);
			}
		}
	}

	auto result_name = StringUtil::Join(expr.column_names, ".");
	return BindResult(make_unique<BoundConstantExpression>(Value(result_name)));
}

BindResult TableFunctionBinder::BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth,
                                               bool root_expression) {
	auto &expr = **expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF:
		return BindColumnReference((ColumnRefExpression &)expr, depth);
	case ExpressionClass::SUBQUERY:
		throw BinderException("Table function cannot contain subqueries");
	case ExpressionClass::DEFAULT:
		return BindResult("Table function cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult("Table function cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string TableFunctionBinder::UnsupportedAggregateMessage() {
	return "Table function cannot contain aggregates!";
}

static bool IsSpecialType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::ANY:
	case LogicalTypeId::TABLE:
	case LogicalTypeId::POINTER:
		return true;
	default:
		return false;
	}
}

static bool IsTableInTableOutFunction(TableFunctionCatalogEntry &table_function) {
	// If any of the functions is a table in-out function
	for (auto &fun : table_function.functions.functions) {
		if (fun.in_out_function) {
			return true;
		}
	}
	return false;
}

vector<LogicalType> TableFunctionBinder::GetArguments() {
	if (arguments.empty()) {
		// Need to first compute the arguments
		for (auto& arg : lazy_arguments) {
			switch (arg.argument_type) {
			case LazyEvaluatedArgumentType::SUBQUERY: {
				D_ASSERT(subquery);
				for (auto& type : subquery->subquery->types) {
					arguments.push_back(type);
				}
				break;
			}
			case LazyEvaluatedArgumentType::CONSUMED: {
				// This has been bundled together with the subquery
				continue;
			}
			case LazyEvaluatedArgumentType::SPECIAL:
			case LazyEvaluatedArgumentType::REGULAR:
				arguments.push_back(arg.type);
				break;
			default:
				throw InternalException("LazyEvaluatedArgumentType not implemented");
			}
		}
	}
	return std::move(arguments);
}

vector<Value> TableFunctionBinder::GetParameters() {
	return std::move(parameters);
}

named_parameter_map_t TableFunctionBinder::GetNamedParameters() {
	return std::move(named_parameters);
}

unique_ptr<BoundSubqueryRef> TableFunctionBinder::GetSubquery() {
	return std::move(subquery);
}

bool TableFunctionBinder::ConvertExpressions(bool can_have_subquery, string &error, vector<unique_ptr<ParsedExpression>> &expressions) {
	bool has_subquery = false;
	for (idx_t param_idx = 0; param_idx < expressions.size(); param_idx++) {
		auto &child = expressions[param_idx];
		string parameter_name;

		// hack to make named parameters work
		if (child->type == ExpressionType::COMPARE_EQUAL) {
			// comparison, check if the LHS is a columnref
			auto &comp = (ComparisonExpression &)*child;
			if (comp.left->type == ExpressionType::COLUMN_REF) {
				auto &colref = (ColumnRefExpression &)*comp.left;
				if (!colref.IsQualified()) {
					parameter_name = colref.GetColumnName();
					child = std::move(comp.right);
				}
			}
		}
		if (child->type == ExpressionType::SUBQUERY) {
			if (!can_have_subquery) {
				return "This table function is not a table in-out function, it can't take a subquery";
			}
			if (has_subquery) {
				return "Table function can have at most one subquery parameter";
			}
			has_subquery = true;
			lazy_arguments.emplace_back(LazyEvaluatedArgumentType::SUBQUERY, LogicalType::ANY);
			children.emplace_back(std::move(child));
			continue;
		}

		LogicalType sql_type;
		auto copied_expr = child->Copy();
		auto expr = Bind(copied_expr, &sql_type);
		if (expr->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!expr->IsScalar()) {
			return "Table function requires a constant parameter";
		}
		bool special_type = IsSpecialType(sql_type);
		auto constant = ExpressionExecutor::EvaluateScalar(context, *expr, true);
		if (!parameter_name.empty()) {
			named_parameters[parameter_name] = std::move(constant);
			continue;
		}
		// unnamed parameter
		if (!named_parameters.empty()) {
			return "Unnamed parameters cannot come after named parameters";
		}
		if (can_have_subquery) {
			lazy_arguments.emplace_back(special_type ? LazyEvaluatedArgumentType::SPECIAL : LazyEvaluatedArgumentType::REGULAR, sql_type);
		} else {
			arguments.emplace_back(sql_type);
		}
		parameters.emplace_back(std::move(constant));
		children.emplace_back(std::move(child));
	}
	return has_subquery;
}

static SubqueryExpression &GetSubqueryExpression(vector<unique_ptr<ParsedExpression>> &children) {
	if (children.size() > 1) {
		for (auto& child : children) {
			if (child->type == ExpressionType::SUBQUERY) {
				return (SubqueryExpression&)*child;
			}
		}
		throw InternalException("No subquery was found, but it was expected");
	} else {
		D_ASSERT(children[0]->type == ExpressionType::SUBQUERY);
		return (SubqueryExpression&)*children[0];
	}
}

void TableFunctionBinder::BundleParametersIntoSubquery(QueryNode& node) {
	// This should only be called after BindArguments was used to bind arguments for a (potential) table-in-out function
	D_ASSERT(lazy_arguments.size());
	D_ASSERT(children.size());

	vector<unique_ptr<ParsedExpression>> child_expressions;
	for (idx_t i = 0; i < children.size(); i++) {
		auto &child = children[i];
		auto& type = lazy_arguments[i].argument_type;
		if (child->type == ExpressionType::SUBQUERY) {
			for (auto &expr : node.GetSelectList()) {
				child_expressions.push_back(expr->Copy());
			}
		} else if (type != LazyEvaluatedArgumentType::SPECIAL) {
			D_ASSERT(type == LazyEvaluatedArgumentType::REGULAR);
			type = LazyEvaluatedArgumentType::CONSUMED;
			child_expressions.push_back(std::move(child));
		}
	}
	// FIXME: the original subquery isn't guaranteed to be a SELECT statement
	D_ASSERT(node.type == QueryNodeType::SELECT_NODE);
	auto &existing_select_node = (SelectNode &)node;
	existing_select_node.select_list = std::move(child_expressions);
}

string TableFunctionBinder::BindArguments(Binder &binder, TableFunctionCatalogEntry &function, vector<unique_ptr<ParsedExpression>> expressions) {
	bool is_table_in_out = IsTableInTableOutFunction(function);

	string error;
	bool has_subquery = ConvertExpressions(is_table_in_out, error, expressions);
	if (!error.empty()) {
		return error;
	}

	if (!has_subquery) {
		return string();
	}

	auto subquery_binder = Binder::CreateBinder(binder.context, &binder, true);
	unique_ptr<BoundQueryNode> node;

	auto &se = GetSubqueryExpression(children);
	if (children.size() > 1) {
		// There are other expressions, bundle them together with the existing subquery
		vector<unique_ptr<ParsedExpression>> child_expressions;
		D_ASSERT(lazy_arguments.size() == children.size());
		BundleParametersIntoSubquery(*se.subquery->node);
	}
	// Bind the subquery so we can know the types
	node = subquery_binder->BindNode(*se.subquery->node);
	subquery = make_unique<BoundSubqueryRef>(std::move(subquery_binder), std::move(node));
	binder.MoveCorrelatedExpressions(*subquery->binder);
	parameters.clear();
	return string();
}

} // namespace duckdb
