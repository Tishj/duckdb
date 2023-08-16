#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/tableref/bound_subqueryref.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/function/table/read_csv.hpp"

namespace duckdb {

static bool IsTableInTableOutFunction(const TableFunctionCatalogEntry &table_function) {
	// If any of the functions is a table in-out function
	for (auto &fun : table_function.functions.functions) {
		if (fun.in_out_function) {
			return true;
		}
	}
	return false;
}

string Binder::BindTableFunctionExpressions(vector<unique_ptr<ParsedExpression>> &expressions,
                                            vector<Value> &parameters, named_parameter_map_t &named_parameters) {
	for (idx_t param_idx = 0; param_idx < expressions.size(); param_idx++) {
		auto &child = expressions[param_idx];
		string parameter_name;

		// hack to make named parameters work
		if (child->type == ExpressionType::COMPARE_EQUAL) {
			// comparison, check if the LHS is a columnref
			auto &comp = child->Cast<ComparisonExpression>();
			if (comp.left->type == ExpressionType::COLUMN_REF) {
				auto &colref = comp.left->Cast<ColumnRefExpression>();
				if (!colref.IsQualified()) {
					parameter_name = colref.GetColumnName();
					child = std::move(comp.right);
				}
			}
		}
		if (child->type == ExpressionType::SUBQUERY) {
			return "Only table-in-out functions can have subquery parameters";
		}

		TableFunctionBinder binder(*this, context);
		LogicalType sql_type;
		auto copied_expr = child->Copy();
		auto expr = binder.Bind(copied_expr, &sql_type);
		if (expr->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!expr->IsScalar()) {
			// should have been eliminated before
			throw InternalException("Table function requires a constant parameter");
		}
		auto constant = ExpressionExecutor::EvaluateScalar(context, *expr, true);
		if (parameter_name.empty()) {
			// unnamed parameter
			if (!named_parameters.empty()) {
				return "Unnamed parameters cannot come after named parameters";
			}
			parameters.emplace_back(std::move(constant));
		} else {
			named_parameters[parameter_name] = std::move(constant);
		}
	}
	return string();
}

static bool ParameterIsInternal(ParsedExpression &expr) {
	if (expr.expression_class != ExpressionClass::CONSTANT) {
		return false;
	}
	auto &value_expr = expr.Cast<ConstantExpression>();
	if (value_expr.value.type() != LogicalType::POINTER) {
		return false;
	}
	return true;
}

static vector<unique_ptr<ParsedExpression>>
ExtractInternalParameters(vector<unique_ptr<ParsedExpression>> &expressions) {
	vector<unique_ptr<ParsedExpression>> internal_parameters;
	vector<unique_ptr<ParsedExpression>> remaining_parameters;
	for (auto &expr : expressions) {
		if (ParameterIsInternal(*expr)) {
			internal_parameters.push_back(std::move(expr));
		} else {
			remaining_parameters.push_back(std::move(expr));
		}
	}
	expressions = std::move(remaining_parameters);
	return internal_parameters;
}

bool Binder::BindTableInTableOutFunctionParameters(const TableFunctionCatalogEntry &table_function,
                                                   vector<unique_ptr<ParsedExpression>> &expressions,
                                                   vector<LogicalType> &arguments,
                                                   unique_ptr<BoundSubqueryRef> &subquery, string &error) {
	D_ASSERT(IsTableInTableOutFunction(table_function));

	auto binder = Binder::CreateBinder(this->context, this, true);
	unique_ptr<QueryNode> subquery_node;
	if (expressions.size() == 1 && expressions[0]->type == ExpressionType::SUBQUERY) {
		// general case: argument is a subquery, bind it as part of the node

		auto original_subquery = std::move(expressions[0]);
		auto &subquery_expression = original_subquery->Cast<SubqueryExpression>();

		// NOTE: 'child' of SubqueryExpression gets lost here
		// Wrap the existing subquery in a new subquery: (select * from <existing_subquery>)
		auto subquery_ref =
		    make_uniq<SubqueryRef>(std::move(subquery_expression.subquery), "__unnamed_subquery_bind__");

		auto select_node = make_uniq<SelectNode>();
		select_node->select_list.push_back(make_uniq_base<ParsedExpression, StarExpression>());
		select_node->from_table = std::move(subquery_ref);

		subquery_node = std::move(select_node);
	} else {
		// special case: non-subquery parameter to table-in table-out function
		// generate a subquery and bind that (i.e. UNNEST([1,2,3]) becomes UNNEST((SELECT [1,2,3]))
		auto select_node = make_uniq<SelectNode>();
		select_node->select_list = std::move(expressions);
		select_node->from_table = make_uniq<EmptyTableRef>();
		subquery_node = std::move(select_node);
	}
	auto node = binder->BindNode(*subquery_node);
	subquery = make_uniq<BoundSubqueryRef>(std::move(binder), std::move(node));
	MoveCorrelatedExpressions(*subquery->binder);
	arguments.clear();
	for (auto &type : subquery->subquery->types) {
		arguments.push_back(type);
	}
	return true;
}

bool Binder::BindTableFunctionParameters(const TableFunctionCatalogEntry &table_function,
                                         vector<unique_ptr<ParsedExpression>> &expressions,
                                         vector<LogicalType> &arguments, vector<Value> &parameters,
                                         named_parameter_map_t &named_parameters, string &error) {
	D_ASSERT(!IsTableInTableOutFunction(table_function));
	error = BindTableFunctionExpressions(expressions, parameters, named_parameters);
	if (!error.empty()) {
		return false;
	}
	for (auto &param : parameters) {
		arguments.push_back(param.type());
	}
	return true;
}

unique_ptr<LogicalOperator>
Binder::BindTableFunctionInternal(TableFunction &table_function, const string &function_name, vector<Value> parameters,
                                  named_parameter_map_t named_parameters, vector<LogicalType> input_table_types,
                                  vector<string> input_table_names, const vector<string> &column_name_alias,
                                  unique_ptr<ExternalDependency> external_dependency) {
	auto bind_index = GenerateTableIndex();
	// perform the binding
	unique_ptr<FunctionData> bind_data;
	vector<LogicalType> return_types;
	vector<string> return_names;
	if (table_function.bind || table_function.bind_replace) {
		TableFunctionBindInput bind_input(parameters, named_parameters, input_table_types, input_table_names,
		                                  table_function.function_info.get());
		if (table_function.bind_replace) {
			auto new_plan = table_function.bind_replace(context, bind_input);
			if (new_plan != nullptr) {
				return CreatePlan(*Bind(*new_plan));
			} else if (!table_function.bind) {
				throw BinderException("Failed to bind \"%s\": nullptr returned from bind_replace without bind function",
				                      table_function.name);
			}
		}
		bind_data = table_function.bind(context, bind_input, return_types, return_names);
		if (table_function.name == "pandas_scan" || table_function.name == "arrow_scan") {
			auto &arrow_bind = bind_data->Cast<PyTableFunctionData>();
			arrow_bind.external_dependency = std::move(external_dependency);
		}
		if (table_function.name == "read_csv" || table_function.name == "read_csv_auto") {
			auto &csv_bind = bind_data->Cast<ReadCSVData>();
			if (csv_bind.single_threaded) {
				table_function.extra_info = "(Single-Threaded)";
			} else {
				table_function.extra_info = "(Multi-Threaded)";
			}
		}
	} else {
		throw InvalidInputException("Cannot call function \"%s\" directly - it has no bind function",
		                            table_function.name);
	}
	if (return_types.size() != return_names.size()) {
		throw InternalException("Failed to bind \"%s\": return_types/names must have same size", table_function.name);
	}
	if (return_types.empty()) {
		throw InternalException("Failed to bind \"%s\": Table function must return at least one column",
		                        table_function.name);
	}
	// overwrite the names with any supplied aliases
	for (idx_t i = 0; i < column_name_alias.size() && i < return_names.size(); i++) {
		return_names[i] = column_name_alias[i];
	}
	for (idx_t i = 0; i < return_names.size(); i++) {
		if (return_names[i].empty()) {
			return_names[i] = "C" + to_string(i);
		}
	}

	auto get = make_uniq<LogicalGet>(bind_index, table_function, std::move(bind_data), return_types, return_names);
	get->parameters = parameters;
	get->named_parameters = named_parameters;
	get->input_table_types = input_table_types;
	get->input_table_names = input_table_names;
	if (table_function.in_out_function && !table_function.projection_pushdown) {
		get->column_ids.reserve(return_types.size());
		for (idx_t i = 0; i < return_types.size(); i++) {
			get->column_ids.push_back(i);
		}
	}
	// now add the table function to the bind context so its columns can be bound
	bind_context.AddTableFunction(bind_index, function_name, return_names, return_types, get->column_ids,
	                              get->GetTable().get());
	return std::move(get);
}

unique_ptr<LogicalOperator> Binder::BindTableFunction(TableFunction &function, vector<Value> parameters) {
	named_parameter_map_t named_parameters;
	vector<LogicalType> input_table_types;
	vector<string> input_table_names;
	vector<string> column_name_aliases;
	return BindTableFunctionInternal(function, function.name, std::move(parameters), std::move(named_parameters),
	                                 std::move(input_table_types), std::move(input_table_names), column_name_aliases,
	                                 nullptr);
}

static bool CanCastFromType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::ANY:
	case LogicalTypeId::TABLE:
	case LogicalTypeId::POINTER:
	case LogicalTypeId::LIST:
		return false;
	default:
		return true;
	}
}

bool SubqueryRequiresCast(const vector<LogicalType> &function_args, const vector<LogicalType> &subquery_types) {
	if (function_args.empty()) {
		// This function takes varargs, no need to cast anything
		return false;
	}
	D_ASSERT(function_args.size() == subquery_types.size());
	for (idx_t i = 0; i < function_args.size(); i++) {
		if (function_args[i] != subquery_types[i]) {
			return true;
		}
	}
	return false;
}

unique_ptr<BoundTableRef> Binder::Bind(TableFunctionRef &ref) {
	QueryErrorContext error_context(root_statement, ref.query_location);

	D_ASSERT(ref.function->type == ExpressionType::FUNCTION);
	auto &fexpr = ref.function->Cast<FunctionExpression>();

	// fetch the function from the catalog
	auto &func_catalog = Catalog::GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY, fexpr.catalog, fexpr.schema,
	                                       fexpr.function_name, error_context);

	if (func_catalog.type == CatalogType::TABLE_MACRO_ENTRY) {
		auto &macro_func = func_catalog.Cast<TableMacroCatalogEntry>();
		auto query_node = BindTableMacro(fexpr, macro_func, 0);
		D_ASSERT(query_node);

		auto binder = Binder::CreateBinder(context, this);
		binder->can_contain_nulls = true;

		binder->alias = ref.alias.empty() ? "unnamed_query" : ref.alias;
		auto query = binder->BindNode(*query_node);

		idx_t bind_index = query->GetRootIndex();
		// string alias;
		string alias = (ref.alias.empty() ? "unnamed_query" + to_string(bind_index) : ref.alias);

		auto result = make_uniq<BoundSubqueryRef>(std::move(binder), std::move(query));
		// remember ref here is TableFunctionRef and NOT base class
		bind_context.AddSubquery(bind_index, alias, ref, *result->subquery);
		MoveCorrelatedExpressions(*result->binder);
		return std::move(result);
	}
	D_ASSERT(func_catalog.type == CatalogType::TABLE_FUNCTION_ENTRY);
	auto &function = func_catalog.Cast<TableFunctionCatalogEntry>();

	// evaluate the input parameters to the function
	vector<LogicalType> arguments;
	vector<Value> parameters;
	named_parameter_map_t named_parameters;
	unique_ptr<BoundSubqueryRef> subquery;
	string error;

	auto is_table_in_out = IsTableInTableOutFunction(function);
	if (is_table_in_out) {
		// We don't want to bundle these into the subquery because they are required at bind time
		auto internal_expressions = ExtractInternalParameters(fexpr.children);
		if (!BindTableInTableOutFunctionParameters(function, fexpr.children, arguments, subquery, error)) {
			throw BinderException(FormatError(ref, error));
		}
		if (!internal_expressions.empty()) {
			// We have parameters that we use internally to provide extra data to a function, such as POINTER
			error = BindTableFunctionExpressions(internal_expressions, parameters, named_parameters);
			if (!error.empty()) {
				throw BinderException(FormatError(ref, error));
			}
			for (auto &param : parameters) {
				arguments.push_back(param.type());
			}
		}
		D_ASSERT(subquery);
	} else {
		if (!BindTableFunctionParameters(function, fexpr.children, arguments, parameters, named_parameters, error)) {
			throw BinderException(FormatError(ref, error));
		}
	}

	// select the function based on the input parameters
	FunctionBinder function_binder(context);
	idx_t best_function_idx = function_binder.BindFunction(function.name, function.functions, arguments, error);
	if (best_function_idx == DConstants::INVALID_INDEX) {
		throw BinderException(FormatError(ref, error));
	}
	auto table_function = function.functions.GetFunctionByOffset(best_function_idx);

	// now check the named parameters
	BindNamedParameters(table_function.named_parameters, named_parameters, error_context, table_function.name);

	vector<LogicalType> input_table_types;
	vector<string> input_table_names;
	if (!is_table_in_out) {
		D_ASSERT(!subquery);
		// cast the parameters to the type of the function
		D_ASSERT(parameters.size() >= arguments.size());
		for (idx_t i = 0; i < arguments.size() && i < table_function.arguments.size(); i++) {
			auto &target_type =
			    i < table_function.arguments.size() ? table_function.arguments[i] : table_function.varargs;
			if (CanCastFromType(target_type)) {
				parameters[i] = parameters[i].CastAs(context, target_type);
			}
		}
	} else {
		D_ASSERT(subquery);
		auto subquery_requires_cast = SubqueryRequiresCast(table_function.arguments, subquery->subquery->types);
		input_table_types = subquery->subquery->types;
		input_table_names = subquery->subquery->names;
		if (subquery_requires_cast) {
			// We have ensured before that the subquery is a plain SELECT before
			D_ASSERT(subquery->subquery->type == QueryNodeType::SELECT_NODE);
			auto &select_node = (BoundSelectNode &)*subquery->subquery;
			auto &select_list = select_node.select_list;

			// Take the original expressions, apply the cast
			for (idx_t i = 0; i < select_list.size(); i++) {
				auto &source_expr = select_list[i];
				auto &target_type =
				    i < table_function.arguments.size() ? table_function.arguments[i] : table_function.varargs;
				source_expr = BoundCastExpression::AddCastToType(context, std::move(source_expr), target_type, false);

				// Update the expected types of the subquery
				input_table_types[i] = target_type;
			}
		}
	}

	auto get = BindTableFunctionInternal(table_function, ref.alias.empty() ? fexpr.function_name : ref.alias,
	                                     std::move(parameters), std::move(named_parameters),
	                                     std::move(input_table_types), std::move(input_table_names),
	                                     ref.column_name_alias, std::move(ref.external_dependency));
	if (subquery) {
		get->children.push_back(Binder::CreatePlan(*subquery));
	}

	return make_uniq_base<BoundTableRef, BoundTableFunction>(std::move(get));
}

} // namespace duckdb
