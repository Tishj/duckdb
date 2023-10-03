#include "duckdb/main/acero/declaration.hpp"
#include "duckdb/main/acero/dataset/table.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/relation.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"

#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/table_function_relation.hpp"
#include "duckdb/main/relation/join_relation.hpp"

#include "duckdb/main/acero/dataset/scan_node_options.hpp"
#include "duckdb/main/acero/dataset/scan_options.hpp"
#include "duckdb/main/acero/project_node_options.hpp"
#include "duckdb/main/acero/source_node_options.hpp"
#include "duckdb/main/acero/hash_join_node_options.hpp"

#include "duckdb/main/acero/util/arrow_stream_factory.hpp"
#include "duckdb/main/acero/util/arrow_test_factory.hpp"

namespace duckdb {
namespace ac {

static vector<unique_ptr<ParsedExpression>> ConvertExpressions(const vector<cp::Expression> &inputs) {
	vector<unique_ptr<ParsedExpression>> expressions;
	for (auto &input : inputs) {
		expressions.push_back(input.InternalExpression());
	}
	return expressions;
}

static vector<Value> ArrowScanInput(const shared_ptr<arrow::dataset::Dataset> &dataset) {
	vector<Value> params;
	auto arrow_object = dataset->ArrowObject();
	auto from_duckdb_result = true;

	params.push_back(Value::POINTER(arrow_object));
	if (from_duckdb_result) {
		params.push_back(Value::POINTER((uintptr_t)&ArrowTestFactory::CreateStream));
		params.push_back(Value::POINTER((uintptr_t)&ArrowTestFactory::GetSchema));
	} else {
		params.push_back(Value::POINTER((uintptr_t)&ArrowStreamTestFactory::CreateStream));
		params.push_back(Value::POINTER((uintptr_t)&ArrowStreamTestFactory::GetSchema));
	}

	return params;
}

static duckdb::JoinType ConvertJoinType(ac::JoinType type) {
	switch (type) {
	case ac::JoinType::INNER:
		return duckdb::JoinType::INNER;
	default: {
		throw NotImplementedException("JoinType not implemented");
	}
	}
}

static unique_ptr<ParsedExpression> ConvertCondition(const vector<string> &left_cols, const vector<string> &right_cols,
                                                     Relation &left, Relation &right) {
	auto result = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	D_ASSERT(left_cols.size() == right_cols.size());
	for (idx_t i = 0; i < left_cols.size(); i++) {
		auto &left_expr = left_cols[i];
		auto &right_expr = right_cols[i];

		// FIXME: this is horrible
		auto l = make_uniq<ColumnRefExpression>(left_expr, left.GetAlias());
		auto r = make_uniq<ColumnRefExpression>(right_expr, right.GetAlias());
		auto expr = make_uniq_base<ParsedExpression, ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(l),
		                                                                   std::move(r));
		result->AddExpression(std::move(expr));
	}
	return result;
}

static shared_ptr<Relation> ConvertDeclaration(const std::shared_ptr<ClientContext> &context, const Declaration &plan) {
	using ScanNodeOptions = arrow::dataset::ScanNodeOptions;
	using OptionType = arrow::dataset::ExecNodeOptions::OptionType;
	auto type = plan.options->Type();
	switch (type) {
	case OptionType::SCAN_NODE: {
		auto &scan_node_options = plan.options->Cast<ScanNodeOptions>();
		auto &dataset = scan_node_options.dataset;
		auto params = ArrowScanInput(dataset);
		return make_shared<TableFunctionRelation>(context, "arrow_scan", std::move(params));
	}
	case OptionType::PROJECT_NODE: {
		auto &project_node_options = plan.options->Cast<ProjectNodeOptions>();
		auto children = ConvertExpressions(project_node_options.expressions);
		auto &names = project_node_options.names;

		auto &inputs = plan.inputs;
		D_ASSERT(inputs.size() == 1);
		auto &input = inputs[0];
		auto child_relation = ConvertDeclaration(context, input);
		auto project_relation = make_shared<ProjectionRelation>(std::move(child_relation), std::move(children), names);
		return project_relation;
	}
	case OptionType::SOURCE_NODE: {
		auto &source_options = plan.options->Cast<SourceNodeOptions>();
		auto &dataset = source_options.generator;
		// TODO: Construct a TableFunctionRelation over the arrow dataset
		auto params = ArrowScanInput(dataset);
		return make_shared<TableFunctionRelation>(context, "arrow_scan", std::move(params));
	}
	case OptionType::HASH_JOIN_NODE: {
		auto &hash_join_options = plan.options->Cast<HashJoinNodeOptions>();
		auto join_type = ConvertJoinType(hash_join_options.join_type);

		auto &inputs = plan.inputs;
		D_ASSERT(inputs.size() == 2);
		auto left = ConvertDeclaration(context, inputs[0]);
		auto right = ConvertDeclaration(context, inputs[1]);
		left = left->Alias("left");
		right = right->Alias("right");
		auto condition = ConvertCondition(hash_join_options.left_keys, hash_join_options.right_keys, *left, *right);
		return make_shared<JoinRelation>(std::move(left), std::move(right), std::move(condition), join_type);
	}
	default: {
		throw NotImplementedException("Type not implemented for ExecNodeOptions::OptionType");
	}
	}
}

static unique_ptr<QueryResult> CompletePendingQuery(PendingQueryResult &pending_query) {
	PendingExecutionResult execution_result;
	do {
		execution_result = pending_query.ExecuteTask();
	} while (!PendingQueryResult::IsFinished(execution_result));
	if (execution_result == PendingExecutionResult::EXECUTION_ERROR) {
		pending_query.ThrowError();
	}
	return pending_query.Execute();
}

shared_ptr<arrow::Table> DeclarationToTable(Declaration plan) {
	DuckDB db(nullptr);
	Connection con(db);

	auto rel = ConvertDeclaration(con.context, std::move(plan));

	auto &context = con.context;
	auto pending_query = context->PendingQuery(rel, false);
	auto result = CompletePendingQuery(*pending_query);
	result->Print();

	// create an arrow table from the arrow query result
	auto table = make_shared<arrow::Table>();
	return table;
}

} // namespace ac
} // namespace duckdb
