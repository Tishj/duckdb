#include "duckdb/main/acero/declaration.hpp"
#include "duckdb/main/acero/dataset/table.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/relation.hpp"

#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/table_function_relation.hpp"

#include "duckdb/main/acero/dataset/scan_node_options.hpp"
#include "duckdb/main/acero/dataset/scan_options.hpp"
#include "duckdb/main/acero/project_node_options.hpp"

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

static shared_ptr<Relation> ConvertDeclaration(const std::shared_ptr<ClientContext> &context, const Declaration &plan) {
	using ScanNodeOptions = arrow::dataset::ScanNodeOptions;
	using OptionType = arrow::dataset::ExecNodeOptions::OptionType;
	auto type = plan.options->Type();
	switch (type) {
	case OptionType::SCAN_NODE: {
		auto &scan_node_options = plan.options->Cast<ScanNodeOptions>();
		auto &dataset = scan_node_options.dataset;
		// TODO: Construct a TableFunctionRelation over the arrow dataset
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

	// create an arrow table from the arrow query result
	auto table = make_shared<arrow::Table>();
	return table;
}

} // namespace ac
} // namespace duckdb
