#include "catch.hpp"
#include "test_helpers.hpp"

#include "arrow/arrow_test_helper.hpp"

#include <iostream>
#include <map>
#include <set>
#include "duckdb/main/acero/dataset/scan_options.hpp"
#include "duckdb/main/acero/dataset/scan_node_options.hpp"
#include "duckdb/main/acero/hash_join_node_options.hpp"
#include "duckdb/main/acero/dataset/dataset.hpp"
#include "duckdb/main/acero/declaration.hpp"
#include "duckdb/main/acero/project_node_options.hpp"
#include "duckdb/main/acero/table_source_node_options.hpp"
#include "duckdb/main/acero/aggregate_node_options.hpp"
#include "duckdb/main/acero/filter_node_options.hpp"
#include "duckdb/main/acero/compute/expression.hpp"
#include "duckdb/main/acero/compute/compute.hpp"
#include "duckdb/main/acero/source_node_options.hpp"
#include "duckdb/main/acero/util/array_vector_stream.hpp"
#include "duckdb/main/acero/arrow_conversion.hpp"
#include "duckdb/main/acero/dataset/date32_scalar.hpp"
#include "duckdb/main/acero/compute/scalar_aggregate_options.hpp"
#include "duckdb/common/arrow/physical_arrow_collector.hpp"


using namespace duckdb;
using namespace std;

void VerifyResult(arrow::Table &table) {
	auto array_vector_stream = new arrow::ArrayVectorStream(std::move(table));
	auto &stream = array_vector_stream->stream;

	DuckDB db(nullptr);
	Connection conn(db);
	auto params = ArrowTestHelper::ConstructArrowScan(stream);
	auto result = ArrowTestHelper::ScanArrowObject(conn, params);
	// idx_t tuple_count = 0;
	// while (true) {
	//	auto chunk = result->Fetch();
	//	if (!chunk) {
	//		break;
	//	}
	//	tuple_count += chunk->size();
	//}
	// Printer::Print(StringUtil::Format("Produced tuples: %d\n", tuple_count));
	result->Print();
}

void ExecutePlanAndCollectAsTable(ac::Declaration plan) {
	// collect sink_reader into a Table
	auto result = ac::DeclarationToTable(std::move(plan));
	VerifyResult(*result);
}

std::shared_ptr<arrow::dataset::Dataset> GetDataset() {
	// Create a result and create an arrow dataset on top of the result
	auto db = make_uniq<DuckDB>(nullptr);
	auto con = make_uniq<Connection>(*db);
	con->Query("create table tbl (a bigint, b boolean)");
	con->Query(R"EOF(
		insert into tbl from (VALUES
			(1, NULL),
			(2, true),
			(NULL, true),
			(3, false),
			(NULL, true),
			(4, false),
			(5, NULL),
			(6, false),
			(7, false),
			(8, true),
		)
	)EOF");
	auto ds = make_shared<arrow::dataset::Dataset>(std::move(db), std::move(con), "select * from tbl");
	return ds;
}

std::shared_ptr<arrow::dataset::Dataset> MakeGroupableBatches(int multiplicity = 1) {
	// Create a result and create an arrow dataset on top of the result
	auto db = make_uniq<DuckDB>(nullptr);
	auto con = make_uniq<Connection>(*db);
	con->Query("CREATE TABLE my_table (\"left\" INT, \"right\" INT)");
	string insert_query = R"EOF(
		INSERT INTO my_table ("left", "right") VALUES
	)EOF";

	string data = R"EOF(
		(12, 1),
		(7, 2),
		(3, 1),
		(-2, 1),
		(-1, 3),
		(3, 1),
		(5, 3),
		(3, 2),
		(-8, 1),
	)EOF";

	for (int repeat = 0; repeat < multiplicity; ++repeat) {
		insert_query += data;
	}

	con->Query(insert_query);

	auto ds = make_shared<arrow::dataset::Dataset>(std::move(db), std::move(con), "select * from my_table");
	return ds;
}

static duckdb::unique_ptr<QueryResult> CompletePendingQuery(PendingQueryResult &pending_query) {
	PendingExecutionResult execution_result;
	do {
		execution_result = pending_query.ExecuteTask();
	} while (!PendingQueryResult::IsFinished(execution_result));
	if (execution_result == PendingExecutionResult::EXECUTION_ERROR) {
		pending_query.ThrowError();
	}
	return pending_query.Execute();
}

shared_ptr<arrow::Table> GetLineItem() {
	auto db = make_uniq<DuckDB>(nullptr);
	auto con = make_uniq<Connection>(*db);
	con->context->config.result_collector = [](ClientContext &context, PreparedStatementData &data) {
		return PhysicalArrowCollector::Create(context, data, 2048);
	};

	auto rel = con->RelationFromQuery("from '/Users/thijs/DuckDBLabs/arrow_acero/tmp/sf1/lineitem.parquet';");
	auto pending_query = con->context->PendingQuery(rel, false);
	auto res = CompletePendingQuery(*pending_query);
	return ac::ArrowConversion::ConvertToTable(std::move(res));
};

TEST_CASE("Test Acero Mock - TPCH Q06", "[api]") {
	auto table = GetLineItem();
	// Create a scan over the dataset
	// declare table source
	int max_batch_size = 1000;
	auto table_source_options = ac::TableSourceNodeOptions {table, max_batch_size};
	ac::Declaration source {"table_source", std::move(table_source_options)};

	// declare filter operation
	cp::Expression filter_expr = cp::call(
	    "and",
	    {cp::call(
	         "and",
	         {
	             cp::call("and",
	                      {
	                          cp::greater_equal(cp::field_ref("l_shipdate"), cp::literal(arrow::Date32Scalar(8766))),
	                          // January 1, 1994 is 8766 days after January 1, 1970
	                          cp::less(cp::field_ref("l_shipdate"), cp::literal(arrow::Date32Scalar(9131)))
	                          // January 1, 1995 is 9131 days after January 1, 1970
	                      }),
	             cp::call("and", {cp::greater_equal(cp::field_ref("l_discount"), cp::literal(0.05)),
	                              cp::less_equal(cp::field_ref("l_discount"), cp::literal(0.07))}),
	         }),
	     cp::less(cp::field_ref("l_quantity"), cp::literal(24.0))});
	auto filter_options = ac::FilterNodeOptions(filter_expr);
	ac::Declaration filter {"filter", {std::move(source)}, std::move(filter_options)};

	// declare project operation
	auto project_options = ac::ProjectNodeOptions {
	    {cp::call("multiply", {cp::field_ref("l_extendedprice"), cp::field_ref("l_discount")})}, {"product"}};
	ac::Declaration project {"project", {std::move(filter)}, std::move(project_options)};

	// declare aggregate operation
	auto options = std::make_shared<cp::ScalarAggregateOptions>(/*skip_nulls=*/true, /*min_count=*/1);
	auto aggregate_options =
	    ac::AggregateNodeOptions {/*aggregates=*/ {{"sum", options, "product", "revenue"}}, /*keys=*/ {}};
	ac::Declaration aggregate {"aggregate", {std::move(project)}, std::move(aggregate_options)};

	ExecutePlanAndCollectAsTable(std::move(aggregate));
}

TEST_CASE("Test Acero Mock - Projection", "[api]") {
	// Create a scan over the dataset
	auto options = std::make_shared<arrow::dataset::ScanOptions>();
	options->projection = cp::project({}, {});

	auto dataset = GetDataset();
	auto scan_node_options = arrow::dataset::ScanNodeOptions {dataset, options};
	ac::Declaration scan {"scan", std::move(scan_node_options)};

	// The expressions that form the projection
	cp::Expression a_times_2 = cp::call("multiply", {cp::field_ref("a"), cp::literal(2)});
	// Create a projection operator
	ac::Declaration project {"project", {std::move(scan)}, ac::ProjectNodeOptions({a_times_2})};

	ExecutePlanAndCollectAsTable(std::move(project));
}

TEST_CASE("Test Acero Mock - Hash Join", "[api]") {
	const idx_t multiplicity = 2000;
	// const idx_t multiplicity = 100;
	auto input_l = MakeGroupableBatches(multiplicity);
	auto input_r = MakeGroupableBatches(multiplicity);

	ac::Declaration left {"source", ac::SourceNodeOptions {input_l->schema, input_l}};
	ac::Declaration right {"source", ac::SourceNodeOptions {input_r->schema, input_r}};

	ac::HashJoinNodeOptions join_opts {ac::JoinType::INNER,
	                                   /*left_keys=*/ {"right"},
	                                   /*right_keys=*/ {"right"}, cp::literal(true), "l_", "r_"};

	ac::Declaration hashjoin {"hashjoin", {std::move(left), std::move(right)}, std::move(join_opts)};

	return ExecutePlanAndCollectAsTable(std::move(hashjoin));
}
