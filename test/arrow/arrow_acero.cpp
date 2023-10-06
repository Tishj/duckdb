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
#include "duckdb/main/acero/compute/expression.hpp"
#include "duckdb/main/acero/compute/compute.hpp"
#include "duckdb/main/acero/source_node_options.hpp"

using namespace duckdb;
using namespace std;

void ExecutePlanAndCollectAsTable(ac::Declaration plan) {
	// collect sink_reader into a Table
	auto result = ac::DeclarationToTable(std::move(plan));
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
	con->Query("CREATE TABLE my_table (left INT, right INT)");
	string insert_query = R"EOF(
		INSERT INTO my_table (left, right) VALUES
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
	const idx_t multiplicity = 5;
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
