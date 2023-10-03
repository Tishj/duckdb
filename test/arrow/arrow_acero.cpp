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
	con->Query("CREATE TABLE my_table (i32 INT, str VARCHAR)");
	auto insert_query = R"EOF(
		INSERT INTO my_table (i32, str) VALUES
			(12, 'alpha'),
			(7, 'beta'),
			(3, 'alpha'),
			(-2, 'alpha'),
			(-1, 'gamma'),
			(3, 'alpha'),
			(5, 'gamma'),
			(3, 'beta'),
			(-8, 'alpha')
	)EOF";

	for (int repeat = 0; repeat < multiplicity; ++repeat) {
		con->Query(insert_query);
	}

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
	auto input = MakeGroupableBatches(1);

	ac::Declaration left {"source", ac::SourceNodeOptions {input->schema, input}};
	ac::Declaration right {"source", ac::SourceNodeOptions {input->schema, input}};

	ac::HashJoinNodeOptions join_opts {ac::JoinType::INNER,
	                                   /*left_keys=*/ {"str"},
	                                   /*right_keys=*/ {"str"}, cp::literal(true), "l_", "r_"};

	ac::Declaration hashjoin {"hashjoin", {std::move(left), std::move(right)}, std::move(join_opts)};

	return ExecutePlanAndCollectAsTable(std::move(hashjoin));
}
