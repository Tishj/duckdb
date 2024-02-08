#include "capi_tester.hpp"
#include "duckdb.h"

using namespace duckdb;
using namespace std;

TEST_CASE("Test free of LIST types C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	REQUIRE(tester.OpenDatabase(nullptr));

	string query_1 = R"EOF(
		CREATE TABLE test(
			id BIGINT,
			one DECIMAL(18,3)[],
			two DECIMAL(18,3)[],
			three BIGINT,
			four DECIMAL(18,3)[]
		);
	)EOF";
	string query_2 = "INSERT INTO test VALUES (410, '[]', '[]',4,'[]');";
	string query_3 = "INSERT INTO test VALUES (412, '[]', '[]',4,'[]');";
	string query_4 = R"EOF(
		select
			id,
			one,
			two,
			three,
			four
		from test;
	)EOF";
	result = tester.Query(query_1);
	result = tester.Query(query_2);
	result = tester.Query(query_3);
	result = tester.Query(query_4);

	auto &result_c = result->InternalResult();
	int row_count = duckdb_row_count(&result_c);
	int column_count = duckdb_column_count(&result_c);
	for (idx_t row = 0; row < row_count; row++) {
		for (idx_t col = 0; col < column_count; col++) {
			duckdb_string str_val = duckdb_value_string(&result_c, col, row);
			printf("%s\n", str_val.data);
		}
	}
}
