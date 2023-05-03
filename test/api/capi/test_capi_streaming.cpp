#include "capi_tester.hpp"
#include "duckdb.h"

using namespace duckdb;
using namespace std;

TEST_CASE("Test streaming results in C API", "[capi]") {
	CAPITester tester;
	CAPIPrepared prepared;
	CAPIPending pending;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(prepared.Prepare(tester, "SELECT * from read_parquet('/Users/thijs/DuckDBLabs/duckdb/tmp/HelloDuckDB/resources/types_nested.parquet')"));
	REQUIRE(pending.PendingStreaming(prepared));

	while (true) {
		auto state = pending.ExecuteTask();
		REQUIRE(state != DUCKDB_PENDING_ERROR);
		if (state == DUCKDB_PENDING_RESULT_READY) {
			break;
		}
	}

	for (idx_t i = 0; i < 10; i++) {
		auto result = pending.Execute();
		REQUIRE(result);
		REQUIRE(!result->HasError());

		idx_t chunk_count = 0;
		duckdb::vector<duckdb::unique_ptr<CAPIDataChunk>> chunks;
		while (true) {
			auto chunk = result->StreamChunk();
			if (chunk == nullptr) {
				break;
			}
			auto vector = chunk->GetVector(0);
			chunk_count++;
			if (chunk_count % 2 == 0) {
				continue;
			}
			chunks.emplace_back(std::move(chunk));
		}
		for (auto i = chunks.rbegin(); i != chunks.rend(); i++) {
			auto ptr = std::move(*i);
		}
	}
}

TEST_CASE("Test other methods on streaming results in C API", "[capi]") {
	CAPITester tester;
	CAPIPrepared prepared;
	CAPIPending pending;
	duckdb::unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(prepared.Prepare(tester, "SELECT i::UINT32 FROM range(1000000) tbl(i)"));
	REQUIRE(pending.PendingStreaming(prepared));

	while (true) {
		auto state = pending.ExecuteTask();
		REQUIRE(state != DUCKDB_PENDING_ERROR);
		if (state == DUCKDB_PENDING_RESULT_READY) {
			break;
		}
	}

	// Once we've done this, the StreamQueryResult is made
	result = pending.Execute();
	REQUIRE(result);
	REQUIRE(!result->HasError());
	REQUIRE(result->IsStreaming());

	// interrogate the result with various methods
	auto chunk_count = result->ChunkCount();
	REQUIRE(chunk_count == 0);
	auto column_count = result->ColumnCount();
	(void)column_count;
	auto column_name = result->ColumnName(0);
	(void)column_name;
	auto column_type = result->ColumnType(0);
	(void)column_type;
	auto error_message = result->ErrorMessage();
	REQUIRE(error_message == nullptr);
	auto fetched_chunk = result->FetchChunk(0);
	REQUIRE(fetched_chunk == nullptr);
	auto has_error = result->HasError();
	REQUIRE(has_error == false);
	auto row_count = result->row_count();
	REQUIRE(row_count == 0);
	auto rows_changed = result->rows_changed();
	REQUIRE(rows_changed == 0);

	// this succeeds because the result is materialized if a stream-result method hasn't being used yet
	auto column_data = result->ColumnData<uint32_t>(0);
	REQUIRE(column_data != nullptr);

	// this materializes the result
	auto is_null = result->IsNull(0, 0);
	REQUIRE(is_null == false);
}
