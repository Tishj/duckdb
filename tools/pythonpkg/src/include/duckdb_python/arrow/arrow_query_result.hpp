//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/arrow/arrow_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb_python/arrow/array_wrapper.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/common/preserved_error.hpp"

namespace duckdb {

class ClientContext;

class ArrowQueryResult : public QueryResult {
public:
	static constexpr const QueryResultType TYPE = QueryResultType::ARROW_RESULT;

public:
	friend class ClientContext;
	//! Creates a successful query result with the specified names and types
	DUCKDB_API ArrowQueryResult(StatementType statement_type, StatementProperties properties, vector<string> names,
	                            vector<LogicalType> types, unique_ptr<ArrowResultConversion> collection,
	                            ClientProperties client_properties, idx_t row_count);
	//! Creates an unsuccessful query result with error condition
	DUCKDB_API explicit ArrowQueryResult(PreservedError error);

public:
	//! Fetches a DataChunk from the query result.
	//! This will consume the result (i.e. the result can only be scanned once with this function)
	DUCKDB_API unique_ptr<DataChunk> Fetch() override;
	DUCKDB_API unique_ptr<DataChunk> FetchRaw() override;
	//! Converts the QueryResult to a string
	DUCKDB_API string ToString() override;
	DUCKDB_API string ToBox(ClientContext &context, const BoxRendererConfig &config) override;

	DUCKDB_API idx_t RowCount() const;

public:
	py::list &GetRecordBatches();

private:
	py::list record_batches;
	idx_t row_count;
};

} // namespace duckdb
