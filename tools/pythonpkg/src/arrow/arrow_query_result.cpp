#include "duckdb_python/arrow/arrow_query_result.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/box_renderer.hpp"

namespace duckdb {

ArrowQueryResult::ArrowQueryResult(StatementType statement_type, StatementProperties properties, vector<string> names_p,
                                   vector<LogicalType> types_p py::list record_batches,
                                   ClientProperties client_properties, idx_t row_count)
    : QueryResult(QueryResultType::ARROW_RESULT, statement_type, std::move(properties), std::move(types_p),
                  std::move(names_p), std::move(client_properties)),
      record_batches(std::move(record_batches)), row_count(row_count) {
}

ArrowQueryResult::ArrowQueryResult(PreservedError error)
    : QueryResult(QueryResultType::ARROW_RESULT, std::move(error)) {
}

unique_ptr<DataChunk> ArrowQueryResult::Fetch() {
	throw NotImplementedException("Can't 'Fetch' from ArrowQueryResult");
}
unique_ptr<DataChunk> ArrowQueryResult::FetchRaw() {
	throw NotImplementedException("Can't 'FetchRaw' from ArrowQueryResult");
}

string ArrowQueryResult::ToString() {
	// FIXME: can't throw an exception here as it's used for verification
	return "";
}

string ArrowQueryResult::ToBox(ClientContext &context, const BoxRendererConfig &config) {
	// FIXME: can't throw an exception here as it's used for verification
	return "";
}

idx_t ArrowQueryResult::RowCount() const {
	return row_count;
}

py::list &ArrowQueryResult::GetRecordBatches() {
	if (HasError()) {
		throw InvalidInputException("Attempting to get collection from an unsuccessful query result\n: Error %s",
		                            GetError());
	}
	return record_batches;
}

} // namespace duckdb
