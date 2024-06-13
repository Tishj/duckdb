#include "duckdb/common/arrow/arrow_query_result.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"

namespace duckdb {

ArrowQueryResult::ArrowQueryResult(StatementType statement_type, StatementProperties properties, vector<string> names_p,
                                   vector<LogicalType> types_p, ClientProperties client_properties, idx_t row_count,
                                   idx_t batch_size)
    : QueryResult(QueryResultType::ARROW_RESULT, statement_type, std::move(properties), std::move(types_p),
                  std::move(names_p), std::move(client_properties)),
      row_count(row_count), batch_size(batch_size) {
}

ArrowQueryResult::ArrowQueryResult(ErrorData error) : QueryResult(QueryResultType::ARROW_RESULT, std::move(error)) {
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

vector<unique_ptr<ArrowArrayWrapper>> ArrowQueryResult::ConsumeArrays() {
	if (HasError()) {
		throw InvalidInputException("Attempting to fetch ArrowArrays from an unsuccessful query result\n: Error %s",
		                            GetError());
	}
	it = arrays.end();
	return std::move(arrays);
}

vector<unique_ptr<ArrowArrayWrapper>> &ArrowQueryResult::Arrays() {
	if (HasError()) {
		throw InvalidInputException("Attempting to fetch ArrowArrays from an unsuccessful query result\n: Error %s",
		                            GetError());
	}
	return arrays;
}

void ArrowQueryResult::SetArrowData(vector<unique_ptr<ArrowArrayWrapper>> arrays) {
	D_ASSERT(this->arrays.empty());
	this->arrays = std::move(arrays);
	this->it = this->arrays.begin();
}

unique_ptr<ArrowArrayWrapper> ArrowQueryResult::FetchArray() {
	if (it == arrays.end()) {
		return nullptr;
	}
	auto current_array = std::move(*it);
	it++;
	return current_array;
}

idx_t ArrowQueryResult::BatchSize() const {
	return batch_size;
}

} // namespace duckdb