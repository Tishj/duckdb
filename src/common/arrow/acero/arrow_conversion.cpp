#include "duckdb/main/acero/arrow_conversion.hpp"
#include "duckdb/main/chunk_scan_state/query_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"

namespace duckdb {
namespace ac {

namespace {

struct ArrowConversionResult {
public:
	ArrowConversionResult(ClientProperties properties) : properties(properties) {
		schema = make_shared<ArrowSchemaWrapper>();
	}

public:
	void AddChunk(ArrowArray &&array) {
		if (arrays.empty()) {
			for (int64_t i = 0; i < array.n_children; i++) {
				arrays.push_back(make_shared<arrow::ChunkedArray>());
			}
		}
		D_ASSERT(!arrays.empty());
		for (int64_t i = 0; i < array.n_children; i++) {
			auto child = array.children[i];
			auto wrapper = make_shared<ArrowArrayWrapper>();
			wrapper->arrow_array = *child;
			child->release = nullptr;

			auto &chunked_array = arrays[i];
			chunked_array->AddChunk(std::move(wrapper));
		}
	}

public:
	shared_ptr<arrow::Table> ToTable(vector<LogicalType> types, vector<string> names) {
		auto table = make_shared<arrow::Table>(std::move(arrays), std::move(schema), std::move(types), std::move(names), properties);
		return table;
	}

public:
	ClientProperties properties;
	shared_ptr<ArrowSchemaWrapper> schema;
	vector<shared_ptr<arrow::ChunkedArray>> arrays;
	idx_t rows_per_batch = 2048;
};

} // namespace

static bool FetchArrowChunk(ArrowConversionResult &result, ChunkScanState &scan_state) {
	auto rows_per_batch = result.rows_per_batch;
	auto properties = result.properties;

	ArrowArray data;
	idx_t count;
	count = ArrowUtil::FetchChunk(scan_state, properties, rows_per_batch, &data);
	if (count == 0) {
		return false;
	}
	result.AddChunk(std::move(data));
	return true;
}

shared_ptr<arrow::Table> ArrowConversion::ConvertToTable(unique_ptr<QueryResult> result) {
	QueryResultChunkScanState scan_state(*result.get());
	vector<shared_ptr<arrow::ChunkedArray>> arrays;

	auto &query_result = *result;

	ArrowConversionResult conversion(result->client_properties);

	auto &arrow_schema = conversion.schema->arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, query_result.types, query_result.names,
	                              query_result.client_properties);

	while (FetchArrowChunk(conversion, scan_state)) {
	}
	return conversion.ToTable(result->types, result->names);
}

} // namespace ac
} // namespace duckdb
