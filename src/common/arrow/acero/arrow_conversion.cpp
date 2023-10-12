#include "duckdb/main/acero/arrow_conversion.hpp"
#include "duckdb/main/chunk_scan_state/query_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_query_result.hpp"

namespace duckdb {
namespace ac {

namespace {

struct ArrowConversionResult {
public:
	ArrowConversionResult(ClientProperties properties) : properties(properties) {
		schema = make_uniq<ArrowSchemaWrapper>();
	}

public:
	void AddChunk(ArrowArrayWrapper &array_wrapper) {
		auto &array = array_wrapper.arrow_array;
		if (arrays.empty()) {
			for (int64_t i = 0; i < array.n_children; i++) {
				arrays.push_back(make_uniq<arrow::ChunkedArray>());
			}
		}
		D_ASSERT(!arrays.empty());
		for (int64_t i = 0; i < array.n_children; i++) {
			auto child = array.children[i];
			auto wrapper = make_uniq<ArrowArrayWrapper>();
			wrapper->arrow_array = *child;
			child->release = nullptr;

			auto &chunked_array = arrays[i];
			chunked_array->AddChunk(std::move(wrapper));
		}
		// array.release = nullptr;
	}

public:
	shared_ptr<arrow::Table> ToTable(vector<LogicalType> types, vector<string> names) {
		auto table = make_shared<arrow::Table>(std::move(arrays), std::move(schema), std::move(types), std::move(names), properties);
		return table;
	}

public:
	ClientProperties properties;
	unique_ptr<ArrowSchemaWrapper> schema;
	vector<unique_ptr<arrow::ChunkedArray>> arrays;
	idx_t rows_per_batch = 2048;
};

} // namespace

shared_ptr<arrow::Table> ArrowConversion::ConvertToTable(unique_ptr<QueryResult> result) {
	D_ASSERT(result->type == QueryResultType::ARROW_RESULT);
	auto &arrow_result = result->Cast<ArrowQueryResult>();
	ArrowConversionResult conversion(arrow_result.client_properties);
	auto arrays = arrow_result.ConsumeArrays();
	for (auto &array : arrays) {
		conversion.AddChunk(*array);
	}

	auto &schema = conversion.schema->arrow_schema;
	return conversion.ToTable(result->types, result->names);
}

} // namespace ac
} // namespace duckdb
