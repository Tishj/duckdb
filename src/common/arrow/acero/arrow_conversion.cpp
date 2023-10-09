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
		schema = make_shared<ArrowSchemaWrapper>();
	}

public:
	void AddChunk(ArrowArrayWrapper &array_wrapper) {
		auto &array = array_wrapper.arrow_array;
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
		//array.release = nullptr;
	}

public:
	shared_ptr<arrow::Table> ToTable() {
		auto table = make_shared<arrow::Table>(std::move(arrays), std::move(schema));
		return table;
	}

public:
	ClientProperties properties;
	shared_ptr<ArrowSchemaWrapper> schema;
	vector<shared_ptr<arrow::ChunkedArray>> arrays;
	idx_t rows_per_batch = 2048;
};

} // namespace

//static bool FetchArrowChunk(ArrowConversionResult &result, ChunkScanState &scan_state) {
//	auto rows_per_batch = result.rows_per_batch;
//	auto properties = result.properties;

//	ArrowArray data;
//	idx_t count;
//	count = ArrowUtil::FetchChunk(scan_state, properties, rows_per_batch, &data);
//	if (count == 0) {
//		return false;
//	}
//	result.AddChunk(std::move(data));
//	return true;
//}

shared_ptr<arrow::Table> ArrowConversion::ConvertToTable(unique_ptr<QueryResult> result) {
	D_ASSERT(result->type == QueryResultType::ARROW_RESULT);
	auto &arrow_result = result->Cast<ArrowQueryResult>();
	ArrowConversionResult conversion(arrow_result.client_properties);
	auto arrays = arrow_result.ConsumeArrays();
	for (auto &array : arrays) {
		conversion.AddChunk(*array);
	}

	auto &schema = conversion.schema->arrow_schema;
	return conversion.ToTable();
}

} // namespace ac
} // namespace duckdb
