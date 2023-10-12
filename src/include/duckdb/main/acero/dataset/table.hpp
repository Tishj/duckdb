#pragma once

#include "duckdb/common/arrow/arrow_wrapper.hpp"

namespace duckdb {
namespace arrow {

using Array = ArrowArrayWrapper;
using Schema = ArrowSchemaWrapper;

struct ChunkedArray {
public:
	ChunkedArray() {
	}

public:
	void AddChunk(shared_ptr<Array> &&array) {
		chunks.push_back(array);
	}
	ArrowArray TakeChunk(idx_t index) {
		ArrowArray result;
		auto &chunk_p = chunks[index];
		result = chunk_p->arrow_array;
		chunk_p->arrow_array.release = nullptr;
		return result;
	}

	vector<shared_ptr<Array>> chunks;
};

// This wraps a C-data interface arrow array
struct Table {
public:
	Table(vector<shared_ptr<ChunkedArray>> arrays, shared_ptr<Schema> schema, vector<LogicalType> types,
	      vector<string> names, ClientProperties properties)
	    : arrays(std::move(arrays)), schema(std::move(schema)), types(std::move(types)), names(std::move(names)),
	      properties(properties) {
	}

public:
	vector<shared_ptr<ChunkedArray>> arrays;
	shared_ptr<Schema> schema;
	vector<LogicalType> types;
	vector<string> names;
	ClientProperties properties;
};

} // namespace arrow
} // namespace duckdb
