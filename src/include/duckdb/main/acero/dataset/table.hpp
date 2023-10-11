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
	void AddChunk(unique_ptr<Array> &&array) {
		chunks.push_back(std::move(array));
	}
	idx_t Count() const {
		return chunks.size();
	}
	ArrowArray TakeChunk(idx_t index) {
		ArrowArray result;
		auto &chunk_p = chunks[index];
		result = chunk_p->arrow_array;
		chunk_p->arrow_array.release = nullptr;
		return result;
	}

private:
	vector<unique_ptr<Array>> chunks;
};

// This wraps a C-data interface arrow array
struct Table {
public:
	Table(vector<unique_ptr<ChunkedArray>> arrays, unique_ptr<Schema> schema)
	    : arrays(std::move(arrays)), schema(std::move(schema)) {
	}

public:
	vector<unique_ptr<ChunkedArray>> arrays;
	unique_ptr<Schema> schema;
};

} // namespace arrow
} // namespace duckdb
