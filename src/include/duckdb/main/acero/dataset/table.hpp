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

private:
	vector<shared_ptr<Array>> chunks;
};

// This wraps a C-data interface arrow array
struct Table {
public:
	Table(vector<shared_ptr<ChunkedArray>> arrays, shared_ptr<Schema> schema)
	    : arrays(std::move(arrays)), schema(std::move(schema)) {
	}

public:
	vector<shared_ptr<ChunkedArray>> arrays;
	shared_ptr<Schema> schema;
};

} // namespace arrow
} // namespace duckdb
