//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/result_arrow_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/query_result.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/main/chunk_scan_state.hpp"
#include "duckdb/main/acero/dataset/table.hpp"

namespace duckdb {
namespace arrow {

class ArrayVectorStream {
public:
	explicit ArrayVectorStream(Table &&table, vector<LogicalType> types, vector<string> names,
	                           ClientProperties properties);

public:
	Table table;
	ArrowArrayStream stream;
	idx_t chunk_index = 0;
	vector<LogicalType> types;
	vector<string> names;
	ClientProperties properties;

private:
	static int GetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out);
	static int GetNext(struct ArrowArrayStream *stream, struct ArrowArray *out);
	static void Release(struct ArrowArrayStream *stream);
	static const char *GetLastError(struct ArrowArrayStream *stream);
};

} // namespace arrow
} // namespace duckdb
