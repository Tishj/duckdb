//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/buffered_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

class BufferedQueryResult;

struct BlockedSink {
public:
	BlockedSink(InterruptState state, idx_t chunk_size, bool is_minimum = true)
	    : state(state), chunk_size(chunk_size), is_minimum(is_minimum) {
	}

public:
	//! The handle to reschedule the blocked sink
	InterruptState state;
	//! The amount of tuples this sink would add
	idx_t chunk_size;
	// FIXME: make BlockedSink an abstract class, only BatchedBlockedSink cares about this
	bool is_minimum;
};

class BufferedData {
private:
	//! (roughly) The max amount of tuples we'll keep buffered at a time
	static constexpr idx_t BUFFER_SIZE = 100000;

public:
	BufferedData(shared_ptr<ClientContext> context) : context(context) {
	}
	virtual ~BufferedData() {
	}

public:
	virtual void Append(unique_ptr<DataChunk> chunk) = 0;
	virtual void AddToBacklog(BlockedSink blocked_sink) = 0;
	virtual bool BufferIsFull(bool is_minimum_batch = true) = 0;
	virtual void ReplenishBuffer(BufferedQueryResult &result) = 0;
	virtual unique_ptr<DataChunk> Scan() = 0;

protected:
	shared_ptr<ClientContext> context;
	//! Protect against populate/fetch race condition
	mutex glock;
};

} // namespace duckdb
