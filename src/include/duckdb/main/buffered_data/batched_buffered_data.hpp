//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/batched_buffered_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"

namespace duckdb {

class BufferedQueryResult;

struct BatchedBufferedDataScanState {
	//! The chunk we're currently scanning from
	unique_ptr<DataChunk> chunk = nullptr;
	//! The offset into the current chunk
	idx_t offset = 0;
};

class BatchedBufferedData : public BufferedData {
private:
	//! (roughly) The max amount of tuples we'll keep buffered at a time
	static constexpr idx_t BUFFER_SIZE = 100000;

public:
	BatchedBufferedData(shared_ptr<ClientContext> context);

public:
	void Append(unique_ptr<DataChunk> chunk, optional_idx batch = optional_idx()) override;
	void AddToBacklog(BlockedSink blocked_sink) override;
	bool BufferIsFull() const override;
	void ReplenishBuffer(BufferedQueryResult &result) override;
	unique_ptr<DataChunk> Scan() override;

private:
	void UnblockSinks(idx_t &estimated_tuples);

private:
	queue<BlockedSink> blocked_sinks;
	//! The queue of chunks
	queue<unique_ptr<DataChunk>> buffered_chunks;
	//! The current capacity of the buffer (tuples)
	atomic<idx_t> buffered_count;
	//! Scan state
	BatchedBufferedDataScanState scan_state;
};

} // namespace duckdb
