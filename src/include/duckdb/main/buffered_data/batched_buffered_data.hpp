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

struct BufferedDataBatch {
public:
	BufferedDataBatch() = delete;
	BufferedDataBatch(BufferedDataBatch &other) = delete;
	BufferedDataBatch(idx_t batch);

public:
	idx_t batch_index;
	queue<unique_ptr<DataChunk>> buffered_chunks;
};

class BatchedBufferedData : public BufferedData {
private:
	//! (roughly) The max amount of tuples we'll keep buffered at a time
	static constexpr idx_t BUFFER_SIZE = 100000;
	static constexpr idx_t CURRENT_BATCH_BUFFER_SIZE = BUFFER_SIZE * 0.6;
	static constexpr idx_t OTHER_BATCHES_BUFFER_SIZE = BUFFER_SIZE * 0.4;

public:
	BatchedBufferedData(shared_ptr<ClientContext> context);

public:
	void Append(unique_ptr<DataChunk> chunk, optional_idx batch = optional_idx()) override;
	void AddToBacklog(BlockedSink blocked_sink) override;
	bool BufferIsFull(optional_idx batch = optional_idx()) override;
	void ReplenishBuffer(BufferedQueryResult &result) override;
	unique_ptr<DataChunk> Scan() override;

private:
	void UnblockSinks(idx_t &estimated_min, idx_t estimated_others);

private:
	unordered_map<idx_t, BlockedSink> blocked_sinks;
	//! The queue of chunks
	unordered_map<idx_t, unique_ptr<BufferedDataBatch>> batches;
	//! The amount of tuples buffered for the current batch
	atomic<idx_t> current_batch_tuple_count;
	//! The amount of tuples buffered for the other batches
	atomic<idx_t> other_batches_tuple_count;
};

} // namespace duckdb
