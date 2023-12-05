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
	BufferedDataBatch(idx_t batch) : batch_index(batch) {
	}

public:
	idx_t batch_index;
	queue<unique_ptr<DataChunk>> buffered_chunks;
	idx_t tuple_count = 0;
};

struct ReplenishBufferState {
	idx_t min_batch;
	idx_t estimated_min_tuples;
	idx_t estimated_other_tuples;
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

public:
	void SetPipeline(Pipeline &pipeline);

private:
	ReplenishBufferState InitializeState();
	void UnblockSinks(ReplenishBufferState &state);
	Pipeline &GetPipeline();
	idx_t GetTuplesForBatch(idx_t batch);
	bool BuffersAreFull(ReplenishBufferState &state);
	bool OtherBatchesFilled() const;

private:
	map<idx_t, BlockedSink> blocked_sinks;
	//! The queue of chunks
	map<idx_t, unique_ptr<BufferedDataBatch>> batches;
	//! The amount of tuples buffered for the other batches
	atomic<idx_t> other_batches_tuple_count;
	//! The final pipeline
	optional_ptr<Pipeline> pipeline;
};

} // namespace duckdb
