#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/buffered_query_result.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

void BatchedBufferedData::AddToBacklog(BlockedSink blocked_sink) {
	lock_guard<mutex> lock(glock);
	auto batch = blocked_sink.batch.GetIndex();
	D_ASSERT(blocked_sinks.find(batch) == blocked_sinks.end());
	blocked_sinks.emplace(std::make_pair(batch, blocked_sink));
}

BatchedBufferedData::BatchedBufferedData(shared_ptr<ClientContext> context) : BufferedData(std::move(context)) {
}

void BatchedBufferedData::SetPipeline(Pipeline &pipeline) {
	lock_guard<mutex> lock(glock);
	if (!this->pipeline) {
		this->pipeline = &pipeline;
	}
	// All Sinks are assumed to be scheduled on the same pipeline
	D_ASSERT(this->pipeline == &pipeline);
}

Pipeline &BatchedBufferedData::GetPipeline() {
	return *pipeline;
}

bool BatchedBufferedData::BufferIsFull(optional_idx batch_p) {
	auto batch = batch_p.GetIndex();
	lock_guard<mutex> lock(glock);
	auto &pipeline = GetPipeline();
	auto min_index = pipeline.GetMinimumBatchIndex();
	if (batch == min_index) {
		auto it = batches.find(batch);
		if (it == batches.end()) {
			return false;
		}
		auto &buffered_batch = *it->second;
		return buffered_batch.tuple_count >= CURRENT_BATCH_BUFFER_SIZE;
	} else {
		return other_batches_tuple_count >= OTHER_BATCHES_BUFFER_SIZE;
	}
}

bool BatchedBufferedData::OtherBatchesFilled() const {
	return other_batches_tuple_count >= OTHER_BATCHES_BUFFER_SIZE;
}

void BatchedBufferedData::UnblockSinks(ReplenishBufferState &state) {
	auto &estimated_min = state.estimated_min_tuples;
	auto &estimated_others = state.estimated_other_tuples;

	lock_guard<mutex> lock(glock);
	auto &pipeline = GetPipeline();
	auto min_index = pipeline.GetMinimumBatchIndex();
	bool min_batch_filled = GetTuplesForBatch(min_index) >= CURRENT_BATCH_BUFFER_SIZE;
	bool other_batches_filled = OtherBatchesFilled();
	if (min_batch_filled && other_batches_filled) {
		return;
	}

	auto min_sink_it = blocked_sinks.find(min_index);
	if (min_sink_it != blocked_sinks.end()) {
		// Reschedule the sink
		auto &min_sink = min_sink_it->second;
		estimated_min += min_sink.chunk_size;
		min_sink.state.Callback();
		blocked_sinks.erase(min_sink_it);
	}

	queue<idx_t> sinks_to_remove;
	for (auto &kv : blocked_sinks) {
		auto &index = kv.first;
		auto &blocked_sink = kv.second;
		D_ASSERT(blocked_sink.batch.GetIndex() == index);
		if (other_batches_tuple_count + estimated_others >= OTHER_BATCHES_BUFFER_SIZE) {
			// We have unblocked enough sinks already
			break;
		}
		estimated_others += blocked_sink.chunk_size;
		blocked_sink.state.Callback();
		sinks_to_remove.push(index);
	}

	// Clean up the indices from the map
	while (!sinks_to_remove.empty()) {
		auto index = sinks_to_remove.front();
		sinks_to_remove.pop();
		blocked_sinks.erase(index);
	}
}

idx_t BatchedBufferedData::GetTuplesForBatch(idx_t index) {
	D_ASSERT(batches.find(index) != batches.end());
	auto &batch = *batches.at(index);
	return batch.tuple_count;
}

bool BatchedBufferedData::BuffersAreFull(ReplenishBufferState &state) {
	auto &estimated_min = state.estimated_min_tuples;
	auto &estimated_others = state.estimated_other_tuples;

	lock_guard<mutex> lock(glock);
	auto &pipeline = GetPipeline();
	auto min_index = pipeline.GetMinimumBatchIndex();
	if (state.min_batch != min_index) {
		D_ASSERT(min_index > state.min_batch);
		state.min_batch = min_index;
		// We have no clue how many tuples are expected for this
		estimated_min = 0;
	}
	bool min_filled = GetTuplesForBatch(min_index) + estimated_min >= CURRENT_BATCH_BUFFER_SIZE;
	bool others_filled = other_batches_tuple_count + estimated_others >= OTHER_BATCHES_BUFFER_SIZE;
	if (min_filled && others_filled) {
		// Maybe stop already if only 'min_filled' is true?
		return true;
	}
	return false;
}

ReplenishBufferState BatchedBufferedData::InitializeState() {
	lock_guard<mutex> lock(glock);
	auto &pipeline = GetPipeline();
	ReplenishBufferState state;
	state.min_batch = pipeline.GetMinimumBatchIndex();
	state.estimated_min_tuples = 0;
	state.estimated_other_tuples = 0;
	return state;
}

void BatchedBufferedData::ReplenishBuffer(BufferedQueryResult &result) {
	if (!context) {
		// Result has already been closed
		return;
	}
	auto state = InitializeState();
	if (BuffersAreFull(state)) {
		return;
	}
	UnblockSinks(state);
	// Let the executor run until the buffer is no longer empty
	auto context_lock = context->LockContext();
	while (!PendingQueryResult::IsFinished(context->ExecuteTaskInternal(*context_lock, result))) {
		if (BuffersAreFull(state)) {
			break;
		}
		// Check if we need to unblock more sinks to reach the buffer size
		UnblockSinks(state);
	}
}

unique_ptr<DataChunk> BatchedBufferedData::Scan() {
	lock_guard<mutex> lock(glock);
	unique_ptr<DataChunk> chunk;
	auto &pipeline = GetPipeline();
	auto min_batch = pipeline.GetMinimumBatchIndex();
	auto &it = batches.at(min_batch);

	chunk = std::move(it->buffered_chunks.front());
	it->buffered_chunks.pop();
	if (chunk) {
		it->tuple_count -= chunk->size();
	}
	return chunk;
}

void BatchedBufferedData::Append(unique_ptr<DataChunk> chunk, optional_idx batch_p) {
	auto batch = batch_p.GetIndex();
	unique_lock<mutex> lock(glock);
	auto it = batches.find(batch);
	if (it == batches.end()) {
		auto result = batches.emplace(batch, make_uniq<BufferedDataBatch>(batch));
		D_ASSERT(result.second);
		it = result.first;
	}
	auto &data = *it->second;
	data.tuple_count += chunk->size();
	data.buffered_chunks.push(std::move(chunk));
}

} // namespace duckdb
