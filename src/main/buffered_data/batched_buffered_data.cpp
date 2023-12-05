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
		min_batch_index = pipeline.GetMinimumBatchIndex();
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

void BatchedBufferedData::UnblockSinks() {
	auto &estimated_min = replenish_state.estimated_min_tuples;
	auto &estimated_others = replenish_state.estimated_other_tuples;

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
	auto it = batches.find(index);
	if (it == batches.end()) {
		return 0;
	}
	auto &batch = *it->second;
	return batch.tuple_count;
}

bool BatchedBufferedData::BuffersAreFull() {
	auto &estimated_min = replenish_state.estimated_min_tuples;
	auto &estimated_others = replenish_state.estimated_other_tuples;

	lock_guard<mutex> lock(glock);
	bool min_filled = GetTuplesForBatch(min_batch_index) + estimated_min >= CURRENT_BATCH_BUFFER_SIZE;
	bool others_filled = other_batches_tuple_count + estimated_others >= OTHER_BATCHES_BUFFER_SIZE;
	if (min_filled && others_filled) {
		// Maybe stop already if only 'min_filled' is true?
		return true;
	}
	return false;
}

void BatchedBufferedData::UpdateMinBatchIndex() {
	lock_guard<mutex> lock(glock);
	auto &pipeline = GetPipeline();
	auto min_index = pipeline.GetMinimumBatchIndex();
	if (min_batch_index != min_index) {
		D_ASSERT(min_index > min_batch_index);
		min_batch_index = min_index;

		// We can't make any assumptions about how many tuples are buffered now
		replenish_state.estimated_min_tuples = 0;
	}
}

void BatchedBufferedData::ResetReplenishState() {
	replenish_state.estimated_min_tuples = 0;
	replenish_state.estimated_other_tuples = 0;
}

void BatchedBufferedData::ReplenishBuffer(BufferedQueryResult &result) {
	if (!context) {
		// Result has already been closed
		return;
	}
	UpdateMinBatchIndex();
	ResetReplenishState();
	if (BuffersAreFull()) {
		return;
	}
	UnblockSinks();
	// Let the executor run until the buffer is no longer empty
	auto context_lock = context->LockContext();
	while (!PendingQueryResult::IsFinished(context->ExecuteTaskInternal(*context_lock, result))) {
		UpdateMinBatchIndex();
		if (BuffersAreFull()) {
			break;
		}
		// Check if we need to unblock more sinks to reach the buffer size
		UnblockSinks();
	}
	UpdateMinBatchIndex();
}

unique_ptr<DataChunk> BatchedBufferedData::Scan() {
	lock_guard<mutex> lock(glock);
	unique_ptr<DataChunk> chunk;
	auto it = batches.begin();
	if (it == batches.end()) {
		context.reset();
		return nullptr;
	}

	// Take a chunk from the current batch
	auto &data = *it->second;
	chunk = std::move(data.buffered_chunks.front());
	data.buffered_chunks.pop();

	if (chunk) {
		Printer::Print(StringUtil::Format("Batch Index: %d | Buffer capacity: %d | Chunk size: %d", data.batch_index,
		                                  data.tuple_count, chunk->size()));
		data.tuple_count -= chunk->size();
	}
	if (data.buffered_chunks.empty()) {
		batches.erase(it);
	}
	return chunk;
}

void BatchedBufferedData::Append(unique_ptr<DataChunk> chunk, optional_idx batch_p) {
	auto batch = batch_p.GetIndex();
	unique_lock<mutex> lock(glock);

	auto it = batches.find(batch);
	if (it == batches.end()) {
		// Create the BufferedDataBatch if it doesn't exist yet
		auto result = batches.emplace(batch, make_uniq<BufferedDataBatch>(batch));
		D_ASSERT(result.second);
		it = result.first;
	}

	auto &data = *it->second;
	if (data.batch_index != min_batch_index) {
		// Keep track of the count of tuples of "other" batches
		other_batches_tuple_count += chunk->size();
	}
	data.tuple_count += chunk->size();
	data.buffered_chunks.push(std::move(chunk));
}

} // namespace duckdb
