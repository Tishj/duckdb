#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/buffered_query_result.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/execution/operator/helper/physical_buffered_batch_collector.hpp"

namespace duckdb {

void BatchedBufferedData::AddToBacklog(BlockedSink blocked_sink) {
	lock_guard<mutex> lock(glock);
	if (blocked_sink.is_minimum) {
		D_ASSERT(!blocked_min);
		blocked_min = make_uniq<BlockedSink>(blocked_sink);
	} else {
		blocked_sinks.push(blocked_sink);
	}
}

BatchedBufferedData::BatchedBufferedData(shared_ptr<ClientContext> context) : BufferedData(std::move(context)) {
}

bool BatchedBufferedData::BufferIsFull(bool is_minimum_batch) {
	if (is_minimum_batch) {
		return current_batch_tuple_count >= CURRENT_BATCH_BUFFER_SIZE;
	} else {
		return other_batches_tuple_count >= OTHER_BATCHES_BUFFER_SIZE;
	}
}

void BatchedBufferedData::UnblockSinks() {
	auto &estimated_min = replenish_state.estimated_min_tuples;
	auto &estimated_others = replenish_state.estimated_other_tuples;

	if (blocked_min && current_batch_tuple_count >= CURRENT_BATCH_BUFFER_SIZE) {
		auto &sink = *blocked_min;
		estimated_min += sink.chunk_size;
		sink.state.Callback();
		blocked_min.reset();
	}

	while (!blocked_sinks.empty()) {
		auto &blocked_sink = blocked_sinks.front();
		if (other_batches_tuple_count >= OTHER_BATCHES_BUFFER_SIZE) {
			break;
		}
		estimated_others += blocked_sink.chunk_size;
		blocked_sink.state.Callback();
		blocked_sinks.pop();
	}
}

bool BatchedBufferedData::BuffersAreFull() {
	auto &estimated_min = replenish_state.estimated_min_tuples;
	auto &estimated_others = replenish_state.estimated_other_tuples;

	bool min_filled = current_batch_tuple_count + estimated_min >= CURRENT_BATCH_BUFFER_SIZE;
	bool others_filled = other_batches_tuple_count + estimated_others >= OTHER_BATCHES_BUFFER_SIZE;
	if (min_filled && others_filled) {
		// Maybe stop already if only 'min_filled' is true?
		return true;
	}
	return false;
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
	ResetReplenishState();
	if (BuffersAreFull()) {
		return;
	}
	UnblockSinks();
	// Let the executor run until the buffer is no longer empty
	auto context_lock = context->LockContext();
	while (!PendingQueryResult::IsFinished(context->ExecuteTaskInternal(*context_lock, result))) {
		if (BuffersAreFull()) {
			break;
		}
		// Check if we need to unblock more sinks to reach the buffer size
		UnblockSinks();
	}
}

unique_ptr<DataChunk> BatchedBufferedData::Scan() {
	lock_guard<mutex> lock(glock);
	if (batches.empty()) {
		context.reset();
		return nullptr;
	}
	auto chunk = std::move(batches.front());
	batches.pop();

	// auto count = buffered_count.load();
	// Printer::Print(StringUtil::Format("Buffer capacity: %d", count));

	if (chunk) {
		current_batch_tuple_count -= chunk->size();
	}
	return chunk;
}

void BatchedBufferedData::Append(unique_ptr<DataChunk> chunk, LocalSinkState &lstate) {
	auto &state = lstate.Cast<BufferedBatchCollectorLocalState>();
	if (state.GetMinimumBatchIndex() == state.BatchIndex()) {
		unique_lock<mutex> lock(glock);
		while (!state.buffered_chunks.empty()) {
			auto to_append = std::move(state.buffered_chunks.front());
			state.buffered_chunks.pop();
			// If the chunk was ever buffered in the local state we need to subtract the sizes of these chunks from
			// 'other_batches_tuple_count'
			other_batches_tuple_count -= to_append->size();
			batches.push(std::move(to_append));
		}
		count += chunk->size();
		current_batch_tuple_count += chunk->size();
		batches.push(std::move(chunk));
	} else {
		other_batches_tuple_count += chunk->size();
		state.BufferChunk(std::move(chunk));
	}
}

} // namespace duckdb
