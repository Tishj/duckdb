#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/buffered_query_result.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

void BatchedBufferedData::AddToBacklog(BlockedSink blocked_sink) {
	lock_guard<mutex> lock(glock);
	throw NotImplementedException("TODO: AddToBacklog");
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

bool BatchedBufferedData::BufferIsFull(optional_idx batch) {
	D_ASSERT(batch.IsValid());
	lock_guard<mutex> lock(glock);
	// TODO: how to check if this is the minimum batch index???
	throw NotImplementedException("TODO: BufferIsFull");
}

void BatchedBufferedData::UnblockSinks(idx_t &estimated_min, idx_t estimated_others) {
	if (false) {
		// TODO: determine if the buffers are full before needing to lock
		return;
	}
	// Reschedule enough blocked sinks to populate the buffer
	lock_guard<mutex> lock(glock);
	throw NotImplementedException("TODO: UnblockSinks");
	// TODO: check the minimum batch index and the other batch indexes separately.
}

void BatchedBufferedData::ReplenishBuffer(BufferedQueryResult &result) {
	if (!context) {
		// Result has already been closed
		return;
	}
	if (BufferIsFull()) {
		// The buffer isn't empty yet, just return
		return;
	}
	throw NotImplementedException("TODO: ReplenishBuffer");
	idx_t estimated_min = 0;
	idx_t estimated_others = 0;
	UnblockSinks(estimated_min, estimated_others);
	// Let the executor run until the buffer is no longer empty
	auto context_lock = context->LockContext();
	while (!PendingQueryResult::IsFinished(context->ExecuteTaskInternal(*context_lock, result))) {
		if (current_batch_tuple_count >= BUFFER_SIZE) {
			break;
		}
		// Check if we need to unblock more sinks to reach the buffer size
		UnblockSinks(estimated_min, estimated_others);
	}
}

unique_ptr<DataChunk> BatchedBufferedData::Scan() {
	lock_guard<mutex> lock(glock);
	throw NotImplementedException("TODO: ReplenishBuffer");

	unique_ptr<DataChunk> chunk;
	// TODO: fetch a chunk from the minimum batch index

	// auto count = buffered_count.load();
	// Printer::Print(StringUtil::Format("Buffer capacity: %d", count));

	if (chunk) {
		// TODO: Reduce the count of buffered tuples for the minimum batch index
	}
	return chunk;
}

void BatchedBufferedData::Append(unique_ptr<DataChunk> chunk, optional_idx batch) {
	D_ASSERT(batch.IsValid());
	unique_lock<mutex> lock(glock);
	throw NotImplementedException("TODO: Append");
}

} // namespace duckdb
