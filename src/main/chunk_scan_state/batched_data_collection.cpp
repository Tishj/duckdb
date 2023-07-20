#include "duckdb/main/chunk_scan_state/batched_data_collection.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

BatchCollectionChunkScanState::BatchCollectionChunkScanState(BatchedDataCollection &collection,
                                                             BatchedChunkIteratorRange &range, ClientContext &context)
    : ChunkScanState(), collection(collection) {
	collection.InitializeScan(state, range);
	current_chunk = make_uniq<DataChunk>();
	auto &allocator = BufferManager::GetBufferManager(context).GetBufferAllocator();
	current_chunk->Initialize(allocator, collection.Types());
}

BatchCollectionChunkScanState::~BatchCollectionChunkScanState() {
}

bool BatchCollectionChunkScanState::InternalLoad(PreservedError &error) {
	if (state.range.begin == state.range.end) {
		return false;
	}
	current_chunk->Reset();
	collection.Scan(state, *current_chunk);
	state.range.begin++;
	return true;
}

bool BatchCollectionChunkScanState::HasError() const {
	return false;
}

PreservedError &BatchCollectionChunkScanState::GetError() {
	throw NotImplementedException("BatchDataCollections don't have an internal error object");
}

const vector<LogicalType> &BatchCollectionChunkScanState::Types() const {
	return collection.Types();
}

const vector<string> &BatchCollectionChunkScanState::Names() const {
	throw NotImplementedException("BatchDataCollections don't have names");
}

bool BatchCollectionChunkScanState::LoadNextChunk(PreservedError &error) {
	if (finished) {
		return !finished;
	}
	auto load_result = InternalLoad(error);
	if (!load_result) {
		finished = true;
	}
	offset = 0;
	return !finished;
}

} // namespace duckdb
