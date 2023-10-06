#pragma once

#include "duckdb/main/chunk_scan_state.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/preserved_error.hpp"

namespace duckdb {

class BatchCollectionChunkScanState : public ChunkScanState {
public:
	BatchCollectionChunkScanState(BatchedDataCollection &collection, BatchedChunkIteratorRange &range,
	                              ClientContext &context);
	~BatchCollectionChunkScanState();

public:
	BatchCollectionChunkScanState(const BatchCollectionChunkScanState &other) = delete;
	BatchCollectionChunkScanState &operator=(const BatchCollectionChunkScanState &other) = delete;
	BatchCollectionChunkScanState(BatchCollectionChunkScanState &&other) = default;

public:
	bool LoadNextChunk(PreservedError &error) override;
	bool HasError() const override;
	PreservedError &GetError() override;
	const vector<LogicalType> &Types() const override;
	const vector<string> &Names() const override;

private:
	void InternalLoad(PreservedError &error);

private:
	BatchedDataCollection &collection;
	BatchedChunkScanState state;
};

} // namespace duckdb
