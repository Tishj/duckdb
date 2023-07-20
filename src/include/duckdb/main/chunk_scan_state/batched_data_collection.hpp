#pragma once

#include "duckdb/main/chunk_scan_state.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/preserved_error.hpp"

namespace duckdb {

class BatchedDataCollection;
struct BatchedChunkIteratorRange;
struct BatchedChunkScanState;

class BatchCollectionChunkScanState : public ChunkScanState {
public:
	BatchCollectionChunkScanState(BatchedDataCollection &result, BatchedChunkIteratorRange &range);
	~BatchCollectionChunkScanState();

public:
	bool LoadNextChunk(PreservedError &error) override;
	bool HasError() const override;
	PreservedError &GetError() override;
	vector<LogicalType> &Types() override;
	vector<string> &Names() override;

private:
	bool InternalLoad(PreservedError &error);

private:
	BatchedDataCollection &result;
	BatchedChunkScanState state;
};

} // namespace duckdb
