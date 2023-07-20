#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class DataChunk;

//! Abstract chunk fetcher
class ChunkScanState {
public:
	explicit ChunkScanState() {
	}
	virtual ~ChunkScanState() {
	}

public:
	virtual bool LoadNextChunk(PreservedError &error) = 0;
	virtual bool HasError() const = 0;
	virtual PreservedError &GetError() = 0;
	virtual vector<LogicalType> &Types() = 0;
	virtual vector<string> &Names() = 0;
	idx_t CurrentOffset() const;
	idx_t RemainingInChunk() const;
	DataChunk &CurrentChunk();
	bool ChunkIsEmpty() const;
	bool Finished() const;
	bool ScanStarted() const;
	void IncreaseOffset(idx_t increment);

protected:
	idx_t offset = 0;
	bool finished = false;
	unique_ptr<DataChunk> current_chunk;
};

} // namespace duckdb
