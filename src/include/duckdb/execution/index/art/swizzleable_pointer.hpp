//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/swizzleable_pointer.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/hybrid_pointer.hpp"

namespace duckdb {

class SwizzleablePointer : public HybridPointer<uint64_t, uint64_t> {
public:
	~SwizzleablePointer() override;
	explicit SwizzleablePointer(duckdb::MetaBlockReader &reader);
	SwizzleablePointer() : HybridPointer() {
	}

public:
	//! Transforms from Node* to uint64_t
	SwizzleablePointer &operator=(const Node *ptr);
	friend bool operator!=(const SwizzleablePointer &s_ptr, const uint64_t &ptr);

	//! Get the underlying pointer value
	uint64_t Pointer() const {
		return this->GetSecondary();
	}
	//! Get the swizzled data (the block info)
	uint64_t BlockInfo() const {
		return this->GetPrimary();
	}
	//! Extracts block info from swizzled pointer
	BlockPointer GetSwizzledBlockInfo();
	//! Checks if pointer is swizzled
	bool IsSwizzled();
	//! Deletes the underlying object (if necessary) and set the pointer to null_ptr
	void Reset();
	//! Unswizzle the pointer (if possible)
	Node *Unswizzle(ART &art);

	BlockPointer Serialize(ART &art, duckdb::MetaBlockWriter &writer);
};
} // namespace duckdb
