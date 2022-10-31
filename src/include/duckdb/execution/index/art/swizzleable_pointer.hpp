//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/swizzleable_pointer.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/flagged_pointer.hpp"

namespace duckdb {

//! Swizzleable pointers are owning pointers

class SwizzleablePointer : public FlaggedPointer {
public:
	~SwizzleablePointer() override;
	explicit SwizzleablePointer(duckdb::MetaBlockReader &reader);
	SwizzleablePointer() : FlaggedPointer() {
	}

public:
	void SetSwizzled() {
		Set<0>();
	}
	void SetRowid() {
		Set<1>();
	}
	void UnsetSwizzled() {
		// This is destructive. Pointer will be invalid after this operation.
		// That's okay because this is only ever called from UnsetSwizzled.
		Unset<0>();
	}
	void UnsetRowid() {
		Unset<1>();
	}

	bool IsSwizzled() const {
		return IsSet<0>();
	}
	bool IsRowid() const {
		return IsSet<1>();
	}

	//! Transforms from Node* to uint64_t
	SwizzleablePointer &operator=(const BaseNode *ptr);
	friend bool operator!=(const SwizzleablePointer &s_ptr, const uint64_t &ptr);

	//! Get the swizzled data (the block info)
	uint64_t BlockInfo() const {
		return this->Pointer();
	}
	//! Extracts block info from swizzled pointer
	BlockPointer GetSwizzledBlockInfo();
	//! Deletes the underlying object (if necessary) and set the pointer to null_ptr
	void Reset();
	//! Unswizzle the pointer (if possible)
	BaseNode *Unswizzle(ART &art);

	BlockPointer Serialize(ART &art, duckdb::MetaBlockWriter &writer);
};

} // namespace duckdb
