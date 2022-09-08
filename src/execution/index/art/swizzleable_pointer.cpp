#include "duckdb/execution/index/art/swizzleable_pointer.hpp"
#include "duckdb/execution/index/art/rowid_leaf.hpp"

namespace duckdb {
SwizzleablePointer::~SwizzleablePointer() {
	Reset();
}

SwizzleablePointer::SwizzleablePointer(duckdb::MetaBlockReader &reader) {
	block_id_t raw_block_id = reader.Read<block_id_t>();
	uint32_t raw_offset = reader.Read<uint32_t>();
	if ((raw_offset >> 31) & 1) {
		//// Actually a rowid in disguise
		// auto rowid_leaf = new RowidLeaf((row_t)raw_block_id);
		// pointer = (uint64_t)rowid_leaf;
		// SetRowid();
		return;
	}

	idx_t block_id = raw_block_id;
	idx_t offset = raw_offset;
	if (block_id == DConstants::INVALID_INDEX || offset == DConstants::INVALID_INDEX) {
		pointer = 0;
		return;
	}
	idx_t pointer_size = sizeof(pointer) * 8;
	pointer = block_id;
	pointer = pointer << (pointer_size / 2);
	pointer += offset;
	SetSwizzled();
}

SwizzleablePointer &SwizzleablePointer::operator=(const BaseNode *ptr) {
	FlaggedPointer::operator=(ptr);
	return *this;
}

bool operator!=(const SwizzleablePointer &s_ptr, const uint64_t &ptr) {
	return (s_ptr.Pointer() != ptr);
}

BlockPointer SwizzleablePointer::GetSwizzledBlockInfo() {
	D_ASSERT(IsSwizzled());
	UnsetSwizzled();
	idx_t pointer_size = sizeof(pointer) * 8;
	uint32_t block_id = Pointer() >> (pointer_size / 2);
	uint32_t offset = Pointer() & 0xffffffff;
	return {block_id, offset};
}

void SwizzleablePointer::Reset() {
	if (Pointer()) {
		if (!IsSwizzled()) {
			delete (Node *)Pointer();
		}
	}
	*this = nullptr;
}

Node *SwizzleablePointer::Unswizzle(ART &art) {
	if (IsSwizzled()) {
		// This means our pointer is not yet in memory, gotta deserialize this
		// first we unset the bae
		auto block_info = GetSwizzledBlockInfo();
		D_ASSERT(!IsRowid()); // RowIdLeaf can and should not be deserialized
		*this = Node::Deserialize(art, block_info.block_id, block_info.offset);
	}
	return (Node *)Pointer();
}

BlockPointer SwizzleablePointer::Serialize(ART &art, duckdb::MetaBlockWriter &writer) {
	if (Pointer()) {
		Unswizzle(art);
		return ((Node *)Pointer())->Serialize(art, writer);
	} else {
		return {(block_id_t)DConstants::INVALID_INDEX, (uint32_t)DConstants::INVALID_INDEX};
	}
}
} // namespace duckdb
