#include "duckdb/execution/index/art/swizzleable_pointer.hpp"
#include "duckdb/execution/index/art/rowid_leaf.hpp"

namespace duckdb {
SwizzleablePointer::~SwizzleablePointer() {
	Reset();
}

SwizzleablePointer::SwizzleablePointer(duckdb::MetaBlockReader &reader) {
	auto block_pointer = BlockPointer::Deserialize(reader);

	if ((block_pointer.offset >> 31) & 1) {
		// Actually a rowid in disguise
		auto rowid_leaf = new RowidLeaf((row_t)block_pointer.block_id);
		pointer = (uint64_t)rowid_leaf;
		SetRowid();
		return;
	}
	if (block_pointer.IsInvalid()) {
		pointer = 0;
		return;
	}
	idx_t pointer_size = sizeof(pointer) * 8;
	pointer = block_pointer.block_id;
	pointer = pointer << (pointer_size / 2);
	pointer += block_pointer.offset;
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
			delete (BaseNode *)Pointer();
		}
	}
	*this = nullptr;
}

BaseNode *SwizzleablePointer::Unswizzle(ART &art) {
	if (IsSwizzled()) {
		// This means our pointer is not yet in memory, gotta deserialize this
		auto block_info = GetSwizzledBlockInfo();
		D_ASSERT(!IsRowid()); // RowIdLeaf can and should not be deserialized
		*this = Node::Deserialize(art, block_info.block_id, block_info.offset);
	}
	return (BaseNode *)Pointer();
}

BlockPointer SwizzleablePointer::Serialize(ART &art, duckdb::MetaBlockWriter &writer) {
	if (Pointer()) {
		Unswizzle(art);
		return ((BaseNode *)Pointer())->Serialize(art, writer);
	} else {
		return BlockPointer::Invalid();
	}
}

} // namespace duckdb
