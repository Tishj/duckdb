#include "duckdb/execution/index/art/swizzleable_pointer.hpp"

namespace duckdb {
SwizzleablePointer::~SwizzleablePointer() {
	if (Pointer()) {
		if (!IsSwizzled()) {
			delete (Node *)Pointer();
		}
	}
}

SwizzleablePointer::SwizzleablePointer(duckdb::MetaBlockReader &reader) {
	idx_t block_id = reader.Read<block_id_t>();
	idx_t offset = reader.Read<uint32_t>();
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

SwizzleablePointer &SwizzleablePointer::operator=(const Node *ptr) {
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
