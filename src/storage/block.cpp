#include "duckdb/storage/block.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

namespace duckdb {

Block::Block(Allocator &allocator, block_id_t id)
    : FileBuffer(allocator, FileBufferType::BLOCK, Storage::BLOCK_ALLOC_SIZE), id(id) {
}

Block::Block(FileBuffer &source, block_id_t id) : FileBuffer(source, FileBufferType::BLOCK), id(id) {
	D_ASSERT(GetMallocedSize() == Storage::BLOCK_ALLOC_SIZE);
	D_ASSERT(size == Storage::BLOCK_SIZE);
}

BlockPointer BlockPointer::Deserialize(MetaBlockReader &reader) {
	auto block_id = reader.Read<block_id_t>();
	auto offset = reader.Read<uint32_t>();
	return BlockPointer(block_id, offset);
}

} // namespace duckdb
