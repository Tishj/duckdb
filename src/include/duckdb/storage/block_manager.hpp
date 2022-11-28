//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/block_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/block_manager_interface.hpp"

namespace duckdb {
class BlockHandle;
class BufferManager;
class ClientContext;
class DatabaseInstance;

//! BlockManager is an abstract representation to manage blocks on DuckDB backed by a BufferManager. When writing or reading blocks, the
//! BlockManager creates and accesses blocks. The concrete types implement how blocks are stored.
class BlockManager : public BlockManagerInterface {
public:
	explicit BlockManager(BufferManager &buffer_manager) : BlockManagerInterface(), buffer_manager(buffer_manager) {
	}
	virtual ~BlockManager() = default;

	//! The buffer manager
	BufferManager &buffer_manager;

public:
	//! Register a block with the given block id in the base file
	shared_ptr<BlockHandle> RegisterBlock(block_id_t block_id) override;
	//! Convert an existing in-memory buffer into a persistent disk-backed block
	shared_ptr<BlockHandle> ConvertToPersistent(block_id_t block_id, shared_ptr<BlockHandle> old_block) override;

	void UnregisterBlock(block_id_t block_id, bool can_destroy) override;

	static BlockManager &GetBlockManager(ClientContext &context);
	static BlockManager &GetBlockManager(DatabaseInstance &db);

private:
	//! The lock for the set of blocks
	mutex blocks_lock;
	//! A mapping of block id -> BlockHandle
	unordered_map<block_id_t, weak_ptr<BlockHandle>> blocks;
};
} // namespace duckdb
