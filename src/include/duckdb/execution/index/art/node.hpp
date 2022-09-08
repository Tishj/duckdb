//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

class ART;
class Node;
class SwizzleablePointer;

struct InternalType {
	explicit InternalType(BaseNode *n);
	void Set(uint8_t *key_p, uint16_t key_size_p, SwizzleablePointer *children_p, uint16_t children_size_p);
	uint8_t *key;
	uint16_t key_size;
	SwizzleablePointer *children;
	uint16_t children_size;
};

class Node : public BaseNode {
public:
	static const uint8_t EMPTY_MARKER = 48;
	explicit Node(NodeType type);

	virtual ~Node() {
	}
	//! number of non-null children
	uint16_t count;
	//! compressed path (prefix)
	Prefix prefix;

public:
	// -----------------------------
	// Accessors
	// -----------------------------

	//! Get the total amount of child entries in the node
	idx_t Count() const override {
		return count;
	}
	//! Set the total amount of child entries in the node
	void SetCount(idx_t new_count) override {
		count = new_count;
	}
	//! Get a (constant) reference to the prefix of the node
	const Prefix &GetPrefix() const override {
		return prefix;
	}
	//! Get a mutable reference to the prefix of the node
	Prefix &GetMutPrefix() override {
		return prefix;
	}
	//! Set the prefix of the node
	void SetPrefix(Prefix &&prefix) override {
		this->prefix = move(prefix);
	}

	// -----------------------------
	// Methods
	// -----------------------------

	//! Create leaf node
	static BaseNode *CreateLeaf(Key &value, unsigned depth, row_t row_id, bool is_primary);
	//! Insert leaf into inner node
	static void InsertLeaf(BaseNode *&node, uint8_t key, BaseNode *new_node);
	//! Erase entry from node
	static void Erase(BaseNode *&node, idx_t pos, ART &art);

	// -----------------------------
	// Storage
	// -----------------------------

	//! Serialize this Node
	BlockPointer Serialize(ART &art, duckdb::MetaBlockWriter &writer) override;

	static BaseNode *Deserialize(ART &art, idx_t block_id, idx_t offset);

private:
	//! Serialize Internal Nodes
	BlockPointer SerializeInternal(ART &art, duckdb::MetaBlockWriter &writer, InternalType &internal_type);
	//! Deserialize Internal Nodes
	void DeserializeInternal(duckdb::MetaBlockReader &reader) override;
};

} // namespace duckdb
