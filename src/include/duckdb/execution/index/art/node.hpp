//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/meta_block_writer.hpp"

namespace duckdb {
class ART;
class Node;

// Note: SwizzleablePointer assumes top 33 bits of the block_id are 0. Use a different
// pointer implementation if that does not hold.
class SwizzleablePointer;
using ARTPointer = SwizzleablePointer;

struct InternalType {
	explicit InternalType(BaseNode *n);

	void Set(uint8_t *key_p, uint16_t key_size_p, ARTPointer *children_p, uint16_t children_size_p);
	uint8_t *key;
	uint16_t key_size;
	ARTPointer *children;
	uint16_t children_size;
};

struct MergeInfo {
	MergeInfo(ART *l_art, ART *r_art, BaseNode *&l_node, BaseNode *&r_node)
	    : l_art(l_art), r_art(r_art), l_node(l_node), r_node(r_node) {};
	ART *l_art;
	ART *r_art;
	BaseNode *&l_node;
	BaseNode *&r_node;
};

struct ParentsOfNodes {
	ParentsOfNodes(BaseNode *&l_parent, idx_t l_pos, BaseNode *&r_parent, idx_t r_pos)
	    : l_parent(l_parent), l_pos(l_pos), r_parent(r_parent), r_pos(r_pos) {};
	BaseNode *&l_parent;
	idx_t l_pos;
	BaseNode *&r_parent;
	idx_t r_pos;
};

class Node : public BaseNode {
public:
	static const uint8_t EMPTY_MARKER = 48;
	explicit Node(NodeType type);
	virtual ~Node() {
	}

	//! Number of non-null children
	uint16_t count;
	//! Compressed path (prefix)
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
	////! Erase entry from node
	// static void Erase(BaseNode *&node, idx_t pos, ART &art);

	// -----------------------------
	// Storage
	// -----------------------------

	//! Serialize this Node
	BlockPointer Serialize(ART &art, duckdb::MetaBlockWriter &writer) override;

	//! Deserialize this Node
	static BaseNode *Deserialize(ART &art, idx_t block_id, idx_t offset);

	// FIXME: can go?
	//! Get the position of the first child that is greater or equal to the specific byte, or DConstants::INVALID_INDEX
	//! if there are no children matching the criteria
	virtual idx_t GetChildGreaterEqual(uint8_t k, bool &equal) override {
		throw InternalException("Unimplemented GetChildGreaterEqual for ART node");
	}

	// FIXME: can go?
	//! Get the position of the minimum element in the node
	virtual idx_t GetMin() override;

	// FIXME: can go?
	//! Get the next position in the node, or DConstants::INVALID_INDEX if there is no next position. if pos ==
	//! DConstants::INVALID_INDEX, then the first valid position in the node is returned
	virtual idx_t GetNextPos(idx_t pos) override {
		return DConstants::INVALID_INDEX;
	}

	// FIXME: can go?
	//! Get the child at the specified position in the node. pos should be between [0, count). Throws an assertion if
	//! the element is not found
	virtual BaseNode *GetChild(ART &art, idx_t pos) override;

	// FIXME: can go?
	//! Replaces the pointer to a child node
	virtual void ReplaceChildPointer(idx_t pos, BaseNode *node) override;

	//! Insert a new child node at key_byte into the node
	static void InsertChild(BaseNode *&node, uint8_t key_byte, BaseNode *new_child);
	//! Erase child node entry from node
	static void EraseChild(BaseNode *&node, idx_t pos, ART &art);
	//! Get the corresponding node type for the provided size
	static NodeType GetTypeBySize(idx_t size);
	//! Create a new node of the specified type
	static void New(NodeType &type, BaseNode *&node);

	//! Merge r_node into l_node at the specified byte
	static bool MergeAtByte(MergeInfo &info, idx_t depth, idx_t &l_child_pos, idx_t &r_pos, uint8_t &key_byte,
	                        BaseNode *&l_parent, idx_t l_pos);
	//! Merge two ART
	static bool MergeARTs(ART *l_art, ART *r_art);

private:
	//! Serialize internal nodes
	BlockPointer SerializeInternal(ART &art, duckdb::MetaBlockWriter &writer, InternalType &internal_type);
	//! Deserialize internal Nodes
	void DeserializeInternal(duckdb::MetaBlockReader &reader) override;
};

} // namespace duckdb
