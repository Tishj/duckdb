//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/base_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

namespace duckdb {

enum class NodeType : uint8_t { N4 = 0, N16 = 1, N48 = 2, N256 = 3, NLeaf = 4, NRowIdLeaf = 5 };

//! This class serves as an empty interface for Node-like classes

class ART;
class MetaBlockReader;
class Prefix;

class BaseNode {
public:
	virtual ~BaseNode() {
	}
	//! Node type
	NodeType type;

public:
	// -----------------------------
	// Accessors
	// -----------------------------

	//! Get the total amount of child entries in the node
	virtual idx_t Count() const {
		D_ASSERT(0);
		return DConstants::INVALID_INDEX;
	}
	//! Set the total amount of child entries in the node
	virtual void SetCount(idx_t new_count) {
		(void)new_count;
		D_ASSERT(0);
	}
	//! Get a (constant) reference to the prefix of the node
	virtual const Prefix &GetPrefix() const {
		throw InternalException("BaseNode does not have a prefix");
	}
	//! Get a mutable reference to the prefix of the node
	virtual Prefix &GetMutPrefix() {
		throw InternalException("BaseNode does not have a prefix");
	}
	//! Set the prefix of the node
	virtual void SetPrefix(Prefix &&prefix) {
		throw InternalException("BaseNode does not have a prefix");
	}

	// -----------------------------
	// General Node Methods
	// -----------------------------

	//! Whether this node is a leaf node
	virtual bool IsLeaf() const {
		return false;
	}

	//! Get the position of a child corresponding exactly to the specific byte, returns DConstants::INVALID_INDEX if not
	//! exists
	virtual idx_t GetChildPos(uint8_t k) {
		return DConstants::INVALID_INDEX;
	}

	//! Get the position of the first child that is greater or equal to the specific byte, or DConstants::INVALID_INDEX
	//! if there are no children matching the criteria
	virtual idx_t GetChildGreaterEqual(uint8_t k, bool &equal) {
		throw InternalException("Unimplemented GetChildGreaterEqual for ARTNode");
	}

	//! Get the position of the biggest element in node
	virtual idx_t GetMin() {
		D_ASSERT(0);
		return 0;
	}

	//! Get the next position in the node, or DConstants::INVALID_INDEX if there is no next position. if pos ==
	//! DConstants::INVALID_INDEX, then the first valid position in the node will be returned.
	virtual idx_t GetNextPos(idx_t pos) {
		return DConstants::INVALID_INDEX;
	}

	//! Get the child at the specified position in the node. pos should be between [0, count). Throws an assertion if
	//! the element is not found.
	virtual BaseNode *GetChild(ART &art, idx_t pos) {
		D_ASSERT(0);
		return nullptr;
	}

	//! Replaces the pointer
	virtual void ReplaceChildPointer(idx_t pos, BaseNode *node) {
		D_ASSERT(0);
	}

	// -----------------------------
	// Leaf Node Methods
	// -----------------------------

	virtual row_t GetRowId(idx_t index) {
		D_ASSERT(0);
		return DConstants::INVALID_INDEX;
	}

	virtual BlockPointer SerializeLeaf(duckdb::MetaBlockWriter &writer) {
		throw InternalException("(Leaf) Serialize not implemented for Node");
	}

	// -----------------------------
	// Node Storage
	// -----------------------------

	virtual BlockPointer Serialize(ART &art, duckdb::MetaBlockWriter &writer) {
		throw InternalException("(General) Serialize not implemented for Node");
	}

	//! Deserialize a Node from storage
	virtual void DeserializeInternal(MetaBlockReader &reader) {
		D_ASSERT(0);
	}

	//! Returns the string representation of a node
	string ToString(ART &art);

protected:
	BaseNode(NodeType type) : type(type) {
	}
};

} // namespace duckdb
