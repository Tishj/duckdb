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

namespace duckdb {

enum class NodeType : uint8_t { N4 = 0, N16 = 1, N48 = 2, N256 = 3, NLeaf = 4, NRowIdLeaf = 5 };

//! This class serves as an empty interface for Node-like classes

class ART;
class MetaBlockReader;

class BaseNode {
public:
	virtual ~BaseNode() {
	}
	//! node type
	NodeType type;

public:
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

	//! Deserialize a Node from storage
	virtual void DeserializeInternal(MetaBlockReader &reader) {
		D_ASSERT(0);
	}

protected:
	BaseNode(NodeType type) : type(type) {
	}
};

} // namespace duckdb
