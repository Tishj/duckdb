//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/base_leaf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/execution/index/art/base_node.hpp"

namespace duckdb {

//! This interface adds methods that are only relevant to Leaf nodes

class BaseLeaf {
public:
	BaseLeaf() = default;

public:
	virtual row_t GetRowId(idx_t index) {
		D_ASSERT(0);
		return DConstants::INVALID_INDEX;
	}

	virtual BlockPointer Serialize(duckdb::MetaBlockWriter &writer) {
		throw InternalException("Serialize not implemented for Leaf Node");
	}
};

} // namespace duckdb
