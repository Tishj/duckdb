//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/base_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/base_leaf.hpp"

namespace duckdb {

class RowidLeaf : public BaseNode, public BaseLeaf {
public:
	RowidLeaf() : BaseNode(NodeType::NRowIdLeaf), BaseLeaf() {
	}

public:
	bool IsLeaf() const override {
		return true;
	}
	row_t GetRowId(idx_t index) override {
		if (!index) {
			return rowid;
		}
		//! FIXME: just add an assertion that index is 0?
		return DConstants::INVALID_INDEX;
	}

	void DeserializeInternal(MetaBlockReader &reader) override;

private:
	row_t rowid;
};

} // namespace duckdb
