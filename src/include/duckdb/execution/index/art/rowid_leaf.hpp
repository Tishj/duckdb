//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/base_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/base_node.hpp"

namespace duckdb {

class RowidLeaf : public BaseNode {
public:
	RowidLeaf(row_t rowid) : BaseNode(NodeType::NRowIdLeaf) {
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
	//! Serialize the rowid by returning a blockpointer with a special flag set
	BlockPointer SerializeLeaf(duckdb::MetaBlockWriter &writer) override {
		return BlockPointer(rowid);
	}

private:
	row_t rowid;
};

} // namespace duckdb
