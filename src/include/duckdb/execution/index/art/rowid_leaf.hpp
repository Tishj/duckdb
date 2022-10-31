//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/base_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

class RowidLeaf : public BaseNode {
public:
	RowidLeaf(row_t rowid) : BaseNode(NodeType::NRowIdLeaf) {
	}

public:
	idx_t Count() const override {
		return 1;
	}

	const Prefix &GetPrefix() const override {
		return prefix;
	}

	bool IsLeaf() const override {
		return true;
	}

	static string ToString(BaseNode *node) {
		auto leaf = (RowidLeaf *)node;
		return "RowIDLeaf: [" + to_string(leaf->rowid) + "]";
	}

	row_t GetRowId(idx_t index) override {
		if (!index) {
			return rowid;
		}
		return DConstants::INVALID_INDEX;
	}
	//! Serialize the rowid by returning a blockpointer with a special flag set
	BlockPointer SerializeLeaf(duckdb::MetaBlockWriter &writer) override {
		return BlockPointer(rowid);
	}

	static void Merge(BaseNode *&l_node, BaseNode *&r_node) {
		auto l_rowid = l_node->GetRowId(0);
		auto r_rowid = l_node->GetRowId(0);
		if (l_rowid != r_rowid) {
			throw ConstraintException("Data contains duplicates on indexed column(s)");
		}
	}

private:
	Prefix prefix;
	row_t rowid;
};

} // namespace duckdb
