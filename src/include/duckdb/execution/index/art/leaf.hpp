//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/leaf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

namespace duckdb {

class Leaf : public Node {
public:
	~Leaf() override {
	}

	Leaf(Key &value, uint32_t depth, row_t row_id);
	Leaf(Key &value, uint32_t depth, unique_ptr<row_t[]> row_ids, idx_t num_elements);
	Leaf(unique_ptr<row_t[]> row_ids, idx_t num_elements, Prefix &prefix);

	idx_t capacity;

public:
	row_t GetRowId(idx_t index) override {
		return row_ids[index];
	}
	bool IsLeaf() const override {
		return true;
	}

public:
	//! Insert a row_id into a leaf
	void Insert(row_t row_id);
	//! Remove a row_id from a leaf
	void Remove(row_t row_id);

	//! Returns the string representation of a leaf
	static string ToString(BaseNode *node);
	//! Merge two NLeaf nodes
	static void Merge(BaseNode *&l_node, BaseNode *&r_node);

	//! Serialize a leaf
	BlockPointer SerializeLeaf(duckdb::MetaBlockWriter &writer) override;
	// Deserialize a leaf
	static Leaf *Deserialize(duckdb::MetaBlockReader &reader);

private:
	unique_ptr<row_t[]> row_ids;
};

} // namespace duckdb
