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
	Leaf(Key &value, unsigned depth, row_t row_id);

	Leaf(unique_ptr<row_t[]> row_ids, idx_t num_elements, Prefix &prefix);
	idx_t capacity;

public:
	row_t GetRowId(idx_t index) override {
		return row_ids[index];
	}
	bool IsLeaf() const override {
		return true;
	}

	void Insert(row_t row_id);
	void Remove(row_t row_id);

	BlockPointer SerializeLeaf(duckdb::MetaBlockWriter &writer) override;

	static Leaf *Deserialize(duckdb::MetaBlockReader &reader);

private:
	unique_ptr<row_t[]> row_ids;
};

} // namespace duckdb
