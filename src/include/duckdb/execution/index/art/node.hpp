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

public:
	explicit Node(NodeType type);

	virtual ~Node() {
	}
	//! number of non-null children
	uint16_t count;
	//! compressed path (prefix)
	Prefix prefix;

	//! Serialize this Node
	BlockPointer Serialize(ART &art, duckdb::MetaBlockWriter &writer);

	static BaseNode *Deserialize(ART &art, idx_t block_id, idx_t offset);

	//! Insert leaf into inner node
	static void InsertLeaf(BaseNode *&node, uint8_t key, BaseNode *new_node);
	//! Erase entry from node
	static void Erase(BaseNode *&node, idx_t pos, ART &art);

private:
	//! Serialize Internal Nodes
	BlockPointer SerializeInternal(ART &art, duckdb::MetaBlockWriter &writer, InternalType &internal_type);
	//! Deserialize Internal Nodes
	void DeserializeInternal(duckdb::MetaBlockReader &reader) override;
};

} // namespace duckdb
