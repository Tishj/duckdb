#include "duckdb/execution/index/art/node256.hpp"

#include "duckdb/execution/index/art/node48.hpp"

namespace duckdb {

Node256::Node256() : Node(NodeType::N256) {
}

idx_t Node256::GetChildPos(uint8_t k) {
	if (children[k].Pointer()) {
		return k;
	} else {
		return DConstants::INVALID_INDEX;
	}
}

idx_t Node256::GetChildGreaterEqual(uint8_t k, bool &equal) {
	for (idx_t pos = k; pos < 256; pos++) {
		if (children[pos].Pointer()) {
			if (pos == k) {
				equal = true;
			} else {
				equal = false;
			}
			return pos;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node256::GetMin() {
	for (idx_t i = 0; i < 256; i++) {
		if (children[i].Pointer()) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node256::GetNextPos(idx_t pos) {
	for (pos == DConstants::INVALID_INDEX ? pos = 0 : pos++; pos < 256; pos++) {
		if (children[pos].Pointer()) {
			return pos;
		}
	}
	return Node::GetNextPos(pos);
}

BaseNode *Node256::GetChild(ART &art, idx_t pos) {
	return children[pos].Unswizzle(art);
}

void Node256::ReplaceChildPointer(idx_t pos, BaseNode *node) {
	children[pos] = node;
}

void Node256::InsertChild(BaseNode *&node, uint8_t key_byte, BaseNode *new_child) {
	auto n = (Node256 *)(node);

	n->count++;
	n->children[key_byte] = new_child;
}

void Node256::EraseChild(BaseNode *&node, int pos, ART &art) {
	auto n = (Node256 *)(node);
	n->children[pos].Reset();
	n->count--;
	if (node->Count() <= 36) {
		auto new_node = new Node48();
		new_node->prefix = move(n->prefix);
		for (idx_t i = 0; i < 256; i++) {
			if (n->children[i].Pointer()) {
				new_node->child_index[i] = new_node->count;
				new_node->children[new_node->count] = n->children[i];
				n->children[i] = nullptr;
				new_node->count++;
			}
		}
		delete node;
		node = new_node;
	}
}

bool Node256::Merge(MergeInfo &info, idx_t depth, BaseNode *&l_parent, idx_t l_pos) {

	for (idx_t i = 0; i < 256; i++) {
		if (info.r_node->GetChildPos(i) != DConstants::INVALID_INDEX) {

			auto l_child_pos = info.l_node->GetChildPos(i);
			auto key_byte = (uint8_t)i;
			if (!Node::MergeAtByte(info, depth, l_child_pos, i, key_byte, l_parent, l_pos)) {
				return false;
			}
		}
	}
	return true;
}

idx_t Node256::GetSize() {
	return 256;
}

} // namespace duckdb
