//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/art/art_key.hpp"

class MetaBlockWriter;
class MetaBlockReader;

namespace duckdb {
class Prefix {
public:
	Prefix();
	// Prefix created from key starting on `depth`.
	Prefix(Key &key, uint32_t depth, uint32_t size);

	// Returns the Prefix's size
	uint32_t Size() const;

	// Subscript operator
	uint8_t &operator[](idx_t idx);
	//! Const subscript operator
	const uint8_t &operator[](idx_t idx) const;

	// Assign operator
	Prefix &operator=(const Prefix &src);

	// Move operator
	Prefix &operator=(Prefix &&other) noexcept;

	// Concatenate Prefix with a key and another prefix
	// Used when deleting a Node.
	// other.prefix + key + this->Prefix
	void Concatenate(uint8_t key, Prefix &other);
	// Reduces the prefix in n elements, and returns what would be the first one as a key
	uint8_t Reduce(uint32_t n);
	// Serializes Prefix
	void Serialize(duckdb::MetaBlockWriter &writer);
	// Deserializes Prefix
	void Deserialize(duckdb::MetaBlockReader &reader);

	// Compare the key with the prefix of the node, return the position where it mismatches
	uint32_t KeyMismatchPosition(Key &key, uint64_t depth) const;

private:
	unique_ptr<uint8_t[]> prefix;
	uint32_t size;
};

} // namespace duckdb
