//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/literal_info.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include <cstdint>

namespace duckdb {

//! A grammar-local literal ID and opaque keyword properties.
class LiteralInfo {
public:
	static constexpr uint32_t MAX_LITERAL_ID = 0x00FFFFFF;

public:
	//! Zero denotes an unknown literal with no keyword flags, such as a non-keyword identifier.
	LiteralInfo() : value(0) {
	}
	explicit LiteralInfo(uint32_t literal_id, uint32_t flags = 0) : value(literal_id | flags) {
		D_ASSERT(literal_id <= MAX_LITERAL_ID);
		D_ASSERT((flags & MAX_LITERAL_ID) == 0);
	}

	//! Zero means no grammar-local ID has been assigned; keyword flags may still be present.
	uint32_t LiteralId() const {
		return value & MAX_LITERAL_ID;
	}

	LiteralInfo WithLiteralId(uint32_t literal_id) const {
		return LiteralInfo(literal_id, value & ~MAX_LITERAL_ID);
	}

	bool IsKeyword() const {
		return (value & ~MAX_LITERAL_ID) != 0;
	}

	bool HasAnyFlags(uint32_t mask) const {
		return (value & mask & ~MAX_LITERAL_ID) != 0;
	}

	bool operator==(const LiteralInfo &other) const {
		return value == other.value;
	}

private:
	//! The zero sentinel keeps missing lookups compact and lets flag checks run without an absence branch.
	uint32_t value;
};

} // namespace duckdb
