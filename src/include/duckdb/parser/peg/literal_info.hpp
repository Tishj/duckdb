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
	LiteralInfo() = default;
	explicit LiteralInfo(uint32_t literal_id, uint32_t flags = 0) : value(literal_id | flags) {
		D_ASSERT(literal_id <= MAX_LITERAL_ID);
		D_ASSERT((flags & MAX_LITERAL_ID) == 0);
	}

	uint32_t LiteralId() const {
		return value & MAX_LITERAL_ID;
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
	uint32_t value = 0;
};

} // namespace duckdb
