//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/keyword_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/simplified_token.hpp"

namespace duckdb {
struct ParserCache;

struct KeywordCategoryName {
	static constexpr const char *RESERVED = "reserved";
	static constexpr const char *UNRESERVED = "unreserved";
	static constexpr const char *TYPE_FUNC = "type_function";
	static constexpr const char *COL_NAME = "column_name";
	static constexpr const char *TYPE_NAME = "type_name";
};

class PEGKeywordHelper {
public:
	virtual ~PEGKeywordHelper() = default;

public:
	virtual bool KeywordCategoryType(const string &text, const string &category) const = 0;
	virtual bool IsKeyword(const string &text) const = 0;
	virtual vector<ParserKeyword> KeywordList() const = 0;
};

} // namespace duckdb
