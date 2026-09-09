//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/keyword_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/parser/simplified_token.hpp"
#include "duckdb/parser/peg/literal_info.hpp"

namespace duckdb {

class GrammarLiteralTable;

enum class SuggestionState : uint8_t;

class PEGKeywordHelper {
public:
	virtual ~PEGKeywordHelper() = default;

public:
	virtual LiteralInfo LookupKeyword(const string &text) const = 0;
	bool IsKeyword(const string &text) const {
		return LookupKeyword(text).IsKeyword();
	}
	//! Opaque flags accepted in this identifier position, computed when creating a matcher.
	virtual uint32_t GetIdentifierMask(SuggestionState type) const = 0;
	virtual KeywordCategory GetKeywordCategory(const string &text) const = 0;
	virtual vector<ParserKeyword> KeywordList() const = 0;
	//! Opt in only when this immutable table agrees with the helper's keyword predicates.
	virtual optional_ptr<const GrammarLiteralTable> GetLiteralTable() const {
		return nullptr;
	}
};

} // namespace duckdb
