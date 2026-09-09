//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/grammar_literal_table.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/peg/keyword_helper.hpp"

namespace duckdb {

class ParsedGrammar;
class DefaultKeywordMaps;

//! Immutable after construction, including literals only present in keyword-category rules.
class GrammarLiteralTable {
public:
	DUCKDB_API GrammarLiteralTable(const ParsedGrammar &grammar, const DefaultKeywordMaps &keyword_maps);
	GrammarLiteralTable(const GrammarLiteralTable &) = delete;
	GrammarLiteralTable &operator=(const GrammarLiteralTable &) = delete;

	uint64_t CacheId() const {
		return cache_id;
	}

	LiteralInfo Lookup(const string &text) const {
		auto entry = literals.find(text);
		return entry == literals.end() ? LiteralInfo() : entry->second;
	}

private:
	void RegisterCategory(const case_insensitive_set_t &words);
	void Register(const string &text);

private:
	const uint64_t cache_id;
	case_insensitive_map_t<LiteralInfo> literals;
};

} // namespace duckdb
