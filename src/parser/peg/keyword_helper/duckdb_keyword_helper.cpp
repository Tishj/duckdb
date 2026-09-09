#include "duckdb/parser/peg/keyword_helper/duckdb_keyword_helper.hpp"

namespace duckdb {

DuckDBKeywordHelper::DuckDBKeywordHelper() : initialized(false) {
	InitializeKeywordMaps();
}

const DuckDBKeywordHelper &DuckDBKeywordHelper::Instance() {
	static DuckDBKeywordHelper instance;
	return instance;
}

LiteralInfo DuckDBKeywordHelper::LookupKeyword(const string &text) const {
	return keyword_maps.LookupKeyword(text);
}

uint32_t DuckDBKeywordHelper::GetIdentifierMask(SuggestionState type) const {
	return DefaultKeywordMaps::GetIdentifierMask(type);
}

KeywordCategory DuckDBKeywordHelper::GetKeywordCategory(const string &text) const {
	return DefaultKeywordMaps::GetKeywordCategory(LookupKeyword(text));
}

vector<ParserKeyword> DuckDBKeywordHelper::KeywordList() const {
	return keyword_maps.ToList();
}

} // namespace duckdb
