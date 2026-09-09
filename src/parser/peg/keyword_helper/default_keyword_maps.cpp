#include "duckdb/parser/peg/keyword_helper/default_keyword_maps.hpp"
#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

static constexpr uint32_t KEYWORD_UNRESERVED = uint32_t(1) << 24;
static constexpr uint32_t KEYWORD_RESERVED = uint32_t(1) << 25;
static constexpr uint32_t KEYWORD_TYPE_FUNC = uint32_t(1) << 26;
static constexpr uint32_t KEYWORD_COL_NAME = uint32_t(1) << 27;
static constexpr uint32_t KEYWORD_TYPE_NAME = uint32_t(1) << 28;

LiteralInfo DefaultKeywordMaps::LookupKeyword(const string &text, uint32_t literal_id) const {
	uint32_t flags = 0;
	if (unreserved_keyword_map.count(text)) {
		flags |= KEYWORD_UNRESERVED;
	}
	if (reserved_keyword_map.count(text)) {
		flags |= KEYWORD_RESERVED;
	}
	if (typefunc_keyword_map.count(text)) {
		flags |= KEYWORD_TYPE_FUNC;
	}
	if (colname_keyword_map.count(text)) {
		flags |= KEYWORD_COL_NAME;
	}
	if (typename_keyword_map.count(text)) {
		flags |= KEYWORD_TYPE_NAME;
	}
	return LiteralInfo(literal_id, flags);
}

uint32_t DefaultKeywordMaps::GetIdentifierMask(SuggestionState type) {
	switch (type) {
	case SuggestionState::SUGGEST_TYPE_NAME:
		return KEYWORD_UNRESERVED | KEYWORD_TYPE_NAME;
	case SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME:
	case SuggestionState::SUGGEST_TABLE_FUNCTION_NAME:
		return KEYWORD_UNRESERVED | KEYWORD_TYPE_FUNC;
	default:
		return KEYWORD_UNRESERVED | KEYWORD_COL_NAME;
	}
}

KeywordCategory DefaultKeywordMaps::GetKeywordCategory(LiteralInfo info) {
	if (info.HasAnyFlags(KEYWORD_RESERVED)) {
		return KeywordCategory::KEYWORD_RESERVED;
	}
	if (info.HasAnyFlags(KEYWORD_UNRESERVED)) {
		return KeywordCategory::KEYWORD_UNRESERVED;
	}
	if (info.HasAnyFlags(KEYWORD_TYPE_FUNC)) {
		return KeywordCategory::KEYWORD_TYPE_FUNC;
	}
	if (info.HasAnyFlags(KEYWORD_COL_NAME)) {
		return KeywordCategory::KEYWORD_COL_NAME;
	}
	return KeywordCategory::KEYWORD_NONE;
}

vector<ParserKeyword> DefaultKeywordMaps::ToList() const {
	vector<ParserKeyword> result;
	for (auto &kw : reserved_keyword_map) {
		result.push_back({kw, KeywordCategory::KEYWORD_RESERVED});
	}
	for (auto &kw : unreserved_keyword_map) {
		result.push_back({kw, KeywordCategory::KEYWORD_UNRESERVED});
	}
	for (auto &kw : typefunc_keyword_map) {
		result.push_back({kw, KeywordCategory::KEYWORD_TYPE_FUNC});
	}
	for (auto &kw : colname_keyword_map) {
		result.push_back({kw, KeywordCategory::KEYWORD_COL_NAME});
	}
	return result;
}

} // namespace duckdb
