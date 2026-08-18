#include "duckdb/parser/peg/keyword_helper/duckdb_keyword_helper.hpp"

namespace duckdb {

DuckDBKeywordHelper::DuckDBKeywordHelper() : initialized(false) {
	InitializeKeywordMaps();
}

const DuckDBKeywordHelper &DuckDBKeywordHelper::Instance() {
	static DuckDBKeywordHelper instance;
	return instance;
}

bool DuckDBKeywordHelper::KeywordCategoryType(const string &text, const string &category) const {
	if (category == KeywordCategoryName::RESERVED) {
		auto it = reserved_keyword_map.find(text);
		return it != reserved_keyword_map.end();
	}
	if (category == KeywordCategoryName::UNRESERVED) {
		auto it = unreserved_keyword_map.find(text);
		return it != unreserved_keyword_map.end();
	}
	if (category == KeywordCategoryName::TYPE_FUNC) {
		auto it = typefunc_keyword_map.find(text);
		return it != typefunc_keyword_map.end();
	}
	if (category == KeywordCategoryName::COL_NAME) {
		auto it = colname_keyword_map.find(text);
		return it != colname_keyword_map.end();
	}
	if (category == KeywordCategoryName::TYPE_NAME) {
		auto it = typename_keyword_map.find(text);
		return it != typename_keyword_map.end();
	}
	return false;
}

bool DuckDBKeywordHelper::IsKeyword(const string &text) const {
	if (reserved_keyword_map.count(text) != 0 || unreserved_keyword_map.count(text) != 0 ||
	    colname_keyword_map.count(text) != 0 || typefunc_keyword_map.count(text) != 0) {
		return true;
	}
	return false;
};

vector<ParserKeyword> DuckDBKeywordHelper::KeywordList() const {
	vector<ParserKeyword> result;
	for (auto &kw : reserved_keyword_map) {
		result.push_back({kw, KeywordCategoryName::RESERVED});
	}
	for (auto &kw : unreserved_keyword_map) {
		result.push_back({kw, KeywordCategoryName::UNRESERVED});
	}
	for (auto &kw : typefunc_keyword_map) {
		result.push_back({kw, KeywordCategoryName::TYPE_FUNC});
	}
	for (auto &kw : colname_keyword_map) {
		result.push_back({kw, KeywordCategoryName::COL_NAME});
	}
	return result;
}

} // namespace duckdb
