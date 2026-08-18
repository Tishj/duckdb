#pragma once

#include "duckdb/parser/peg/keyword_helper.hpp"

namespace duckdb {

class ParsedGrammar;

class ParsedGrammarKeywordHelper : public PEGKeywordHelper {
public:
	explicit ParsedGrammarKeywordHelper(const ParsedGrammar &grammar);

public:
	bool KeywordCategoryType(const string &text, const string &category) const override;
	bool IsKeyword(const string &text) const override;
	vector<ParserKeyword> KeywordList() const override;

private:
	unordered_map<string, case_insensitive_set_t> keyword_maps;
};

} // namespace duckdb
