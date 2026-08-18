#include "duckdb/parser/peg/keyword_helper/parsed_grammar_keyword_helper.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"

namespace duckdb {

static void PopulateKeywordMap(const ParsedGrammar &grammar, const string &root_rule_name, const string &rule_name,
                               case_insensitive_set_t &keyword_map, case_insensitive_set_t &active_rules) {
	if (!active_rules.insert(rule_name).second) {
		throw InvalidInputException("Keyword grammar rule '%s' contains a recursive reference to rule '%s'",
		                            root_rule_name, rule_name);
	}
	auto &rule = grammar.GetRule(rule_name);
	if (!rule.recipe.parameters.empty()) {
		throw InvalidInputException("Keyword grammar rule '%s' references parameterized rule '%s'", root_rule_name,
		                            rule_name);
	}

	bool expect_keyword = true;
	for (auto &token : rule.recipe.tokens) {
		if (expect_keyword) {
			switch (token.type) {
			case PEGTokenType::LITERAL:
				keyword_map.insert(StringUtil::Lower(token.text.GetString()));
				break;
			case PEGTokenType::REFERENCE:
				PopulateKeywordMap(grammar, root_rule_name, token.text.GetString(), keyword_map, active_rules);
				break;
			default:
				throw InvalidInputException("Keyword grammar rule '%s' contains unsupported token '%s' in rule '%s'",
				                            root_rule_name, token.text.GetString(), rule_name);
			}
		} else if (token.type != PEGTokenType::OPERATOR || token.text.GetString() != "/") {
			throw InvalidInputException("Keyword grammar rule '%s' must contain only alternatives", root_rule_name);
		}
		expect_keyword = !expect_keyword;
	}
	if (expect_keyword) {
		throw InvalidInputException("Keyword grammar rule '%s' ends with an incomplete alternative", root_rule_name);
	}
	active_rules.erase(rule_name);
}

ParsedGrammarKeywordHelper::ParsedGrammarKeywordHelper(const ParsedGrammar &grammar) {
	unordered_map<string, string> category_rules {
	    {KeywordCategoryName::RESERVED, "ReservedKeyword"},   {KeywordCategoryName::UNRESERVED, "UnreservedKeyword"},
	    {KeywordCategoryName::COL_NAME, "ColumnNameKeyword"}, {KeywordCategoryName::TYPE_FUNC, "TypeFuncKeyword"},
	    {KeywordCategoryName::TYPE_NAME, "TypeNameKeyword"},
	};
	for (auto &entry : category_rules) {
		case_insensitive_set_t active_rules;
		auto &keyword_map = keyword_maps[entry.first];
		PopulateKeywordMap(grammar, entry.second, entry.second, keyword_map, active_rules);
	}
}

bool ParsedGrammarKeywordHelper::KeywordCategoryType(const string &text, const string &category) const {
	auto entry = keyword_maps.find(category);
	if (entry == keyword_maps.end()) {
		return false;
	}
	return entry->second.count(text) != 0;
}

bool ParsedGrammarKeywordHelper::IsKeyword(const string &text) const {
	for (auto &entry : keyword_maps) {
		if (entry.second.count(text) != 0) {
			return true;
		}
	}
	return false;
}

vector<ParserKeyword> ParsedGrammarKeywordHelper::KeywordList() const {
	vector<ParserKeyword> result;
	const string categories[] {KeywordCategoryName::RESERVED, KeywordCategoryName::UNRESERVED,
	                           KeywordCategoryName::TYPE_FUNC, KeywordCategoryName::COL_NAME};
	for (auto &category : categories) {
		for (auto &kw : keyword_maps.at(category)) {
			result.push_back({kw, category});
		}
	}
	return result;
}

} // namespace duckdb
