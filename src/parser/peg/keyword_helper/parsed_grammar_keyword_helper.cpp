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
	auto rule_p = grammar.GetRule(rule_name);
	if (!rule_p) {
		throw InvalidInputException("No registered data exists for keyword rule '%s'", rule_name);
	}
	auto &rule = *rule_p;
	if (!rule.recipe.parameters.empty()) {
		throw InvalidInputException("Keyword grammar rule '%s' references parameterized rule '%s'", root_rule_name,
		                            rule_name);
	}

	if (!rule.recipe.root) {
		throw InvalidInputException("Keyword grammar rule '%s' is empty", root_rule_name);
	}
	vector<reference<const PEGNode>> choices;
	if (rule.recipe.root->GetType() == PEGNodeType::CHOICE) {
		for (auto &child : rule.recipe.root->Cast<PEGChoiceNode>().children) {
			choices.push_back(*child);
		}
	} else {
		choices.push_back(*rule.recipe.root);
	}
	for (auto &choice : choices) {
		switch (choice.get().GetType()) {
		case PEGNodeType::LITERAL:
			keyword_map.insert(StringUtil::Lower(choice.get().Cast<PEGLiteralNode>().text));
			break;
		case PEGNodeType::REFERENCE:
			PopulateKeywordMap(grammar, root_rule_name, choice.get().Cast<PEGReferenceNode>().text, keyword_map,
			                   active_rules);
			break;
		default:
			throw InvalidInputException("Keyword grammar rule '%s' must contain only literal or reference alternatives",
			                            root_rule_name);
		}
	}
	active_rules.erase(rule_name);
}

ParsedGrammarKeywordHelper::ParsedGrammarKeywordHelper(const ParsedGrammar &grammar) {
	unordered_map<string, reference<case_insensitive_set_t>> keyword_maps {
	    {"ReservedKeyword", reserved_keyword_map},  {"UnreservedKeyword", unreserved_keyword_map},
	    {"ColumnNameKeyword", colname_keyword_map}, {"TypeFuncKeyword", typefunc_keyword_map},
	    {"TypeNameKeyword", typename_keyword_map},
	};
	for (auto &entry : keyword_maps) {
		case_insensitive_set_t active_rules;
		PopulateKeywordMap(grammar, entry.first, entry.first, entry.second.get(), active_rules);
	}
}

bool ParsedGrammarKeywordHelper::KeywordCategoryType(const string &text, PEGKeywordCategory type) const {
	switch (type) {
	case PEGKeywordCategory::KEYWORD_RESERVED:
		return reserved_keyword_map.count(text) != 0;
	case PEGKeywordCategory::KEYWORD_UNRESERVED:
		return unreserved_keyword_map.count(text) != 0;
	case PEGKeywordCategory::KEYWORD_TYPE_FUNC:
		return typefunc_keyword_map.count(text) != 0;
	case PEGKeywordCategory::KEYWORD_COL_NAME:
		return colname_keyword_map.count(text) != 0;
	case PEGKeywordCategory::KEYWORD_TYPE_NAME:
		return typename_keyword_map.count(text) != 0;
	default:
		return false;
	}
}

bool ParsedGrammarKeywordHelper::IsKeyword(const string &text) const {
	return reserved_keyword_map.count(text) != 0 || unreserved_keyword_map.count(text) != 0 ||
	       colname_keyword_map.count(text) != 0 || typefunc_keyword_map.count(text) != 0;
}

vector<ParserKeyword> ParsedGrammarKeywordHelper::KeywordList() const {
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
