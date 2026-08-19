#include "duckdb/parser/peg/parsed_grammar.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#ifdef PEG_PARSER_SOURCE_FILE
#include <fstream>
#else
#include "duckdb/parser/peg/inlined_grammar.hpp"
#endif

namespace duckdb {

ParsedGrammar::ParsedGrammar(ParsedGrammar &&other) noexcept : rules(std::move(other.rules)) {
	string_heap.Move(other.string_heap);
}

ParsedGrammar &ParsedGrammar::operator=(ParsedGrammar &&other) noexcept {
	if (this != &other) {
		rules.clear();
		string_heap.Destroy();
		string_heap.Move(other.string_heap);
		rules = std::move(other.rules);
	}
	return *this;
}

ParsedGrammar ParsedGrammar::Parse(const string &grammar) {
	PEGParser parser;
	parser.ParseRules(grammar.c_str());
	ParsedGrammar result;
	for (auto &entry : parser.rules) {
		result.AddParsedRule(ParsedGrammarRule(entry.first, std::move(entry.second)));
	}
	return result;
}

ParsedGrammar ParsedGrammar::CreateDefault() {
#ifdef PEG_PARSER_SOURCE_FILE
	std::ifstream t(PEG_PARSER_SOURCE_FILE);
	std::stringstream buffer;
	buffer << t.rdbuf();
	auto grammar_string = buffer.str();

	const char *grammar = grammar_string.c_str();
#else
	const char *grammar = const_char_ptr_cast(INLINED_PEG_GRAMMAR);
#endif
	auto result = Parse(grammar);
	if (!result.GetRule("EndOfInput")) {
		result.AddParsedRule(ParsedGrammarRule("EndOfInput", PEGRule()));
	}
	PEGTransformerFactory::RegisterDefaultTransforms(result);
	return result;
}

optional_ptr<const ParsedGrammarRule> ParsedGrammar::GetRule(const string &rule_name) const {
	auto entry = rules.find(rule_name);
	if (entry == rules.end()) {
		return nullptr;
	}
	return *entry->second;
}
ParsedGrammarRule &ParsedGrammar::GetMutableRule(const string &rule_name) {
	auto entry = rules.find(rule_name);
	if (entry == rules.end()) {
		throw InvalidInputException("Grammar rule '%s' does not exist", rule_name);
	}
	return *entry->second;
}

ParsedGrammarRule ParsedGrammar::ParseSingleRule(const string &rule_definition) {
	PEGParser parser;
	parser.ParseRules(rule_definition.c_str());
	if (parser.rules.size() != 1) {
		throw InvalidInputException("Expected exactly one PEG rule definition");
	}
	auto &entry = *parser.rules.begin();
	return ParsedGrammarRule(entry.first, std::move(entry.second));
}

void ParsedGrammar::RegisterStrings(PEGRule &rule) {
	string_map_t<idx_t> parameters;
	for (auto &entry : rule.parameters) {
		parameters.emplace(string_heap.AddString(entry.first), entry.second);
	}
	rule.parameters = std::move(parameters);
	for (auto &token : rule.tokens) {
		token.text = string_heap.AddString(token.text);
	}
}

void ParsedGrammar::AddParsedRule(ParsedGrammarRule rule) {
	if (GetRule(rule.name)) {
		throw InvalidInputException("Grammar rule '%s' already exists", rule.name);
	}
	RegisterStrings(rule.recipe);
	auto name = rule.name;
	rules.emplace(std::move(name), make_uniq<ParsedGrammarRule>(std::move(rule)));
}

void ParsedGrammar::AddRule(const string &rule_definition, grammar_transform_function_t transform) {
	auto rule = ParseSingleRule(rule_definition);
	rule.transform = std::move(transform);
	AddParsedRule(std::move(rule));
}

static idx_t FindChoiceCursor(const ParsedGrammarRule &rule, const grammar_cursor_function_t &find_cursor,
                              bool prepend) {
	if (!find_cursor) {
		return prepend ? 0 : rule.recipe.tokens.size();
	}

	vector<bool> choice_separator(rule.recipe.tokens.size(), false);
	idx_t depth = 0;
	for (idx_t token_idx = 0; token_idx < rule.recipe.tokens.size(); token_idx++) {
		auto &token = rule.recipe.tokens[token_idx];
		if (token.type == PEGTokenType::FUNCTION_CALL ||
		    (token.type == PEGTokenType::OPERATOR && token.text.GetString() == "(")) {
			depth++;
		} else if (token.type == PEGTokenType::OPERATOR && token.text.GetString() == ")") {
			D_ASSERT(depth > 0);
			depth--;
		} else if (depth == 0 && token.type == PEGTokenType::OPERATOR && token.text.GetString() == "/") {
			choice_separator[token_idx] = true;
		}
	}

	for (idx_t token_idx = 0; token_idx < rule.recipe.tokens.size(); token_idx++) {
		auto &token = rule.recipe.tokens[token_idx];
		if (!find_cursor(token)) {
			continue;
		}
		if (prepend && token_idx != 0 && !choice_separator[token_idx - 1]) {
			throw InvalidInputException("PrependChoice cursor for rule '%s' must select the first token of a choice",
			                            rule.name);
		}
		if (!prepend && token_idx + 1 != rule.recipe.tokens.size() && !choice_separator[token_idx + 1]) {
			throw InvalidInputException("AddChoice cursor for rule '%s' must select the final token of a choice",
			                            rule.name);
		}
		return prepend ? token_idx : token_idx + 1;
	}
	throw InvalidInputException("Could not find a choice cursor in grammar rule '%s'", rule.name);
}

void ParsedGrammar::InsertChoice(const string &rule_name, const string &choice, grammar_cursor_function_t find_cursor,
                                 bool prepend) {
	auto choice_definition = StringUtil::Format("Choice <- %s", choice);
	auto choice_rule = ParseSingleRule(choice_definition);
	auto &rule = GetMutableRule(rule_name);
	RegisterStrings(choice_rule.recipe);
	auto cursor = FindChoiceCursor(rule, find_cursor, prepend);

	vector<PEGToken> tokens;
	tokens.reserve(rule.recipe.tokens.size() + choice_rule.recipe.tokens.size() + 1);
	for (idx_t token_idx = 0; token_idx < cursor; token_idx++) {
		tokens.push_back(rule.recipe.tokens[token_idx]);
	}
	if (!prepend && !rule.recipe.tokens.empty()) {
		tokens.push_back({PEGTokenType::OPERATOR, string_heap.AddString("/")});
	}
	for (auto &token : choice_rule.recipe.tokens) {
		tokens.push_back(std::move(token));
	}
	if (prepend && !rule.recipe.tokens.empty()) {
		tokens.push_back({PEGTokenType::OPERATOR, string_heap.AddString("/")});
	}
	for (idx_t token_idx = cursor; token_idx < rule.recipe.tokens.size(); token_idx++) {
		tokens.push_back(rule.recipe.tokens[token_idx]);
	}
	rule.recipe.tokens = std::move(tokens);
}

void ParsedGrammar::AddChoice(const string &rule_name, const string &choice, grammar_cursor_function_t find_cursor) {
	InsertChoice(rule_name, choice, std::move(find_cursor), false);
}

void ParsedGrammar::PrependChoice(const string &rule_name, const string &choice,
                                  grammar_cursor_function_t find_cursor) {
	InsertChoice(rule_name, choice, std::move(find_cursor), true);
}

void ParsedGrammar::ReplaceRule(const string &rule_definition, grammar_transform_function_t transform) {
	auto rule = ParseSingleRule(rule_definition);
	auto entry = rules.find(rule.name);
	if (entry == rules.end()) {
		throw InvalidInputException("Grammar rule '%s' does not exist", rule.name);
	}
	RegisterStrings(rule.recipe);
	rule.transform = std::move(transform);
	entry->second = make_uniq<ParsedGrammarRule>(std::move(rule));
}

void ParsedGrammar::SetTransform(const string &rule_name, grammar_transform_function_t transform) {
	auto &rule = GetMutableRule(rule_name);
	rule.transform = std::move(transform);
}

} // namespace duckdb
