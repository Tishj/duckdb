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
}

ParsedGrammar &ParsedGrammar::operator=(ParsedGrammar &&other) noexcept {
	if (this != &other) {
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

optional_ptr<ParsedGrammarRule> ParsedGrammar::GetRule(const string &rule_name) {
	auto entry = rules.find(rule_name);
	if (entry == rules.end()) {
		return nullptr;
	}
	return *entry->second;
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

void ParsedGrammar::AddParsedRule(ParsedGrammarRule rule) {
	if (GetRule(rule.name)) {
		throw InvalidInputException("Grammar rule '%s' already exists", rule.name);
	}
	auto name = rule.name;
	rules.emplace(std::move(name), make_uniq<ParsedGrammarRule>(std::move(rule)));
}

void ParsedGrammar::AddRule(const string &rule_definition, grammar_transform_function_t transform) {
	auto rule = ParseSingleRule(rule_definition);
	rule.transform = std::move(transform);
	AddParsedRule(std::move(rule));
}

void ParsedGrammar::InsertChoice(const string &rule_name, const string &choice, grammar_cursor_function_t find_cursor,
                                 bool prepend) {
	auto choice_definition = StringUtil::Format("Choice <- %s", choice);
	auto choice_rule = ParseSingleRule(choice_definition);
	auto &rule = GetMutableRule(rule_name);
	if (!rule.recipe.root || !choice_rule.recipe.root) {
		throw InvalidInputException("Cannot add an empty choice to grammar rule '%s'", rule_name);
	}

	vector<reference<PEGNode>> existing_choices;
	if (rule.recipe.root->GetType() == PEGNodeType::CHOICE) {
		for (auto &child : rule.recipe.root->Cast<PEGChoiceNode>().children) {
			existing_choices.push_back(*child);
		}
	} else {
		existing_choices.push_back(*rule.recipe.root);
	}

	idx_t cursor = prepend ? 0 : existing_choices.size();
	if (find_cursor) {
		bool found = false;
		for (idx_t choice_idx = 0; choice_idx < existing_choices.size(); choice_idx++) {
			if (find_cursor(existing_choices[choice_idx])) {
				cursor = prepend ? choice_idx : choice_idx + 1;
				found = true;
				break;
			}
		}
		if (!found) {
			throw InvalidInputException("Could not find a choice cursor in grammar rule '%s'", rule.name);
		}
	}

	if (rule.recipe.root->GetType() != PEGNodeType::CHOICE) {
		vector<unique_ptr<PEGNode>> children;
		children.push_back(std::move(rule.recipe.root));
		rule.recipe.root = make_uniq<PEGChoiceNode>(std::move(children));
	}
	auto &children = rule.recipe.root->Cast<PEGChoiceNode>().children;
	vector<unique_ptr<PEGNode>> new_choices;
	if (choice_rule.recipe.root->GetType() == PEGNodeType::CHOICE) {
		new_choices = std::move(choice_rule.recipe.root->Cast<PEGChoiceNode>().children);
	} else {
		new_choices.push_back(std::move(choice_rule.recipe.root));
	}
	for (auto &new_choice : new_choices) {
		children.insert(children.begin() + cursor++, std::move(new_choice));
	}
}

void ParsedGrammar::AddChoice(const string &rule_name, const string &choice, grammar_cursor_function_t find_cursor) {
	InsertChoice(rule_name, choice, std::move(find_cursor), false);
}

void ParsedGrammar::PrependChoice(const string &rule_name, const string &choice,
                                  grammar_cursor_function_t find_cursor) {
	InsertChoice(rule_name, choice, std::move(find_cursor), true);
}

void ParsedGrammar::RemoveChoice(const string &rule_name, grammar_cursor_function_t find_cursor) {
	if (!find_cursor) {
		throw InvalidInputException("RemoveChoice requires a choice cursor");
	}
	auto &rule = GetMutableRule(rule_name);
	if (!rule.recipe.root) {
		throw InvalidInputException("Cannot remove a choice from empty grammar rule '%s'", rule_name);
	}

	if (rule.recipe.root->GetType() != PEGNodeType::CHOICE) {
		if (find_cursor(*rule.recipe.root)) {
			throw InvalidInputException("Cannot remove the final choice from grammar rule '%s'", rule_name);
		}
		throw InvalidInputException("Could not find a choice cursor in grammar rule '%s'", rule_name);
	}

	auto &children = rule.recipe.root->Cast<PEGChoiceNode>().children;
	for (idx_t choice_idx = 0; choice_idx < children.size(); choice_idx++) {
		if (!find_cursor(*children[choice_idx])) {
			continue;
		}
		if (children.size() == 1) {
			throw InvalidInputException("Cannot remove the final choice from grammar rule '%s'", rule_name);
		}
		children.erase(children.begin() + choice_idx);
		if (children.size() == 1) {
			auto remaining_choice = std::move(children[0]);
			rule.recipe.root = std::move(remaining_choice);
		}
		return;
	}
	throw InvalidInputException("Could not find a choice cursor in grammar rule '%s'", rule_name);
}

void ParsedGrammar::ReplaceRule(const string &rule_definition, grammar_transform_function_t transform) {
	auto rule = ParseSingleRule(rule_definition);
	auto entry = rules.find(rule.name);
	if (entry == rules.end()) {
		throw InvalidInputException("Grammar rule '%s' does not exist", rule.name);
	}
	rule.transform = std::move(transform);
	entry->second = make_uniq<ParsedGrammarRule>(std::move(rule));
}

void ParsedGrammar::SetTransform(const string &rule_name, grammar_transform_function_t transform) {
	auto &rule = GetMutableRule(rule_name);
	rule.transform = std::move(transform);
}

} // namespace duckdb
