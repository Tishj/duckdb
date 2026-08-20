#include "duckdb/parser/peg/matcher_factory.hpp"
#include "duckdb/parser/peg/peg_parser.hpp"
#include "duckdb/parser/peg/matcher/list.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"

namespace duckdb {

Matcher &MatcherFactory::CreateMatcher(const PEGExpression &expression, const string_map_t<idx_t> &parameter_map,
                                       vector<reference<Matcher>> &parameters) {
	switch (expression.kind) {
	case PEGExpression::Kind::LITERAL:
		return Keyword(expression.text.GetString());
	case PEGExpression::Kind::REFERENCE: {
		auto parameter = parameter_map.find(expression.text);
		if (parameter != parameter_map.end()) {
			return parameters[parameter->second].get();
		}
		return CreateMatcher(expression.text);
	}
	case PEGExpression::Kind::FUNCTION_CALL: {
		if (expression.children.size() != 1) {
			throw InternalException("Function call '%s' expected a single argument", expression.text.GetString());
		}
		vector<reference<Matcher>> function_parameters;
		function_parameters.push_back(CreateMatcher(expression.children[0], parameter_map, parameters));
		return CreateMatcher(expression.text, function_parameters);
	}
	case PEGExpression::Kind::SEQUENCE: {
		vector<reference<Matcher>> children;
		for (auto &child : expression.children) {
			children.push_back(CreateMatcher(child, parameter_map, parameters));
		}
		return List(std::move(children));
	}
	case PEGExpression::Kind::CHOICE: {
		vector<reference<Matcher>> children;
		for (auto &child : expression.children) {
			children.push_back(CreateMatcher(child, parameter_map, parameters));
		}
		return Choice(std::move(children));
	}
	case PEGExpression::Kind::OPTIONAL:
	case PEGExpression::Kind::REPEAT:
	case PEGExpression::Kind::OPTIONAL_REPEAT: {
		if (expression.children.size() != 1) {
			throw InternalException("PEG postfix expression expected a single child");
		}
		auto &child = CreateMatcher(expression.children[0], parameter_map, parameters);
		if (expression.kind == PEGExpression::Kind::OPTIONAL) {
			return Optional(child);
		}
		auto &repeat = Repeat(child);
		if (expression.kind == PEGExpression::Kind::OPTIONAL_REPEAT) {
			return Optional(repeat);
		}
		return repeat;
	}
	case PEGExpression::Kind::REGEX:
		throw InternalException("REGEX operator not supported in PEG grammar");
	case PEGExpression::Kind::END_OF_INPUT:
		throw InternalException("End-of-input expression must have a matcher override");
	default:
		throw InternalException("Unrecognized PEG expression kind");
	}
}

Matcher &MatcherFactory::CreateMatcher(string_t rule_name, vector<reference<Matcher>> &parameters) {
	bool is_function_call = !parameters.empty();
	if (!is_function_call) {
		// check if the matcher has already been created first
		auto matcher_entry = matchers.find(rule_name);
		if (matcher_entry != matchers.end()) {
			// return the created matcher
			return matcher_entry->second.get();
		}
	}
	// look up the rule
	auto entry = grammar.rules.find(rule_name.GetString());
	if (entry == grammar.rules.end()) {
		throw InternalException("Failed to create matcher for rule %s - rule is missing", rule_name.GetString());
	}
	// create a matcher and cache it
	// since matchers can be recursive we need to cache it prior to recursively constructing the other rules
	auto &matcher = List();
	if (!is_function_call) {
		matchers.insert(make_pair(string_t(entry->second->name), reference<Matcher>(matcher)));
	}

	// fill the matcher from the given set of rules
	auto &rule = entry->second->recipe;
	if (rule.parameters.size() > 1) {
		throw InternalException("Only functions with a single parameter are supported");
	}
	if (parameters.size() != rule.parameters.size()) {
		throw InternalException("Parameter count mismatch (rule %s expected %d parameters but got %d)",
		                        rule_name.GetString(), rule.parameters.size(), parameters.size());
	}
	auto &expression_matcher = CreateMatcher(rule.expression, rule.parameters, parameters);
	if (rule.expression.kind == PEGExpression::Kind::SEQUENCE) {
		matcher.matchers = std::move(expression_matcher.Cast<ListMatcher>().matchers);
	} else {
		matcher.matchers.push_back(expression_matcher);
	}

	auto rule_name_str = rule_name.GetString();
	auto rule_p = compiled.GetRule(rule_name_str);
	if (!rule_p) {
		throw InvalidInputException("Failed to compile rule '%s', no registered data exists for it", rule_name_str);
	}
	auto &compiled_rule = *rule_p;

	matcher.SetRule(compiled_rule);
	if (packrat_memoized_rules.count(rule_name)) {
		matcher.SetPackratMemoized();
	}
	if (no_suggestion_rules.count(rule_name)) {
		matcher.Cast<ListMatcher>().suppress_suggestions = true;
	}
	return matcher;
}

void MatcherFactory::AddKeywordOverride(const char *name, KeywordInfo info) {
	keyword_overrides.insert(make_pair(name, info));
}

void MatcherFactory::AddRuleOverride(const char *name, Matcher &matcher) {
	if (packrat_memoized_rules.count(name)) {
		matcher.SetPackratMemoized();
	}
	if (grammar.GetRule(name)) {
		auto rule_p = compiled.GetRule(name);
		if (!rule_p) {
			throw InvalidInputException("No registered data exists for rule '%s', failed to set RuleOverride", name);
		}
		auto &rule = *rule_p;
		matcher.SetRule(rule);
	}
	matchers.insert(make_pair(name, reference<Matcher>(matcher)));
}

void MatcherFactory::AddPackratMemoizedRule(const char *name) {
	packrat_memoized_rules.insert(name);
}

void MatcherFactory::SuppressSuggestions(const char *name) {
	no_suggestion_rules.insert(name);
}

MatcherFactory::MatcherFactory(MatcherAllocator &allocator, const ParsedGrammar &grammar_p, CompiledGrammar &compiled_p)
    : allocator(allocator), grammar(grammar_p), compiled(compiled_p) {
}

Matcher &MatcherFactory::CreateRootMatcher(const string &root_rule) {
	// keyword overrides
	AddKeywordOverride("TABLE", KeywordInfo(1, ' '));
	AddKeywordOverride(".", KeywordInfo(0, '\0'));
	AddKeywordOverride("(", KeywordInfo(0, '\0'));
	// packrat memoized rules
	//===--------------------------------------------------------------------===//
	// START GENERATED PACKRAT MEMOIZED RULES
	//===--------------------------------------------------------------------===//
	AddPackratMemoizedRule("Expression");
	AddPackratMemoizedRule("LambdaArrowExpression");
	AddPackratMemoizedRule("LogicalOrExpression");
	AddPackratMemoizedRule("LogicalAndExpression");
	AddPackratMemoizedRule("LogicalNotExpression");
	AddPackratMemoizedRule("IsExpression");
	AddPackratMemoizedRule("ComparisonExpression");
	AddPackratMemoizedRule("BitwiseExpression");
	AddPackratMemoizedRule("AdditiveExpression");
	AddPackratMemoizedRule("MultiplicativeExpression");
	AddPackratMemoizedRule("ExponentiationExpression");
	AddPackratMemoizedRule("PrefixExpression");
	AddPackratMemoizedRule("CollateExpression");
	AddPackratMemoizedRule("AtTimeZoneExpression");
	AddPackratMemoizedRule("SingleExpression");
	AddPackratMemoizedRule("BaseExpression");
	AddPackratMemoizedRule("ParensExpression");
	AddPackratMemoizedRule("ParenthesisExpression");
	AddPackratMemoizedRule("Identifier");
	AddPackratMemoizedRule("ColId");
	AddPackratMemoizedRule("ColumnReference");
	AddPackratMemoizedRule("FunctionExpression");
	//===--------------------------------------------------------------------===//
	// END GENERATED PACKRAT MEMOIZED RULES
	//===--------------------------------------------------------------------===//

	// rule overrides
	//===--------------------------------------------------------------------===//
	// START GENERATED RULE OVERRIDES
	//===--------------------------------------------------------------------===//
	AddRuleOverride("Identifier", allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE,
	                                                                              compiled.GetKeywordHelper())));
	AddRuleOverride("ReservedIdentifier", allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(
	                                          SuggestionState::SUGGEST_VARIABLE, compiled.GetKeywordHelper())));
	AddRuleOverride("CatalogName", allocator.Allocate(make_uniq<IdentifierMatcher>(
	                                   SuggestionState::SUGGEST_CATALOG_NAME, compiled.GetKeywordHelper())));
	AddRuleOverride("SchemaName", allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_SCHEMA_NAME,
	                                                                              compiled.GetKeywordHelper())));
	AddRuleOverride("ReservedSchemaName", allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(
	                                          SuggestionState::SUGGEST_SCHEMA_NAME, compiled.GetKeywordHelper())));
	AddRuleOverride("TableName", allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_TABLE_NAME,
	                                                                             compiled.GetKeywordHelper())));
	AddRuleOverride("ReservedTableName", allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(
	                                         SuggestionState::SUGGEST_TABLE_NAME, compiled.GetKeywordHelper())));
	AddRuleOverride("ColumnName", allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_COLUMN_NAME,
	                                                                              compiled.GetKeywordHelper())));
	AddRuleOverride("ReservedColumnName", allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(
	                                          SuggestionState::SUGGEST_COLUMN_NAME, compiled.GetKeywordHelper())));
	AddRuleOverride("IndexName", allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE,
	                                                                             compiled.GetKeywordHelper())));
	AddRuleOverride("ReservedIndexName", allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(
	                                         SuggestionState::SUGGEST_VARIABLE, compiled.GetKeywordHelper())));
	AddRuleOverride("SequenceName", allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE,
	                                                                                compiled.GetKeywordHelper())));
	AddRuleOverride("FunctionName", allocator.Allocate(make_uniq<IdentifierMatcher>(
	                                    SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME, compiled.GetKeywordHelper())));
	AddRuleOverride("ReservedFunctionName",
	                allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(
	                    SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME, compiled.GetKeywordHelper())));
	AddRuleOverride("ReservedKeyword", allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(
	                                       SuggestionState::SUGGEST_VARIABLE, compiled.GetKeywordHelper())));
	AddRuleOverride("TableFunctionName",
	                allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_TABLE_FUNCTION_NAME,
	                                                                compiled.GetKeywordHelper())));
	AddRuleOverride("TypeName", allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_TYPE_NAME,
	                                                                            compiled.GetKeywordHelper())));
	AddRuleOverride("ReservedTypeName", allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(
	                                        SuggestionState::SUGGEST_TYPE_NAME, compiled.GetKeywordHelper())));
	AddRuleOverride("PragmaName", allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_PRAGMA_NAME,
	                                                                              compiled.GetKeywordHelper())));
	AddRuleOverride("SettingName", allocator.Allocate(make_uniq<IdentifierMatcher>(
	                                   SuggestionState::SUGGEST_SETTING_NAME, compiled.GetKeywordHelper())));
	AddRuleOverride("CopyOptionName", allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(
	                                      SuggestionState::SUGGEST_VARIABLE, compiled.GetKeywordHelper())));
	AddRuleOverride("NumberLiteral", allocator.Allocate(make_uniq<NumberLiteralMatcher>()));
	AddRuleOverride("StringLiteral", allocator.Allocate(make_uniq<StringLiteralMatcher>()));
	AddRuleOverride("OperatorLiteral", allocator.Allocate(make_uniq<OperatorMatcher>()));
	//===--------------------------------------------------------------------===//
	// END GENERATED RULE OVERRIDES
	//===--------------------------------------------------------------------===//

	// EndOfInput has no grammar body; satisfied here (outside the regenerated block).
	AddRuleOverride("EndOfInput", allocator.Allocate(make_uniq<EndOfInputMatcher>()));

	// suppress suggestions for catch-all rules that would pollute statement-level autocomplete
	SuppressSuggestions("ExpressionStatement");

	// now create the matchers for each of the rules recursively - starting at the root rule
	return CreateMatcher(string_t(root_rule));
}

unique_ptr<KeywordMatcher> MatcherFactory::CreateKeyword(const string &keyword, const KeywordInfo &info) const {
	return make_uniq<KeywordMatcher>(keyword, info);
}

unique_ptr<ListMatcher> MatcherFactory::CreateList() const {
	return make_uniq<ListMatcher>();
}

unique_ptr<ChoiceMatcher> MatcherFactory::CreateChoice(vector<reference<Matcher>> &&matchers) const {
	return make_uniq<ChoiceMatcher>(std::move(matchers));
}

unique_ptr<OptionalMatcher> MatcherFactory::CreateOptional(Matcher &matcher) const {
	return make_uniq<OptionalMatcher>(matcher);
}

unique_ptr<RepeatMatcher> MatcherFactory::CreateRepeat(Matcher &matcher) const {
	return make_uniq<RepeatMatcher>(matcher);
}

KeywordMatcher &MatcherFactory::Keyword(const string &keyword) const {
	auto it = keywords.find(keyword);
	if (it != keywords.end()) {
		return it->second;
	}

	optional<KeywordInfo> info;
	auto entry = keyword_overrides.find(keyword);
	if (entry != keyword_overrides.end()) {
		info.emplace(entry->second);
	} else {
		info.emplace(0, ' ');
	}
	auto &result = allocator.Allocate(CreateKeyword(keyword, *info)).Cast<KeywordMatcher>();
	keywords.emplace(keyword, result);
	return result;
}

ListMatcher &MatcherFactory::List() const {
	return allocator.Allocate(CreateList()).Cast<ListMatcher>();
}

ListMatcher &MatcherFactory::List(vector<reference<Matcher>> matchers) const {
	auto result = CreateList();
	result->matchers = std::move(matchers);
	return allocator.Allocate(std::move(result)).Cast<ListMatcher>();
}

ChoiceMatcher &MatcherFactory::Choice(vector<reference<Matcher>> &&matchers) const {
	return allocator.Allocate(CreateChoice(std::move(matchers))).Cast<ChoiceMatcher>();
}

OptionalMatcher &MatcherFactory::Optional(Matcher &matcher) const {
	return allocator.Allocate(CreateOptional(matcher)).Cast<OptionalMatcher>();
}

RepeatMatcher &MatcherFactory::Repeat(Matcher &matcher) const {
	return allocator.Allocate(CreateRepeat(matcher)).Cast<RepeatMatcher>();
}

Matcher &MatcherFactory::GetMatcher(const string &rule_name) {
	auto entry = matchers.find(rule_name);
	if (entry == matchers.end()) {
		throw InternalException("Matcher for rule '%s' has not been built", rule_name);
	}
	return entry->second.get();
}

Matcher &MatcherFactory::CreateMatcher(string_t rule_name) {
	vector<reference<Matcher>> parameters;
	return CreateMatcher(rule_name, parameters);
}

} // namespace duckdb
