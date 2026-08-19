#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/peg/matcher_factory.hpp"
#include "duckdb/parser/peg/keyword_helper/parsed_grammar_keyword_helper.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/parser/parser_change.hpp"

namespace duckdb {

CompiledGrammar::CompiledGrammar(const ParsedGrammar &grammar, bool has_grammar_changes_p, idx_t version_p)
    : owned_keyword_helper(make_uniq<ParsedGrammarKeywordHelper>(grammar)), keyword_helper(*owned_keyword_helper),
      tokenizer(keyword_helper), has_grammar_changes(has_grammar_changes_p), version(version_p) {
}

idx_t CompiledGrammar::Version() const {
	return version;
}

shared_ptr<CompiledGrammar> CompiledGrammar::Get(ClientContext &context) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &client_config = ClientConfig::GetConfig(context);
	auto &cache = db.GetParserCache();
	if (!client_config.cached_grammar || client_config.cached_grammar->Version() != cache.LatestParserVersion()) {
		client_config.cached_grammar = cache.GetMatcher(context);
	}
	return client_config.cached_grammar;
}

ParserCache::ParserCache() : version(0) {
}

static void ValidateParsedGrammarRoots(const ParsedGrammar &grammar) {
	if (!grammar.GetRule("Program")) {
		throw InvalidInputException("Grammar is missing required root rule 'Program'");
	}
	if (!grammar.GetRule("TopLevelStatement")) {
		throw InvalidInputException("Grammar is missing required root rule 'TopLevelStatement'");
	}
}

shared_ptr<CompiledGrammar> ParserCache::GetMatcher(optional_ptr<const ClientContext> context) {
	idx_t parser_version;
	{
		std::unique_lock<std::mutex> lock(mutex);
		if (matcher) {
			return matcher;
		}
		parser_version = version;
	}

	vector<shared_ptr<ParserChange>> parser_changes;
	if (context) {
		for (auto &change : ExtensionCallbackManager::Get(*context).ParserChanges()) {
			parser_changes.push_back(change);
		}
	}

	auto grammar = ParsedGrammar::CreateDefault();
	bool has_grammar_changes = false;
	for (auto &change : parser_changes) {
		switch (change->type) {
		case ParserChangeType::GRAMMAR:
			has_grammar_changes = true;
			change->Apply(grammar);
			break;
		default:
			throw InternalException("Unsupported parser change type");
		}
	}
	ValidateParsedGrammarRoots(grammar);
	for (auto &entry : grammar.rules) {
		auto &rule = *entry.second;
		for (auto &token : rule.recipe.tokens) {
			if (token.type != PEGTokenType::REFERENCE && token.type != PEGTokenType::FUNCTION_CALL) {
				continue;
			}
			if (token.type == PEGTokenType::REFERENCE && rule.recipe.parameters.count(token.text)) {
				continue;
			}
			if (!grammar.GetRule(token.text.GetString())) {
				throw InvalidInputException("Grammar rule '%s' references missing rule '%s'", rule.name,
				                            token.text.GetString());
			}
		}
	}

	auto new_matcher = shared_ptr<CompiledGrammar>(new CompiledGrammar(grammar, has_grammar_changes, parser_version));
	for (auto &entry : grammar.rules) {
		auto &rule = *entry.second;
		new_matcher->rules.emplace(rule.name, make_uniq<CompiledGrammarRule>(rule.name, rule.transform));
	}
	MatcherFactory factory(new_matcher->allocator, grammar, *new_matcher);
	new_matcher->program_matcher = factory.CreateRootMatcher("Program");
	new_matcher->top_level_statement_matcher = factory.GetMatcher("TopLevelStatement");

	std::unique_lock<std::mutex> lock(mutex);
	if (version == parser_version) {
		if (matcher) {
			return matcher;
		}
		matcher = new_matcher;
	}
	return new_matcher;
}

optional_ptr<const CompiledGrammarRule> CompiledGrammar::GetRule(const string &rule_name) const {
	auto entry = rules.find(rule_name);
	if (entry == rules.end()) {
		return nullptr;
	}
	return *entry->second;
}

idx_t ParserCache::LatestParserVersion() const {
	return version;
}

void ParserCache::Invalidate() {
	std::unique_lock<std::mutex> lock(mutex);
	matcher = nullptr;
	++version;
}

} // namespace duckdb
