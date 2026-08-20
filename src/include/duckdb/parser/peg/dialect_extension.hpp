//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/dialect_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
struct DBConfig;
class MatcherFactory;
class Tokenizer;
class PEGKeywordHelper;
class MatcherAllocator;
class ParsedGrammar;
struct CompiledGrammar;
struct CompiledGrammarRule;
class Matcher;

using compiled_rules_map_t = case_insensitive_map_t<unique_ptr<CompiledGrammarRule>>;

using terminal_rule_overrides_t = case_insensitive_map_t<unique_ptr<Matcher>>;

struct CreateTokenizerInput {
	const PEGKeywordHelper &keyword_helper;
};

struct CreateMatcherFactoryInput {
	MatcherAllocator &allocator;
	const ParsedGrammar &parsed_grammar;
	const compiled_rules_map_t &rules;
	terminal_rule_overrides_t terminal_rules;
};

struct CreateKeywordHelperInput {
	const ParsedGrammar &grammar;
};

//! A named SQL dialect that can customize the PEG parser.
class DialectExtension {
public:
	explicit DialectExtension(string name_p) : name(std::move(name_p)) {
	}
	virtual ~DialectExtension() = default;

public:
	static void Register(DBConfig &config, unique_ptr<DialectExtension> extension);

public:
	shared_ptr<CompiledGrammar> GetCompiledGrammar();
	const string &Name() const;

public:
	virtual void ApplyGrammarChanges(ParsedGrammar &grammar);
	virtual unique_ptr<MatcherFactory> CreateMatcherFactory(CreateMatcherFactoryInput &input);
	virtual unique_ptr<Tokenizer> CreateTokenizer(CreateTokenizerInput &input);
	virtual unique_ptr<PEGKeywordHelper> CreateKeywordHelper(CreateKeywordHelperInput &input);

private:
	string name;
	mutex lock;
	//! Cache the compiled grammar, since it never needs to be invalidated
	shared_ptr<CompiledGrammar> cache;
};

} // namespace duckdb
