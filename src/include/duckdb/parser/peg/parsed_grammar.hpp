//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/parsed_grammar.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/peg/peg_parser.hpp"
#include "duckdb/parser/peg/transformer/transform_result.hpp"

#include <functional>

namespace duckdb {

class ParseResult;
class PEGTransformer;

using grammar_transform_function_t = std::function<unique_ptr<TransformResultValue>(PEGTransformer &, ParseResult &)>;
using grammar_cursor_function_t = std::function<bool(const PEGNode &)>;

struct ParsedGrammarRule {
	ParsedGrammarRule(string name_p, PEGRule recipe_p) : name(std::move(name_p)), recipe(std::move(recipe_p)) {
	}

	string name;
	PEGRule recipe;
	grammar_transform_function_t transform;
};

//! Mutable, owning representation of a PEG grammar before matcher compilation.
class ParsedGrammar {
public:
	ParsedGrammar() = default;
	DUCKDB_API ParsedGrammar(ParsedGrammar &&other) noexcept;
	DUCKDB_API ParsedGrammar &operator=(ParsedGrammar &&other) noexcept;
	ParsedGrammar(const ParsedGrammar &) = delete;
	ParsedGrammar &operator=(const ParsedGrammar &) = delete;

	DUCKDB_API static ParsedGrammar Parse(const string &grammar);
	DUCKDB_API static ParsedGrammar CreateDefault();

	DUCKDB_API optional_ptr<ParsedGrammarRule> GetRule(const string &rule_name);
	DUCKDB_API optional_ptr<const ParsedGrammarRule> GetRule(const string &rule_name) const;
	DUCKDB_API void AddRule(const string &rule_definition, grammar_transform_function_t transform = nullptr);
	//! Adds a choice after the top-level choice selected by find_cursor, or at the end when no cursor is provided.
	DUCKDB_API void AddChoice(const string &rule_name, const string &choice,
	                          grammar_cursor_function_t find_cursor = nullptr);
	//! Adds a choice before the top-level choice selected by find_cursor, or at the start when no cursor is provided.
	DUCKDB_API void PrependChoice(const string &rule_name, const string &choice,
	                              grammar_cursor_function_t find_cursor = nullptr);
	//! Removes the top-level choice selected by find_cursor.
	DUCKDB_API void RemoveChoice(const string &rule_name, grammar_cursor_function_t find_cursor);
	DUCKDB_API void ReplaceRule(const string &rule_definition, grammar_transform_function_t transform = nullptr);
	DUCKDB_API void SetTransform(const string &rule_name, grammar_transform_function_t transform);

private:
	friend class MatcherFactory;
	friend struct ParserCache;
	friend class PEGTransformerFactory;

	void AddParsedRule(ParsedGrammarRule rule);
	void InsertChoice(const string &rule_name, const string &choice, grammar_cursor_function_t find_cursor,
	                  bool prepend);
	ParsedGrammarRule &GetMutableRule(const string &rule_name);
	static ParsedGrammarRule ParseSingleRule(const string &rule_definition);

	case_insensitive_map_t<unique_ptr<ParsedGrammarRule>> rules;
};

//! Immutable semantic data referenced directly by matchers and parse results.
struct CompiledGrammarRule {
	CompiledGrammarRule(string name_p, grammar_transform_function_t transform_p)
	    : name(std::move(name_p)), transform(std::move(transform_p)) {
	}

	string name;
	grammar_transform_function_t transform;
};

} // namespace duckdb
