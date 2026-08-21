//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/matcher_token_stream.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/parser/peg/matcher_token.hpp"

namespace duckdb {

enum class MatcherTokenStreamMode : uint8_t { PARSE, AUTOCOMPLETE };

//! A token collection together with the context needed to interpret its end position.
class MatcherTokenStream {
public:
	explicit MatcherTokenStream(MatcherTokenStreamMode mode_p = MatcherTokenStreamMode::PARSE) : mode(mode_p) {
	}

	template <class... ARGS>
	void emplace_back(ARGS &&... args) {
		tokens.emplace_back(std::forward<ARGS>(args)...);
	}

	void clear() {
		tokens.clear();
	}
	bool empty() const {
		return tokens.empty();
	}
	idx_t size() const {
		return tokens.size();
	}
	MatcherToken &back() {
		return tokens.back();
	}
	const MatcherToken &back() const {
		return tokens.back();
	}
	MatcherToken &operator[](idx_t index) {
		return tokens[index];
	}
	const MatcherToken &operator[](idx_t index) const {
		return tokens[index];
	}
	auto begin() {
		return tokens.begin();
	}
	auto end() {
		return tokens.end();
	}
	auto begin() const {
		return tokens.begin();
	}
	auto end() const {
		return tokens.end();
	}

	vector<MatcherToken> &GetTokens() {
		return tokens;
	}
	const vector<MatcherToken> &GetTokens() const {
		return tokens;
	}

	void SetEnd(idx_t end_offset_p, bool clean_end_p) {
		end_offset = end_offset_p;
		clean_end = clean_end_p;
	}
	idx_t EndOffset() const {
		return end_offset;
	}
	bool CanAutocomplete() const {
		return mode == MatcherTokenStreamMode::AUTOCOMPLETE && clean_end;
	}

private:
	vector<MatcherToken> tokens;
	MatcherTokenStreamMode mode;
	idx_t end_offset = 0;
	bool clean_end = false;
};

} // namespace duckdb
