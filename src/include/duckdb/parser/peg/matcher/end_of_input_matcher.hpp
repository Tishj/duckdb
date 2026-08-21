#pragma once

#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

//! Matches the iterator's end-of-input state; wired into the grammar's EndOfInput rule.
class EndOfInputMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::END_OF_INPUT;

public:
	EndOfInputMatcher() : Matcher(TYPE) {
	}

	MatchResultType Match(MatchState &state) const override {
		if (state.token_iterator.AtEndOfInput()) {
			state.token_iterator.ConsumeEndOfInput();
			state.UpdateMaxTokenIndex();
			return MatchResultType::SUCCESS;
		}
		return MatchResultType::FAIL;
	}

	optional_ptr<ParseResult> MatchParseResultInternal(MatchState &state) const override {
		if (state.token_iterator.AtEndOfInput()) {
			state.token_iterator.ConsumeEndOfInput();
			state.UpdateMaxTokenIndex();
			return state.allocator.Allocate(make_uniq<EndOfInputParseResult>());
		}
		return nullptr;
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "EndOfInput";
	}
};

} // namespace duckdb
