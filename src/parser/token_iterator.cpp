#include "duckdb/parser/token_iterator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {

MatcherTokenStream &TokenIterator::RequireOwnedTokens(const unique_ptr<MatcherTokenStream> &owned_tokens) {
	if (!owned_tokens) {
		throw InternalException("Cannot construct an owning TokenIterator without tokens");
	}
	return *owned_tokens;
}

TokenIterator::TokenIterator(unique_ptr<MatcherTokenStream> owned_tokens_p)
    : owned_tokens(std::move(owned_tokens_p)), tokens(RequireOwnedTokens(owned_tokens)) {
}

TokenIterator::TokenIterator(MatcherTokenStream &tokens_p) : tokens(tokens_p) {
}

TokenIterator::TokenIterator(TokenIterator &other)
    : tokens(other.tokens), position(other.position), end_of_input_consumed(other.end_of_input_consumed) {
}

TokenIterator::TokenIterator(TokenIterator &&other) noexcept
    : owned_tokens(std::move(other.owned_tokens)), tokens(other.tokens), position(other.position),
      end_of_input_consumed(other.end_of_input_consumed) {
}

bool TokenIterator::AtEnd() const {
	return position == tokens.size() && !tokens.CanAutocomplete();
}

bool TokenIterator::AtEndOfInput() const {
	return AtEnd() && !end_of_input_consumed;
}

bool TokenIterator::AtAutocompleteCursor() const {
	return position == tokens.size() && tokens.CanAutocomplete();
}

void TokenIterator::ConsumeEndOfInput() {
	if (!AtEndOfInput()) {
		throw InternalException("TokenIterator is not at end-of-input");
	}
	end_of_input_consumed = true;
}

bool TokenIterator::HasMoreStatements() const {
	for (idx_t index = position; index < tokens.size(); index++) {
		auto type = tokens[index].type;
		if (type != TokenType::TERMINATOR) {
			return true;
		}
	}
	return false;
}

idx_t TokenIterator::Position() const {
	return position;
}

idx_t TokenIterator::Size() const {
	return tokens.size();
}

idx_t TokenIterator::EndOffset() const {
	return tokens.EndOffset();
}

optional_ptr<const MatcherToken> TokenIterator::Current() const {
	if (position >= tokens.size()) {
		return nullptr;
	}
	return tokens[position];
}

const MatcherToken &TokenIterator::Previous() const {
	if (position == 0) {
		throw InternalException("TokenIterator has no previous token");
	}
	return GetToken(position - 1);
}

const MatcherToken &TokenIterator::GetToken(idx_t index) const {
	if (index >= Size()) {
		throw InternalException("Token index %llu is out of range (size %llu)", index, Size());
	}
	return tokens[index];
}

void TokenIterator::Advance(idx_t count) {
	if (count > Size() - position) {
		throw InternalException("Cannot advance TokenIterator by %llu tokens from position %llu (size %llu)", count,
		                        position, Size());
	}
	position += count;
	if (count > 0) {
		end_of_input_consumed = false;
	}
}

void TokenIterator::SetPosition(idx_t position_p) {
	if (position_p > Size()) {
		throw InternalException("Token position %llu is out of range (size %llu)", position_p, Size());
	}
	position = position_p;
	if (position < tokens.size()) {
		end_of_input_consumed = false;
	}
}

void TokenIterator::SetPosition(const TokenIterator &other) {
	if (&tokens != &other.tokens) {
		throw InternalException("Cannot set TokenIterator position from a different token collection");
	}
	position = other.position;
	end_of_input_consumed = other.end_of_input_consumed;
}

void TokenIterator::SetPreviousTokenType(TokenType type) {
	if (position == 0) {
		throw InternalException("TokenIterator has no previous token to annotate");
	}
	tokens[position - 1].type = type;
}

vector<SimpleToken> TokenIterator::RemainingTokens() const {
	vector<SimpleToken> result;
	result.reserve(tokens.size() - position);
	for (idx_t index = position; index < tokens.size(); index++) {
		result.emplace_back(tokens[index].text, tokens[index].type);
	}
	return result;
}

string TokenIterator::ToString() const {
	string result;
	for (auto &token : tokens) {
		result += token.text + " ";
	}
	return result;
}

} // namespace duckdb
