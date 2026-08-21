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
    : owned_tokens(std::move(owned_tokens_p)), tokens(RequireOwnedTokens(owned_tokens)),
      end_of_input("", tokens.EndOffset(), TokenType::END_OF_INPUT) {
}

TokenIterator::TokenIterator(MatcherTokenStream &tokens_p)
    : tokens(tokens_p), end_of_input("", tokens.EndOffset(), TokenType::END_OF_INPUT) {
}

TokenIterator::TokenIterator(TokenIterator &other)
    : tokens(other.tokens), end_of_input(other.end_of_input), position(other.position) {
}

TokenIterator::TokenIterator(TokenIterator &&other) noexcept
    : owned_tokens(std::move(other.owned_tokens)), tokens(other.tokens), end_of_input(std::move(other.end_of_input)),
      position(other.position) {
}

bool TokenIterator::AtEnd() const {
	return !Current() || AtEndOfInput();
}

bool TokenIterator::AtEndOfInput() const {
	return position == tokens.size() && !tokens.CanAutocomplete();
}

bool TokenIterator::AtAutocompleteCursor() const {
	return position == tokens.size() && tokens.CanAutocomplete();
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
	return tokens.size() + 1;
}

idx_t TokenIterator::EndOffset() const {
	return tokens.EndOffset();
}

optional_ptr<const MatcherToken> TokenIterator::Current() const {
	if (position > tokens.size()) {
		return nullptr;
	}
	if (position == tokens.size()) {
		return end_of_input;
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
	if (index == tokens.size()) {
		return end_of_input;
	}
	return tokens[index];
}

void TokenIterator::Advance(idx_t count) {
	if (count > Size() - position) {
		throw InternalException("Cannot advance TokenIterator by %llu tokens from position %llu (size %llu)", count,
		                        position, Size());
	}
	position += count;
}

void TokenIterator::SetPosition(idx_t position_p) {
	if (position_p > Size()) {
		throw InternalException("Token position %llu is out of range (size %llu)", position_p, Size());
	}
	position = position_p;
}

void TokenIterator::SetPosition(const TokenIterator &other) {
	if (&tokens != &other.tokens) {
		throw InternalException("Cannot set TokenIterator position from a different token collection");
	}
	SetPosition(other.position);
}

void TokenIterator::SetPreviousTokenType(TokenType type) {
	if (position == 0) {
		throw InternalException("TokenIterator has no previous token to annotate");
	}
	if (position > tokens.size()) {
		throw InternalException("Cannot annotate the end-of-input token");
	}
	tokens[position - 1].type = type;
}

vector<SimpleToken> TokenIterator::RemainingTokens() const {
	vector<SimpleToken> result;
	result.reserve(Size() - position);
	for (idx_t index = position; index < Size(); index++) {
		auto &token = GetToken(index);
		result.emplace_back(token.text, token.type);
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
