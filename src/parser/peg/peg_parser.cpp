#include "duckdb/parser/peg/peg_parser.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"
#include "duckdb/parser/peg/inlined_grammar.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

PEGNode::PEGNode(PEGNodeType type_p) : type(type_p) {
}

PEGNode::~PEGNode() = default;

PEGNodeType PEGNode::GetType() const {
	return type;
}

PEGTextNode::PEGTextNode(PEGNodeType type, string text_p) : PEGNode(type), text(std::move(text_p)) {
}

PEGLiteralNode::PEGLiteralNode(string text_p) : PEGTextNode(TYPE, std::move(text_p)) {
}

unique_ptr<PEGNode> PEGLiteralNode::Copy() const {
	return make_uniq<PEGLiteralNode>(text);
}

PEGReferenceNode::PEGReferenceNode(string text_p) : PEGTextNode(TYPE, std::move(text_p)) {
}

unique_ptr<PEGNode> PEGReferenceNode::Copy() const {
	return make_uniq<PEGReferenceNode>(text);
}

PEGRegexNode::PEGRegexNode(string text_p) : PEGTextNode(TYPE, std::move(text_p)) {
}

unique_ptr<PEGNode> PEGRegexNode::Copy() const {
	return make_uniq<PEGRegexNode>(text);
}

PEGContainerNode::PEGContainerNode(PEGNodeType type, vector<unique_ptr<PEGNode>> children_p)
    : PEGNode(type), children(std::move(children_p)) {
}

static vector<unique_ptr<PEGNode>> CopyChildren(const vector<unique_ptr<PEGNode>> &children) {
	vector<unique_ptr<PEGNode>> result;
	result.reserve(children.size());
	for (auto &child : children) {
		result.push_back(child->Copy());
	}
	return result;
}

PEGSequenceNode::PEGSequenceNode(vector<unique_ptr<PEGNode>> children_p)
    : PEGContainerNode(TYPE, std::move(children_p)) {
}

unique_ptr<PEGNode> PEGSequenceNode::Copy() const {
	return make_uniq<PEGSequenceNode>(CopyChildren(children));
}

PEGChoiceNode::PEGChoiceNode(vector<unique_ptr<PEGNode>> children_p) : PEGContainerNode(TYPE, std::move(children_p)) {
}

unique_ptr<PEGNode> PEGChoiceNode::Copy() const {
	return make_uniq<PEGChoiceNode>(CopyChildren(children));
}

PEGUnaryNode::PEGUnaryNode(PEGNodeType type, unique_ptr<PEGNode> child_p) : PEGNode(type), child(std::move(child_p)) {
	D_ASSERT(child);
}

PEGGroupNode::PEGGroupNode(unique_ptr<PEGNode> child_p) : PEGUnaryNode(TYPE, std::move(child_p)) {
}

unique_ptr<PEGNode> PEGGroupNode::Copy() const {
	return make_uniq<PEGGroupNode>(child->Copy());
}

PEGOptionalNode::PEGOptionalNode(unique_ptr<PEGNode> child_p) : PEGUnaryNode(TYPE, std::move(child_p)) {
}

unique_ptr<PEGNode> PEGOptionalNode::Copy() const {
	return make_uniq<PEGOptionalNode>(child->Copy());
}

PEGRepeatNode::PEGRepeatNode(unique_ptr<PEGNode> child_p) : PEGUnaryNode(TYPE, std::move(child_p)) {
}

unique_ptr<PEGNode> PEGRepeatNode::Copy() const {
	return make_uniq<PEGRepeatNode>(child->Copy());
}

PEGNegativeLookaheadNode::PEGNegativeLookaheadNode(unique_ptr<PEGNode> child_p)
    : PEGUnaryNode(TYPE, std::move(child_p)) {
}

unique_ptr<PEGNode> PEGNegativeLookaheadNode::Copy() const {
	return make_uniq<PEGNegativeLookaheadNode>(child->Copy());
}

PEGFunctionNode::PEGFunctionNode(string name_p, unique_ptr<PEGNode> child_p)
    : PEGUnaryNode(TYPE, std::move(child_p)), name(std::move(name_p)) {
}

unique_ptr<PEGNode> PEGFunctionNode::Copy() const {
	return make_uniq<PEGFunctionNode>(name, child->Copy());
}

enum class PEGTokenType { LITERAL, REFERENCE, OPERATOR, FUNCTION_CALL, REGEX };

struct PEGToken {
	PEGTokenType type;
	string text;
};

static bool IsOperator(const PEGToken &token, char operator_char) {
	return token.type == PEGTokenType::OPERATOR && token.text.size() == 1 && token.text[0] == operator_char;
}

class PEGNodeParser {
public:
	explicit PEGNodeParser(const vector<PEGToken> &tokens_p) : tokens(tokens_p) {
	}

	unique_ptr<PEGNode> Parse() {
		if (tokens.empty()) {
			return nullptr;
		}
		auto result = ParseChoice();
		if (position != tokens.size()) {
			throw InternalException("Unexpected token while parsing PEG nodes");
		}
		return result;
	}

private:
	unique_ptr<PEGNode> ParseChoice() {
		vector<unique_ptr<PEGNode>> children;
		children.push_back(ParseSequence());
		while (position < tokens.size() && IsOperator(tokens[position], '/')) {
			position++;
			children.push_back(ParseSequence());
		}
		if (children.size() == 1) {
			return std::move(children[0]);
		}
		return make_uniq<PEGChoiceNode>(std::move(children));
	}

	unique_ptr<PEGNode> ParseSequence() {
		vector<unique_ptr<PEGNode>> children;
		while (position < tokens.size() && !IsOperator(tokens[position], '/') && !IsOperator(tokens[position], ')')) {
			children.push_back(ParseNode());
		}
		if (children.empty()) {
			throw InternalException("Empty sequence found while parsing PEG nodes");
		}
		if (children.size() == 1) {
			return std::move(children[0]);
		}
		return make_uniq<PEGSequenceNode>(std::move(children));
	}

	unique_ptr<PEGNode> ParseNode() {
		if (IsOperator(tokens[position], '!')) {
			position++;
			return make_uniq<PEGNegativeLookaheadNode>(ParseNode());
		}

		unique_ptr<PEGNode> result;
		auto &token = tokens[position];
		switch (token.type) {
		case PEGTokenType::LITERAL:
			position++;
			result = make_uniq<PEGLiteralNode>(token.text);
			break;
		case PEGTokenType::REFERENCE:
			position++;
			result = make_uniq<PEGReferenceNode>(token.text);
			break;
		case PEGTokenType::REGEX:
			position++;
			result = make_uniq<PEGRegexNode>(token.text);
			break;
		case PEGTokenType::FUNCTION_CALL: {
			position++;
			auto child = ParseChoice();
			if (position >= tokens.size() || !IsOperator(tokens[position], ')')) {
				throw InternalException("Unclosed function call found while parsing PEG nodes");
			}
			position++;
			result = make_uniq<PEGFunctionNode>(token.text, std::move(child));
			break;
		}
		case PEGTokenType::OPERATOR:
			if (!IsOperator(token, '(')) {
				throw InternalException("Unexpected PEG operator '%s'", token.text);
			}
			position++;
			result = make_uniq<PEGGroupNode>(ParseChoice());
			if (position >= tokens.size() || !IsOperator(tokens[position], ')')) {
				throw InternalException("Unclosed group found while parsing PEG nodes");
			}
			position++;
			break;
		default:
			throw InternalException("Unrecognized PEG token type");
		}

		while (position < tokens.size()) {
			if (IsOperator(tokens[position], '?')) {
				position++;
				result = make_uniq<PEGOptionalNode>(std::move(result));
			} else if (IsOperator(tokens[position], '*')) {
				position++;
				result = make_uniq<PEGRepeatNode>(std::move(result));
				result = make_uniq<PEGOptionalNode>(std::move(result));
			} else if (IsOperator(tokens[position], '+')) {
				position++;
				result = make_uniq<PEGRepeatNode>(std::move(result));
			} else {
				break;
			}
		}
		return result;
	}

private:
	const vector<PEGToken> &tokens;
	idx_t position = 0;
};

void PEGParser::AddRule(string_t rule_name, PEGRule rule) {
	auto entry = rules.find(rule_name.GetString());
	if (entry != rules.end()) {
		throw InternalException("Failed to parse grammar - duplicate rule name %s", rule_name.GetString());
	}
	rules.insert(make_pair(rule_name, std::move(rule)));
}

void PEGParser::ParseRules(const char *grammar) {
	string_t rule_name;
	PEGRule rule;
	vector<PEGToken> tokens;
	PEGParseState parse_state = PEGParseState::RULE_NAME;
	idx_t bracket_count = 0;
	bool in_or_clause = false;
	// look for the rules
	idx_t c = 0;
	while (grammar[c]) {
		if (grammar[c] == '#') {
			// comment - ignore until EOL
			while (grammar[c] && !StringUtil::CharacterIsNewline(grammar[c])) {
				c++;
			}
			continue;
		}
		if (parse_state == PEGParseState::RULE_DEFINITION && StringUtil::CharacterIsNewline(grammar[c]) &&
		    bracket_count == 0 && !in_or_clause && !tokens.empty()) {
			// if we see a newline while we are parsing a rule definition we can complete the rule
			rule.root = PEGNodeParser(tokens).Parse();
			AddRule(rule_name, std::move(rule));
			rule = PEGRule();
			tokens.clear();
			rule_name = string_t();
			// look for the subsequent rule
			parse_state = PEGParseState::RULE_NAME;
			c++;
			continue;
		}
		if (StringUtil::CharacterIsSpace(grammar[c])) {
			// skip whitespace
			c++;
			continue;
		}
		switch (parse_state) {
		case PEGParseState::RULE_NAME: {
			// look for alpha-numerics
			idx_t start_pos = c;
			if (grammar[c] == '%') {
				// rules can start with % (%whitespace)
				c++;
			}
			while (grammar[c] && StringUtil::CharacterIsAlphaNumeric(grammar[c])) {
				c++;
			}
			if (c == start_pos) {
				throw InternalException("Failed to parse grammar - expected an alpha-numeric rule name (pos %d)", c);
			}
			rule_name = string_t(grammar + start_pos, UnsafeNumericCast<uint32_t>(c - start_pos));
			rule.Clear();
			tokens.clear();
			parse_state = PEGParseState::RULE_SEPARATOR;
			break;
		}
		case PEGParseState::RULE_SEPARATOR: {
			if (grammar[c] == '(') {
				if (!rule.parameters.empty()) {
					throw InternalException("Failed to parse grammar - multiple parameters at position %d", c);
				}
				// parameter
				c++;
				idx_t parameter_start = c;
				while (grammar[c] && StringUtil::CharacterIsAlphaNumeric(grammar[c])) {
					c++;
				}
				if (parameter_start == c) {
					throw InternalException("Failed to parse grammar - expected a parameter at position %d", c);
				}
				rule.parameters.insert(
				    make_pair(string(grammar + parameter_start, c - parameter_start), rule.parameters.size()));
				if (grammar[c] != ')') {
					throw InternalException("Failed to parse grammar - expected closing bracket at position %d", c);
				}
				c++;
			} else {
				if (grammar[c] != '<' || grammar[c + 1] != '-') {
					throw InternalException("Failed to parse grammar - expected a rule definition (<-) (pos %d)", c);
				}
				c += 2;
				parse_state = PEGParseState::RULE_DEFINITION;
			}
			break;
		}
		case PEGParseState::RULE_DEFINITION: {
			// we parse either:
			// (1) a literal ('Keyword'i)
			// (2) a rule reference (Rule)
			// (3) an operator ( '(' '/' '?' '*' ')' '+')
			in_or_clause = false;
			if (grammar[c] == '\'') {
				// parse literal
				c++;
				idx_t literal_start = c;
				while (grammar[c] && grammar[c] != '\'') {
					if (grammar[c] == '\\') {
						// escape
						c++;
					}
					c++;
				}
				if (!grammar[c]) {
					throw InternalException("Failed to parse grammar - did not find closing ' (pos %d)", c);
				}
				PEGToken token;
				token.text = string(grammar + literal_start, c - literal_start);
				token.type = PEGTokenType::LITERAL;
				tokens.push_back(std::move(token));
				c++;
				if (grammar[c] == 'i') {
					throw InternalException("Failed to parse grammar - unexpected \"i\" found in grammar near rule %s",
					                        rule_name.GetString());
				}
			} else if (StringUtil::CharacterIsAlphaNumeric(grammar[c])) {
				// alphanumeric character - this is a rule reference
				idx_t rule_start = c;
				while (grammar[c] && StringUtil::CharacterIsAlphaNumeric(grammar[c])) {
					c++;
				}
				PEGToken token;
				token.text = string(grammar + rule_start, c - rule_start);
				if (grammar[c] == '(') {
					// this is a function call
					c++;
					bracket_count++;
					token.type = PEGTokenType::FUNCTION_CALL;
				} else {
					token.type = PEGTokenType::REFERENCE;
				}
				tokens.push_back(std::move(token));
			} else if (grammar[c] == '[' || grammar[c] == '<') {
				// regular expression- [^"] or <...>
				idx_t rule_start = c;
				char final_char = grammar[c] == '[' ? ']' : '>';
				while (grammar[c] && grammar[c] != final_char) {
					if (grammar[c] == '\\') {
						// handle escapes
						c++;
					}
					if (grammar[c]) {
						c++;
					}
				}
				c++;
				PEGToken token;
				token.text = string(grammar + rule_start, c - rule_start);
				token.type = PEGTokenType::REGEX;
				tokens.push_back(std::move(token));
			} else if (IsPEGOperator(grammar[c])) {
				if (grammar[c] == '(') {
					bracket_count++;
				} else if (grammar[c] == ')') {
					if (bracket_count == 0) {
						throw InternalException("Failed to parse grammar - unclosed bracket at position %d in rule %s",
						                        c, rule_name.GetString());
					}
					bracket_count--;
				} else if (grammar[c] == '/') {
					in_or_clause = true;
				}
				// operator - operators are always length 1
				PEGToken token;
				token.text = string(grammar + c, 1);
				token.type = PEGTokenType::OPERATOR;
				tokens.push_back(std::move(token));
				c++;
			} else {
				throw InternalException("Unrecognized rule contents in rule %s (character %s)", rule_name.GetString(),
				                        string(1, grammar[c]));
			}
			break;
		}
		default:
			break;
		}
		if (!grammar[c]) {
			break;
		}
	}
	if (parse_state == PEGParseState::RULE_SEPARATOR) {
		throw InternalException("Failed to parse grammar - rule %s does not have a definition", rule_name.GetString());
	}
	if (parse_state == PEGParseState::RULE_DEFINITION) {
		if (tokens.empty()) {
			throw InternalException("Failed to parse grammar - rule %s is empty", rule_name.GetString());
		}
		rule.root = PEGNodeParser(tokens).Parse();
		AddRule(rule_name, std::move(rule));
	}
}

} // namespace duckdb
