#pragma once
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/windows_undefs.hpp"

namespace duckdb {
enum class PEGNodeType {
	INVALID,
	LITERAL,
	REFERENCE,
	REGEX,
	SEQUENCE,
	CHOICE,
	GROUP,
	FUNCTION,
	OPTIONAL,
	REPEAT,
	NEGATIVE_LOOKAHEAD
};

class DUCKDB_API PEGNode {
public:
	explicit PEGNode(PEGNodeType type_p);
	virtual ~PEGNode();

	PEGNodeType GetType() const;
	virtual unique_ptr<PEGNode> Copy() const = 0;

	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast PEG node to type - PEG node type mismatch");
		}
		return static_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast PEG node to type - PEG node type mismatch");
		}
		return static_cast<const TARGET &>(*this);
	}

private:
	PEGNodeType type;
};

class DUCKDB_API PEGTextNode : public PEGNode {
public:
	PEGTextNode(PEGNodeType type, string text_p);

	string text;
};

class DUCKDB_API PEGLiteralNode : public PEGTextNode {
public:
	static constexpr PEGNodeType TYPE = PEGNodeType::LITERAL;
	explicit PEGLiteralNode(string text_p);
	unique_ptr<PEGNode> Copy() const override;
};

class DUCKDB_API PEGReferenceNode : public PEGTextNode {
public:
	static constexpr PEGNodeType TYPE = PEGNodeType::REFERENCE;
	explicit PEGReferenceNode(string text_p);
	unique_ptr<PEGNode> Copy() const override;
};

class DUCKDB_API PEGRegexNode : public PEGTextNode {
public:
	static constexpr PEGNodeType TYPE = PEGNodeType::REGEX;
	explicit PEGRegexNode(string text_p);
	unique_ptr<PEGNode> Copy() const override;
};

class DUCKDB_API PEGContainerNode : public PEGNode {
public:
	PEGContainerNode(PEGNodeType type, vector<unique_ptr<PEGNode>> children_p);

	vector<unique_ptr<PEGNode>> children;
};

class DUCKDB_API PEGSequenceNode : public PEGContainerNode {
public:
	static constexpr PEGNodeType TYPE = PEGNodeType::SEQUENCE;
	explicit PEGSequenceNode(vector<unique_ptr<PEGNode>> children_p = {});
	unique_ptr<PEGNode> Copy() const override;
};

class DUCKDB_API PEGChoiceNode : public PEGContainerNode {
public:
	static constexpr PEGNodeType TYPE = PEGNodeType::CHOICE;
	explicit PEGChoiceNode(vector<unique_ptr<PEGNode>> children_p = {});
	unique_ptr<PEGNode> Copy() const override;
};

class DUCKDB_API PEGUnaryNode : public PEGNode {
public:
	PEGUnaryNode(PEGNodeType type, unique_ptr<PEGNode> child_p);

	unique_ptr<PEGNode> child;
};

class DUCKDB_API PEGGroupNode : public PEGUnaryNode {
public:
	static constexpr PEGNodeType TYPE = PEGNodeType::GROUP;
	explicit PEGGroupNode(unique_ptr<PEGNode> child_p);
	unique_ptr<PEGNode> Copy() const override;
};

class DUCKDB_API PEGOptionalNode : public PEGUnaryNode {
public:
	static constexpr PEGNodeType TYPE = PEGNodeType::OPTIONAL;
	explicit PEGOptionalNode(unique_ptr<PEGNode> child_p);
	unique_ptr<PEGNode> Copy() const override;
};

class DUCKDB_API PEGRepeatNode : public PEGUnaryNode {
public:
	static constexpr PEGNodeType TYPE = PEGNodeType::REPEAT;
	explicit PEGRepeatNode(unique_ptr<PEGNode> child_p);
	unique_ptr<PEGNode> Copy() const override;
};

class DUCKDB_API PEGNegativeLookaheadNode : public PEGUnaryNode {
public:
	static constexpr PEGNodeType TYPE = PEGNodeType::NEGATIVE_LOOKAHEAD;
	explicit PEGNegativeLookaheadNode(unique_ptr<PEGNode> child_p);
	unique_ptr<PEGNode> Copy() const override;
};

class DUCKDB_API PEGFunctionNode : public PEGUnaryNode {
public:
	static constexpr PEGNodeType TYPE = PEGNodeType::FUNCTION;
	PEGFunctionNode(string name_p, unique_ptr<PEGNode> child_p);
	unique_ptr<PEGNode> Copy() const override;

	string name;
};

struct PEGRule {
	unordered_map<string, idx_t> parameters;
	unique_ptr<PEGNode> root;

	void Clear() {
		parameters.clear();
		root.reset();
	}
};

struct PEGParser {
public:
	void ParseRules(const char *grammar);
	void AddRule(string_t rule_name, PEGRule rule);

	case_insensitive_map_t<PEGRule> rules;
};

enum class PEGParseState {
	RULE_NAME,      // Rule name
	RULE_SEPARATOR, // look for <-
	RULE_DEFINITION // part of rule definition
};

inline bool IsPEGOperator(char c) {
	switch (c) {
	case '/':
	case '?':
	case '(':
	case ')':
	case '*':
	case '+':
	case '!':
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
