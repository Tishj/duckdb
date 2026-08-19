#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parser_change.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"

using namespace duckdb;

static unique_ptr<TransformResultValue> TransformParserChangeTestAtom(PEGTransformer &, ParseResult &) {
	auto statement = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<ConstantExpression>(Value::INTEGER(42)));
	select_node->from_table = make_uniq<EmptyTableRef>();
	statement->node = std::move(select_node);
	return make_uniq<TypedTransformResult<unique_ptr<SelectStatement>>>(std::move(statement));
}

class AddParserChangeTestValue final : public ParserChange {
public:
	AddParserChangeTestValue() : ParserChange(ParserChangeType::GRAMMAR) {
	}

	void Apply(ParsedGrammar &grammar) const override {
		grammar.AddRule("ParserChangeTestValue <- 'ANSWER'");
	}
};

class AddParserChangeTestAtom final : public ParserChange {
public:
	AddParserChangeTestAtom() : ParserChange(ParserChangeType::GRAMMAR) {
	}

	void Apply(ParsedGrammar &grammar) const override {
		grammar.AddRule("ParserChangeTestAtom <- ParserChangeTestValue", TransformParserChangeTestAtom);
		grammar.PrependChoice("SelectAtom", "ParserChangeTestAtom", [](const PEGNode &node) {
			return node.GetType() == PEGNodeType::REFERENCE && node.Cast<PEGReferenceNode>().text == "SelectParens";
		});
	}
};

static void RegisterParserChangeTestSyntax(DatabaseInstance &db) {
	ParserChange::Register(db, make_shared_ptr<AddParserChangeTestValue>());
	ParserChange::Register(db, make_shared_ptr<AddParserChangeTestAtom>());
}

static void CheckParserChangeTestSyntax(Connection &con) {
	auto result = con.Query("ANSWER");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0) == Value::INTEGER(42));
}

TEST_CASE("Parser changes apply in registration order", "[api][parser_change]") {
	DuckDB db(nullptr);
	RegisterParserChangeTestSyntax(*db.instance);
	Connection con(db);
	CheckParserChangeTestSyntax(con);
}

TEST_CASE("Grammar choices support cursor placement", "[api][parser_change]") {
	auto grammar = ParsedGrammar::Parse("CursorRule <- First('first' / 'one')? FirstTail / Last('last')* LastTail");
	grammar.AddChoice("CursorRule", "Second", [](const PEGNode &node) {
		if (node.GetType() != PEGNodeType::SEQUENCE) {
			return false;
		}
		auto &sequence = node.Cast<PEGSequenceNode>();
		if (sequence.children.size() != 2 || sequence.children[0]->GetType() != PEGNodeType::OPTIONAL) {
			return false;
		}
		auto &function = *sequence.children[0]->Cast<PEGOptionalNode>().child;
		return function.GetType() == PEGNodeType::FUNCTION && function.Cast<PEGFunctionNode>().name == "First" &&
		       function.Cast<PEGFunctionNode>().child->GetType() == PEGNodeType::CHOICE &&
		       sequence.children[1]->GetType() == PEGNodeType::REFERENCE &&
		       sequence.children[1]->Cast<PEGReferenceNode>().text == "FirstTail";
	});
	grammar.PrependChoice("CursorRule", "Third", [](const PEGNode &node) {
		if (node.GetType() != PEGNodeType::SEQUENCE) {
			return false;
		}
		auto &sequence = node.Cast<PEGSequenceNode>();
		if (sequence.children.size() != 2 || sequence.children[0]->GetType() != PEGNodeType::OPTIONAL) {
			return false;
		}
		auto &repeat = *sequence.children[0]->Cast<PEGOptionalNode>().child;
		return repeat.GetType() == PEGNodeType::REPEAT &&
		       repeat.Cast<PEGRepeatNode>().child->GetType() == PEGNodeType::FUNCTION &&
		       repeat.Cast<PEGRepeatNode>().child->Cast<PEGFunctionNode>().name == "Last" &&
		       sequence.children[1]->GetType() == PEGNodeType::REFERENCE &&
		       sequence.children[1]->Cast<PEGReferenceNode>().text == "LastTail";
	});

	vector<string> choices;
	auto rule = grammar.GetRule("CursorRule");
	REQUIRE(rule);
	REQUIRE(rule->recipe.root->GetType() == PEGNodeType::CHOICE);
	for (auto &choice : rule->recipe.root->Cast<PEGChoiceNode>().children) {
		auto node = choice.get();
		if (node->GetType() == PEGNodeType::SEQUENCE) {
			node = node->Cast<PEGSequenceNode>().children[0].get();
		}
		while (node->GetType() == PEGNodeType::OPTIONAL || node->GetType() == PEGNodeType::REPEAT) {
			node = static_cast<PEGUnaryNode *>(node)->child.get();
		}
		if (node->GetType() == PEGNodeType::FUNCTION) {
			choices.push_back(node->Cast<PEGFunctionNode>().name);
		} else {
			REQUIRE(node->GetType() == PEGNodeType::REFERENCE);
			choices.push_back(node->Cast<PEGReferenceNode>().text);
		}
	}
	REQUIRE(choices == vector<string> {"First", "Second", "Third", "Last"});
}

TEST_CASE("Grammar nodes support structured modification", "[api][parser_change]") {
	auto grammar = ParsedGrammar::Parse("MutableRule <- First / Second");
	auto rule = grammar.GetRule("MutableRule");
	REQUIRE(rule);
	auto copy = rule->recipe.root->Copy();
	auto &choices = rule->recipe.root->Cast<PEGChoiceNode>().children;
	choices.erase(choices.begin());
	choices[0] = make_uniq<PEGOptionalNode>(make_uniq<PEGReferenceNode>("Replacement"));
	choices.push_back(make_uniq<PEGLiteralNode>("literal"));

	REQUIRE(choices.size() == 2);
	REQUIRE(choices[0]->Cast<PEGOptionalNode>().child->Cast<PEGReferenceNode>().text == "Replacement");
	REQUIRE(choices[1]->Cast<PEGLiteralNode>().text == "literal");
	REQUIRE(copy->Cast<PEGChoiceNode>().children[0]->Cast<PEGReferenceNode>().text == "First");
}

TEST_CASE("Parser changes invalidate an initialized parser cache", "[api][parser_change]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(*con.Query("SELECT 1"));
	REQUIRE_FALSE(CompiledGrammar::Get(*con.context)->HasGrammarChanges());

	RegisterParserChangeTestSyntax(*db.instance);
	CheckParserChangeTestSyntax(con);
	REQUIRE(CompiledGrammar::Get(*con.context)->HasGrammarChanges());
}

class AddInvalidParserChangeTestRule final : public ParserChange {
public:
	AddInvalidParserChangeTestRule() : ParserChange(ParserChangeType::GRAMMAR) {
	}

	void Apply(ParsedGrammar &grammar) const override {
		grammar.AddRule("ParserChangeInvalid <- ParserChangeMissingRule");
	}
};

TEST_CASE("Invalid parser changes fail grammar compilation", "[api][parser_change]") {
	DuckDB db(nullptr);
	Connection con(db);
	ParserChange::Register(*db.instance, make_shared_ptr<AddInvalidParserChangeTestRule>());
	REQUIRE_THROWS(CompiledGrammar::Get(*con.context));
}
