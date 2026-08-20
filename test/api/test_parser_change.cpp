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
		grammar.PrependChoice("SelectAtom", "ParserChangeTestAtom", [](const PEGExpression &expression) {
			return expression.kind == PEGExpression::Kind::REFERENCE && expression.text.GetString() == "SelectParens";
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
	auto grammar = ParsedGrammar::Parse("CursorRule <- 'first' / 'last'");
	grammar.AddChoice("CursorRule", "'second'", [](const PEGExpression &expression) {
		return expression.kind == PEGExpression::Kind::LITERAL && expression.text.GetString() == "first";
	});
	grammar.PrependChoice("CursorRule", "'third'", [](const PEGExpression &expression) {
		return expression.kind == PEGExpression::Kind::LITERAL && expression.text.GetString() == "last";
	});

	vector<string> choices;
	auto rule = grammar.GetRule("CursorRule");
	REQUIRE(rule);
	for (auto &expression : rule->recipe.expression.children) {
		if (expression.kind == PEGExpression::Kind::LITERAL) {
			choices.push_back(expression.text.GetString());
		}
	}
	REQUIRE(choices == vector<string> {"first", "second", "third", "last"});
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
