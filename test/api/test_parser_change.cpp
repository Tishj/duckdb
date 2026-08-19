#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parser_change.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"

#include <condition_variable>
#include <thread>

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
		grammar.PrependChoice("SelectAtom", "ParserChangeTestAtom", [](const PEGToken &token) {
			return token.type == PEGTokenType::REFERENCE && token.text.GetString() == "SelectParens";
		});
	}
};

static void RegisterParserChangeTestSyntax(DBConfig &config) {
	ParserChange::Register(config, make_shared_ptr<AddParserChangeTestValue>());
	ParserChange::Register(config, make_shared_ptr<AddParserChangeTestAtom>());
}

static void CheckParserChangeTestSyntax(Connection &con) {
	auto result = con.Query("ANSWER");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0) == Value::INTEGER(42));
}

TEST_CASE("Parser changes apply in registration order", "[api][parser_change]") {
	DBConfig config;
	RegisterParserChangeTestSyntax(config);

	DuckDB db(nullptr, &config);
	Connection con(db);
	CheckParserChangeTestSyntax(con);
}

TEST_CASE("Grammar choices support cursor placement", "[api][parser_change]") {
	auto grammar = ParsedGrammar::Parse("CursorRule <- 'first' / 'last'");
	grammar.AddChoice("CursorRule", "'second'", [](const PEGToken &token) {
		return token.type == PEGTokenType::LITERAL && token.text.GetString() == "first";
	});
	grammar.PrependChoice("CursorRule", "'third'", [](const PEGToken &token) {
		return token.type == PEGTokenType::LITERAL && token.text.GetString() == "last";
	});

	vector<string> choices;
	auto rule = grammar.GetRule("CursorRule");
	REQUIRE(rule);
	for (auto &token : rule->recipe.tokens) {
		if (token.type == PEGTokenType::LITERAL) {
			choices.push_back(token.text.GetString());
		}
	}
	REQUIRE(choices == vector<string> {"first", "second", "third", "last"});
}

TEST_CASE("Parser changes invalidate an initialized parser cache", "[api][parser_change]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(*con.Query("SELECT 1"));

	auto &config = DBConfig::GetConfig(*db.instance);
	RegisterParserChangeTestSyntax(config);
	CheckParserChangeTestSyntax(con);
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
	DBConfig config;
	ParserChange::Register(config, make_shared_ptr<AddInvalidParserChangeTestRule>());
	REQUIRE_THROWS(DuckDB(nullptr, &config));
}

#ifndef DUCKDB_NO_THREADS
class BlockingParserChange final : public ParserChange {
public:
	BlockingParserChange() : ParserChange(ParserChangeType::GRAMMAR) {
	}

	void Apply(ParsedGrammar &) const override {
		unique_lock<mutex> guard(lock);
		if (apply_count++ > 0) {
			return;
		}
		entered = true;
		condition.notify_all();
		condition.wait(guard, [&]() { return released; });
	}

	void WaitUntilEntered() {
		unique_lock<mutex> guard(lock);
		condition.wait(guard, [&]() { return entered; });
	}

	void Release() {
		lock_guard<mutex> guard(lock);
		released = true;
		condition.notify_all();
	}

private:
	mutable mutex lock;
	mutable std::condition_variable condition;
	mutable idx_t apply_count = 0;
	mutable bool entered = false;
	mutable bool released = false;
};

TEST_CASE("Parser cache does not publish a stale concurrent build", "[api][parser_change]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(*con.Query("SELECT 1"));

	auto blocker = make_shared_ptr<BlockingParserChange>();
	ParserChange::Register(DBConfig::GetConfig(*db.instance), blocker);
	unique_ptr<MaterializedQueryResult> result;
	std::thread parser_thread([&]() { result = con.Query("ANSWER"); });

	blocker->WaitUntilEntered();
	RegisterParserChangeTestSyntax(DBConfig::GetConfig(*db.instance));
	blocker->Release();
	parser_thread.join();

	REQUIRE(result);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0) == Value::INTEGER(42));
}
#endif
