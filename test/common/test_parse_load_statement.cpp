#include "catch.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/load_statement.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

static duckdb::unique_ptr<LoadInfo> ParseLoad(ClientContext &context, const string &query) {
	Parser parser(context);
	parser.ParseQuery(query);
	REQUIRE(parser.statements.size() == 1);
	REQUIRE(parser.statements[0]->type == StatementType::LOAD_STATEMENT);
	return parser.statements[0]->Cast<LoadStatement>().info->Copy();
}

TEST_CASE("Parse INSTALL / FORCE INSTALL statements", "[parse_load]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE(ParseLoad(*con.context, "INSTALL x")->load_type == LoadType::INSTALL);
	REQUIRE(ParseLoad(*con.context, "FORCE INSTALL x")->load_type == LoadType::FORCE_INSTALL);

	auto from_repo = ParseLoad(*con.context, "FORCE INSTALL x FROM 'some_repo'");
	REQUIRE(from_repo->load_type == LoadType::FORCE_INSTALL);
	REQUIRE(from_repo->repository == "some_repo");

	REQUIRE(ParseLoad(*con.context, "LOAD x")->load_type == LoadType::LOAD);
}
