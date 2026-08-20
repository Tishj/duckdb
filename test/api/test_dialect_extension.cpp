#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/parser/peg/dialect_extension.hpp"
#include "duckdb/parser/peg/matcher.hpp"

using namespace duckdb;

TEST_CASE("Register and select a dialect extension", "[api][dialect_extension]") {
	DBConfig config;
	DialectExtension::Register(config, DialectExtension("test"));

	DuckDB db(nullptr, &config);
	Connection con(db);

	auto dialects = con.Query("SELECT dialect_name FROM duckdb_dialects() ORDER BY dialect_name");
	REQUIRE_NO_FAIL(*dialects);
	REQUIRE(dialects->RowCount() == 2);
	REQUIRE(dialects->GetValue(0, 0) == Value("duckdb"));
	REQUIRE(dialects->GetValue(0, 1) == Value("test"));

	auto &parser_cache = db.instance->GetParserCache();
	auto old_matcher = parser_cache.GetMatcher();
	auto old_transformer = parser_cache.GetTransformerFactory();
	REQUIRE_NO_FAIL(con.Query("SET current_dialect = 'test'"));
	auto new_matcher = parser_cache.GetMatcher();
	auto new_transformer = parser_cache.GetTransformerFactory();
	REQUIRE(old_matcher != new_matcher);
	REQUIRE(old_transformer != new_transformer);
}
