#include "catch.hpp"
#include "test_helpers.hpp"
#include <iostream>
#include <thread>

using namespace duckdb;
using namespace std;

static void SelectTable(Connection con) {
    idx_t i = 0;
    while (true) {
        auto prepare = con.Prepare("select * from foo");
        REQUIRE_NO_FAIL(prepare->Execute());
        std::cout << "select " << i++ << std::endl;
    }
}

static void RecreateTable(Connection con) {
    idx_t i = 0;
    while (true) {
        auto prepare = con.Prepare("create or replace table foo as select * from foo");
        REQUIRE_NO_FAIL(prepare->Execute());
        std::cout << "recreate " << i++ << std::endl;
    }
}

TEST_CASE("Test concurrent prepared", "[api]") {
    duckdb::unique_ptr<QueryResult> result;
    DuckDB db(nullptr);
    Connection con(db);
    con.EnableQueryVerification();

    REQUIRE_NO_FAIL(con.Query("create table foo as select unnest(generate_series(1, 10));"));

    Connection select_conn(db);
    Connection recreate_conn(db);

    std::thread select_function(SelectTable, std::move(select_conn));
    std::thread recreate_function(RecreateTable, std::move(recreate_conn));

    select_function.join();
    recreate_function.join();
}
