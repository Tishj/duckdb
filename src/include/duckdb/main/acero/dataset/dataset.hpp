#pragma once

#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/acero/util/arrow_test_factory.hpp"
#include "duckdb/main/acero/schema.hpp"

namespace duckdb {
namespace arrow {
namespace dataset {

// This wraps a C-data interface arrow array
struct Dataset {
public:
	Dataset(unique_ptr<DuckDB> db, unique_ptr<Connection> conn, const string &query);

public:
	uintptr_t ArrowObject();

public:
	unique_ptr<ac::ArrowTestFactory> factory;
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> conn;
	shared_ptr<arrow::Schema> schema;
};

} // namespace dataset
} // namespace arrow
} // namespace duckdb
