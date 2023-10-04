#pragma once

#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/acero/util/arrow_test_factory.hpp"

namespace duckdb {
namespace arrow {
namespace dataset {

using Schema = ArrowSchemaWrapper;

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
	shared_ptr<Schema> schema;
};

} // namespace dataset
} // namespace arrow
} // namespace duckdb
