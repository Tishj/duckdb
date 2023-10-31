//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/table_description.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/column_definition.hpp"

namespace duckdb {

struct TableDescription {
public:
	TableDescription() {
	}
	TableDescription(const string &schema, const string &table, vector<ColumnDefinition> columns)
	    : schema(schema), table(table), columns(std::move(columns)) {
	}

public:
	//! The schema of the table
	string schema;
	//! The table name of the table
	string table;
	//! The columns of the table
	vector<ColumnDefinition> columns;
};

} // namespace duckdb
