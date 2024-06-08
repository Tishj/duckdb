//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/insert_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"

namespace duckdb {

class InsertRelation : public Relation {
public:
	InsertRelation(shared_ptr<Relation> child, string schema_name, string table_name);
	InsertRelation(const shared_ptr<ClientContext> &context, unique_ptr<InsertStatement> lazy_insert);

public:
	shared_ptr<Relation> child;
	vector<ColumnDefinition> columns;
	unique_ptr<InsertStatement> base_statement;

public:
	BoundStatement Bind(Binder &binder) override;
	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	string ToString(idx_t depth) override;
	bool IsReadOnly() override {
		return false;
	}
};

} // namespace duckdb
