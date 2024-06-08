#include "duckdb/main/relation/insert_relation.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

InsertRelation::InsertRelation(shared_ptr<Relation> child_p, string schema_name, string table_name)
    : Relation(child_p->context, RelationType::INSERT_RELATION), child(std::move(child_p)) {
	base_statement = make_uniq<InsertStatement>();
	base_statement->schema = schema_name;
	base_statement->table = table_name;
	context.GetContext()->TryBindRelation(*this, this->columns);
}

InsertRelation::InsertRelation(const shared_ptr<ClientContext> &context, unique_ptr<InsertStatement> insert)
    : Relation(context, RelationType::INSERT_RELATION), child(nullptr) {
	base_statement = std::move(insert);
	context->TryBindRelation(*this, this->columns);
}

BoundStatement InsertRelation::Bind(Binder &binder) {
	auto insert = unique_ptr_cast<SQLStatement, InsertStatement>(base_statement->Copy());
	if (child) {
		// This insert relation was made from an existing relation
		auto select = make_uniq<SelectStatement>();
		select->node = child->GetQueryNode();
		insert->select_statement = std::move(select);
	}
	return binder.Bind(insert->Cast<SQLStatement>());
}

unique_ptr<QueryNode> InsertRelation::GetQueryNode() {
	throw NotImplementedException("Can't select from an INSERT Relation");
}

const vector<ColumnDefinition> &InsertRelation::Columns() {
	return columns;
}

string InsertRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Insert\n";
	if (child) {
		str += child->ToString(depth + 1);
	}
	return str;
}

} // namespace duckdb
