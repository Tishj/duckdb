#include "duckdb/parser/statement/create_statement.hpp"

namespace duckdb {

CreateStatement::CreateStatement() : SQLStatement(StatementType::CREATE_STATEMENT) {
}

CreateStatement::CreateStatement(const CreateStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> CreateStatement::Copy() const {
	return unique_ptr<CreateStatement>(new CreateStatement(*this));
}

bool CreateStatement::Equals(const SQLStatement *other_p) const {
	if (!other_p) {
		return false;
	}
	if (type != other_p->type) {
		return false;
	}
	auto other = (const CreateStatement &)*other_p;

	return other.info->Equals(info.get());
}

string CreateStatement::ToString() const {
	return info->ToString();
}

} // namespace duckdb
