#include "duckdb/parser/statement/execute_statement.hpp"

namespace duckdb {

ExecuteStatement::ExecuteStatement() : SQLStatement(StatementType::EXECUTE_STATEMENT) {
}

ExecuteStatement::ExecuteStatement(const ExecuteStatement &other) : SQLStatement(other), name(other.name) {
	for (const auto &item : other.named_values) {
		named_values.emplace(std::make_pair(item.first, item.second->Copy()));
	}
}

unique_ptr<SQLStatement> ExecuteStatement::Copy() const {
	return unique_ptr<ExecuteStatement>(new ExecuteStatement(*this));
}

bool ExecuteStatement::Equals(const SQLStatement *other_p) const {
	if (type != other_p->type) {
		return false;
	}
	auto other = (const ExecuteStatement &)*other_p;
	if (other.name != name) {
		return false;
	}
	if (named_values.size() != other.named_values.size()) {
		return false;
	}
	auto it = named_values.begin();
	auto jt = other.named_values.begin();
	for (; it != named_values.end();) {
		if (it->first != jt->first) {
			return false;
		}
		if (it->second->Equals(*jt->second)) {
			return false;
		}
		it++;
		jt++;
	}
	return true;
}

} // namespace duckdb
