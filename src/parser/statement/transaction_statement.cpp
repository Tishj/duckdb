#include "duckdb/parser/statement/transaction_statement.hpp"

namespace duckdb {

TransactionStatement::TransactionStatement(TransactionType type)
    : SQLStatement(StatementType::TRANSACTION_STATEMENT), info(make_uniq<TransactionInfo>(type)) {
}

TransactionStatement::TransactionStatement(const TransactionStatement &other)
    : SQLStatement(other), info(make_uniq<TransactionInfo>(other.info->type)) {
}

unique_ptr<SQLStatement> TransactionStatement::Copy() const {
	return unique_ptr<TransactionStatement>(new TransactionStatement(*this));
}

bool TransactionStatement::Equals(const SQLStatement *other_p) const {
	if (other_p->type != type) {
		return false;
	}
	auto &other = (const TransactionStatement &)*other_p;
	D_ASSERT(info);
	if (!info->Equals(*other.info)) {
		return false;
	}
	return true;
}

} // namespace duckdb
