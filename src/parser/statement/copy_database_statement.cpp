#include "duckdb/parser/statement/copy_database_statement.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

CopyDatabaseStatement::CopyDatabaseStatement(string from_database_p, string to_database_p, CopyDatabaseType copy_type)
    : SQLStatement(StatementType::COPY_DATABASE_STATEMENT), from_database(std::move(from_database_p)),
      to_database(std::move(to_database_p)), copy_type(copy_type) {
}

CopyDatabaseStatement::CopyDatabaseStatement(const CopyDatabaseStatement &other)
    : SQLStatement(other), from_database(other.from_database), to_database(other.to_database),
      copy_type(other.copy_type) {
}

unique_ptr<SQLStatement> CopyDatabaseStatement::Copy() const {
	return unique_ptr<CopyDatabaseStatement>(new CopyDatabaseStatement(*this));
}

bool CopyDatabaseStatement::Equals(const SQLStatement *other_p) const {
	if (type != other_p->type) {
		return false;
	}
	auto other = (const CopyDatabaseStatement &)*other_p;

	if (other.from_database != from_database) {
		return false;
	}
	if (other.to_database != to_database) {
		return false;
	}
	if (other.copy_type != copy_type) {
		return false;
	}
	return true;
}

string CopyDatabaseStatement::ToString() const {
	string result;
	result = "COPY FROM DATABASE ";
	result += KeywordHelper::WriteOptionallyQuoted(from_database);
	result += " TO ";
	result += KeywordHelper::WriteOptionallyQuoted(to_database);
	result += " (";
	switch (copy_type) {
	case CopyDatabaseType::COPY_DATA:
		result += "DATA";
		break;
	case CopyDatabaseType::COPY_SCHEMA:
		result += "SCHEMA";
		break;
	default:
		throw InternalException("Unsupported CopyDatabaseType");
	}
	result += ")";
	return result;
}

} // namespace duckdb
