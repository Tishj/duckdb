#include "duckdb/parser/statement/extension_statement.hpp"

namespace duckdb {

ExtensionStatement::ExtensionStatement(ParserExtension extension_p, unique_ptr<ParserExtensionParseData> parse_data_p)
    : SQLStatement(StatementType::EXTENSION_STATEMENT), extension(std::move(extension_p)),
      parse_data(std::move(parse_data_p)) {
}

unique_ptr<SQLStatement> ExtensionStatement::Copy() const {
	return make_uniq<ExtensionStatement>(extension, parse_data->Copy());
}

bool ExtensionStatement::Equals(const SQLStatement *other_p) const {
	if (type != other_p->type) {
		return false;
	}
	auto other = (ExtensionStatement *)other_p;
	(void)other;
	// FIXME: compare the ParserExtensionParseData
	// requires adding virtual method, which changes the API
	throw NotImplementedException("ParserExtensionParseData does not have an Equals method");
}

} // namespace duckdb
