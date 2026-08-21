//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/token_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

enum class TokenType {
	INVALID,
	KEYWORD,
	STRING_LITERAL,
	NUMBER_LITERAL,
	OPERATOR,
	IDENTIFIER,
	COMMENT,
	TERMINATOR,
	CATALOG_NAME,
	SCHEMA_NAME,
	TABLE_NAME,
	TYPE_NAME,
	COLUMN_NAME,
	SCALAR_FUNCTION,
	TABLE_FUNCTION,
	PRAGMA_FUNCTION,
	SETTING_NAME,
	//! Named TOKEN_ERROR (not ERROR) to avoid colliding with the Win32 `ERROR` macro.
	TOKEN_ERROR
};

} // namespace duckdb
