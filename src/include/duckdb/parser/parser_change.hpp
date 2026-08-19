//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parser_change.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

class ParsedGrammar;
struct DBConfig;

enum class ParserChangeType : uint8_t { GRAMMAR };

//! A database-wide change applied while constructing the PEG parser.
class ParserChange {
public:
	explicit ParserChange(ParserChangeType type_p) : type(type_p) {
	}
	virtual ~ParserChange() {
	}

	virtual void Apply(ParsedGrammar &grammar) const = 0;

	DUCKDB_API static void Register(DBConfig &config, shared_ptr<ParserChange> change);

public:
	const ParserChangeType type;
};

} // namespace duckdb
