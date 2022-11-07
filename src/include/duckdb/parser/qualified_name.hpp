//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/qualified_name.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct QualifiedName {
	string schema;
	string name;

	static QualifiedName FromList(const Value &list_val) {
		D_ASSERT(list_val.type().id() == LogicalTypeId::LIST);
		auto &list = ListValue::GetChildren(list_val);
#ifdef DEBUG
		for (auto &item : list) {
			D_ASSERT(item.type().id() == LogicalTypeId::VARCHAR);
		}
#endif

		QualifiedName qualified_name;
		if (list.size() <= 1) {
			qualified_name.schema = "";
			qualified_name.name = list.empty() ? "" : list[0].GetValue<string>();
			return qualified_name;
		}
		// First value is the schema
		qualified_name.schema = list[0].GetValue<string>();
		string name;
		for (idx_t i = 1; i < list.size(); i++) {
			name += list[i].GetValue<string>();
			if (i + 1 >= list.size()) {
				break;
			}
			name += ".";
		}
		qualified_name.name = name;
		return qualified_name;
	}

	//! Parse the (optional) schema and a name from a string in the format of e.g. "schema"."table"; if there is no dot
	//! the schema will be set to INVALID_SCHEMA
	static QualifiedName Parse(const string &input) {
		string schema;
		string name;
		idx_t idx = 0;
		vector<string> entries;
		string entry;
	normal:
		//! quote
		for (; idx < input.size(); idx++) {
			if (input[idx] == '"') {
				idx++;
				goto quoted;
			} else if (input[idx] == '.') {
				goto separator;
			}
			entry += input[idx];
		}
		goto end;
	separator:
		entries.push_back(entry);
		entry = "";
		idx++;
		goto normal;
	quoted:
		//! look for another quote
		for (; idx < input.size(); idx++) {
			if (input[idx] == '"') {
				//! unquote
				idx++;
				goto normal;
			}
			entry += input[idx];
		}
		throw ParserException("Unterminated quote in qualified name!");
	end:
		if (entries.empty()) {
			schema = INVALID_SCHEMA;
			name = entry;
		} else if (entries.size() == 1) {
			schema = entries[0];
			name = entry;
		} else {
			throw ParserException("Expected schema.entry or entry: too many entries found");
		}
		return QualifiedName {schema, name};
	}
};

struct QualifiedColumnName {
	QualifiedColumnName() {
	}
	QualifiedColumnName(string table_p, string column_p) : table(move(table_p)), column(move(column_p)) {
	}

	string schema;
	string table;
	string column;
};

} // namespace duckdb
