#pragma once

#include "duckdb_python/python_context_state.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

class DuckDBPyRelation;

class PythonSubqueryRef : public SubqueryRef {
public:
	PythonSubqueryRef(unique_ptr<SelectStatement> subquery, string name, PythonContextState &state,
	                  shared_ptr<DuckDBPyRelation> relation, string alias = string());
	~PythonSubqueryRef() override;

public:
	void BindBegin() override;
	void BindEnd() override;

public:
	PythonContextState &state;
	string name;
	shared_ptr<DuckDBPyRelation> relation;
};

struct PythonReplacementScan {
public:
	static unique_ptr<TableRef> Replace(ClientContext &context, const string &table_name, ReplacementScanData *data);
};

} // namespace duckdb
