//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pandas_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/execution/execution_context.hpp"

namespace duckdb {

struct PythonFunction : public TableFunction {
public:
	PythonFunction(table_function_bind_t bind, table_in_out_function_t func);
};

struct PythonDFFunction : public PythonFunction {
public:
	PythonDFFunction();
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);

	static OperatorResultType Function(ExecutionContext &context, TableFunctionInput &data, DataChunk &input,
	                                   DataChunk &output);
};

struct PythonNativeFunction : public PythonFunction {
public:
	PythonNativeFunction();
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);

	static OperatorResultType Function(ExecutionContext &context, TableFunctionInput &data, DataChunk &input,
	                                   DataChunk &output);
};

} // namespace duckdb
