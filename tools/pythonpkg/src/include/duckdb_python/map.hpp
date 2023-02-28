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

struct PythonFunctionData : public TableFunctionData {
	PythonFunctionData() : function(nullptr) {
	}

	//! The user-supplied python UDF
	PyObject *function;
	//! The types of the datachunk given as input to the function
	vector<LogicalType> in_types;
	//! The types of the datachunk received as output of the function
	vector<LogicalType> out_types;

	//! The column names of the input datachunk
	vector<string> in_names;
	//! The column names of the output datachunk
	vector<string> out_names;
};

struct PythonFunction : public TableFunction {
public:
	PythonFunction(table_function_bind_t bind, table_in_out_function_t func);

public:
	static py::handle FunctionCall(const py::object &input, PyObject *function);
	static string TypeVectorToString(vector<LogicalType> &types);
};

struct PythonDFFunction : public PythonFunction {
public:
	PythonDFFunction();

public:
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
