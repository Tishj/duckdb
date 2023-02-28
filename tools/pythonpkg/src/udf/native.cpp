#include "duckdb_python/map.hpp"
#include "duckdb_python/vector_conversion.hpp"
#include "duckdb_python/array_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb_python/pyconnection.hpp"

namespace duckdb {

PythonNativeFunction::PythonNativeFunction() : PythonFunction(Bind, Function) {
}

// Convert from DataChunk to native python
static py::object ConvertInputToPython(PythonFunctionData &data, DataChunk &input) {
}

static void VerifyTypeIsCorrect(const py::handle &object) {
	// Verify that the object is of the expected type
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	if (!import_cache.pandas().DataFrame.IsInstance(object)) {
		throw InvalidInputException("The provided UDF should return a DataFrame!");
	}
}

// Convert back from python to duckdb DataChunk
static void ConvertPythonToChunk(const py::handle &object, DataChunk &out, PythonFunctionData &data) {
	VerifyTypeIsCorrect(object);
}

// Figure out which types are returned from the UDF
static void AnalyzeTypes(const py::handle &object, vector<LogicalType> &out_types, vector<string> &out_names) {
	VerifyTypeIsCorrect(object);
}

// Call the function without arguments to request the return types
unique_ptr<FunctionData> PythonNativeFunction::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto data_uptr = make_unique<PythonFunctionData>();
	auto &data = *data_uptr;
	data.function = (PyObject *)input.inputs[0].GetPointer();
	data.in_names = input.input_table_names;
	data.in_types = input.input_table_types;

	py::gil_scoped_acquire acquire;

	DataChunk empty_chunk;
	empty_chunk.SetCardinality(0);
	auto in_df = ConvertInputToPython(data, empty_chunk);
	auto out_df = FunctionCall(in_df, data.function);
	AnalyzeTypes(out_df, return_types, names);

	data.out_names = names;
	data.out_types = return_types;
	return std::move(data_uptr);
}

OperatorResultType PythonNativeFunction::Function(ExecutionContext &context, TableFunctionInput &data_p,
                                                  DataChunk &input, DataChunk &output) {
	py::gil_scoped_acquire acquire;

	if (input.size() == 0) {
		return OperatorResultType::NEED_MORE_INPUT;
	}

	auto &data = (PythonFunctionData &)*data_p.bind_data;

	auto in_df = ConvertInputToPython(data, input);
	auto out_df = FunctionCall(in_df, data.function);

	ConvertPythonToChunk(out_df, output, data);
	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
