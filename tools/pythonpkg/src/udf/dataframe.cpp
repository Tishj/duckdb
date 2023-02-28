#include "duckdb_python/map.hpp"
#include "duckdb_python/vector_conversion.hpp"
#include "duckdb_python/array_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb_python/pyconnection.hpp"

namespace duckdb {

PythonDFFunction::PythonDFFunction() : PythonFunction(Bind, Function) {
}

static py::object ConvertToDictionaryOfNumpyArrays(const vector<string> &names, NumpyResultConversion &conversion) {
	py::dict in_numpy_dict;
	for (idx_t col_idx = 0; col_idx < names.size(); col_idx++) {
		auto name = names[col_idx].c_str();
		in_numpy_dict[name] = conversion.ToArray(col_idx);
	}
	return in_numpy_dict;
}

static py::object ConvertToDataFrame(const vector<string> &names, NumpyResultConversion &conversion) {
	auto numpy_array_dict = ConvertToDictionaryOfNumpyArrays(names, conversion);
	auto in_df = py::module::import("pandas").attr("DataFrame").attr("from_dict")(numpy_array_dict);
	D_ASSERT(in_df.ptr());

	return in_df;
}

static py::object ConvertInputToDataFrame(PythonFunctionData &data, DataChunk &input) {
	NumpyResultConversion conversion(data.in_types, input.size());
	if (input.size() > 0) {
		conversion.Append(input);
	}
	// Convert the input to a Pandas DataFrame
	return ConvertToDataFrame(data.in_names, conversion);
}

static void VerifyTypeIsDataFrame(const py::handle &object) {
	// Verify that the object is of the expected type
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	if (!import_cache.pandas().DataFrame.IsInstance(object)) {
		string actual_type = py::str(object.get_type());
		throw InvalidInputException("The provided UDF should return a DataFrame, not '%s'!", actual_type);
	}
}

static void ConvertDataFrameToChunk(ExecutionContext &context, const py::handle &object, DataChunk &out,
                                    PythonFunctionData &data) {
	VerifyTypeIsDataFrame(object);

	vector<PandasColumnBindData> pandas_bind_data;
	vector<LogicalType> pandas_return_types;
	vector<string> pandas_names;

	VectorConversion::BindPandas(DBConfig::GetConfig(context.client), object, pandas_bind_data, pandas_return_types,
	                             pandas_names);
	if (pandas_return_types.size() != out.ColumnCount()) {
		throw InvalidInputException("Expected %llu columns from UDF, got %llu", out.ColumnCount(),
		                            pandas_return_types.size());
	}
	D_ASSERT(out.GetTypes() == data.out_types);
	if (pandas_return_types != out.GetTypes()) {
		throw InvalidInputException("UDF column type mismatch, expected [%s], got [%s]",
		                            PythonFunction::TypeVectorToString(data.out_types),
		                            PythonFunction::TypeVectorToString(pandas_return_types));
	}
	if (pandas_names != data.out_names) {
		throw InvalidInputException("UDF column name mismatch, expected [%s], got [%s]",
		                            StringUtil::Join(data.out_names, ", "), StringUtil::Join(pandas_names, ", "));
	}

	auto df_columns = py::list(object.attr("columns"));
	auto get_fun = object.attr("__getitem__");

	idx_t row_count = py::len(get_fun(df_columns[0]));
	if (row_count > STANDARD_VECTOR_SIZE) {
		throw InvalidInputException("UDF returned more than %llu rows, which is not allowed.", STANDARD_VECTOR_SIZE);
	}

	for (idx_t col_idx = 0; col_idx < out.ColumnCount(); col_idx++) {
		auto &bind_data = pandas_bind_data[col_idx];
		auto &numpy_array = bind_data.numpy_col;
		auto &out_vector = out.data[col_idx];

		VectorConversion::NumpyToDuckDB(bind_data, numpy_array, row_count, 0, out_vector);
	}
	out.SetCardinality(row_count);
}

static void AnalyzeTypes(ClientContext &context, const py::handle &object, vector<LogicalType> &out_types,
                         vector<string> &out_names) {
	VerifyTypeIsDataFrame(object);

	vector<PandasColumnBindData> pandas_bind_data; // unused
	VectorConversion::BindPandas(DBConfig::GetConfig(context), object, pandas_bind_data, out_types, out_names);
}

// we call the passed function with a zero-row data frame to infer the output columns and their names.
// they better not change in the actual execution ^^
unique_ptr<FunctionData> PythonDFFunction::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto data_uptr = make_unique<PythonFunctionData>();
	auto &data = *data_uptr;
	data.function = (PyObject *)input.inputs[0].GetPointer();
	data.in_names = input.input_table_names;
	data.in_types = input.input_table_types;

	py::gil_scoped_acquire acquire;

	DataChunk empty_chunk;
	empty_chunk.SetCardinality(0);
	auto in_df = ConvertInputToDataFrame(data, empty_chunk);
	auto out_df = FunctionCall(in_df, data.function);
	AnalyzeTypes(context, out_df, return_types, names);

	data.out_names = names;
	data.out_types = return_types;
	return std::move(data_uptr);
}

OperatorResultType PythonDFFunction::Function(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                              DataChunk &output) {
	py::gil_scoped_acquire acquire;

	if (input.size() == 0) {
		return OperatorResultType::NEED_MORE_INPUT;
	}

	auto &data = (PythonFunctionData &)*data_p.bind_data;

	auto in_df = ConvertInputToDataFrame(data, input);
	auto out_df = FunctionCall(in_df, data.function);

	ConvertDataFrameToChunk(context, out_df, output, data);
	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
