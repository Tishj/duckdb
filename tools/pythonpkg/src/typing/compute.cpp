//#include "duckdb_python/compute.hpp"
//#include "duckdb_python/pytype.hpp"
//#include "duckdb_python/pyconnection/pyconnection.hpp"
//#include "duckdb_python/import_cache/python_import_cache.hpp"
//#include "duckdb/common/printer.hpp"

//#include "duckdb/main/query_result.hpp"
//#include "duckdb_python/pybind11/pybind_wrapper.hpp"
//#include "duckdb/function/scalar_function.hpp"
//#include "duckdb_python/pandas/pandas_scan.hpp"
//#include "duckdb/common/arrow/arrow.hpp"
//#include "duckdb/common/arrow/arrow_converter.hpp"
//#include "duckdb/common/arrow/arrow_wrapper.hpp"
//#include "duckdb/common/arrow/arrow_appender.hpp"
//#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
//#include "duckdb_python/arrow/arrow_array_stream.hpp"
//#include "duckdb/function/table/arrow.hpp"
//#include "duckdb/function/function.hpp"
//#include "duckdb_python/numpy/numpy_scan.hpp"
//#include "duckdb_python/arrow/arrow_export_utils.hpp"
//#include "duckdb/common/types/arrow_aux_data.hpp"

//namespace duckdb {

//void DuckDBPyCompute::Initialize(py::module_ &parent) {
//	auto m = parent.def_submodule("compute", "This module contains classes and methods implementing the PyArrow Compute module");
//	m.def("sort_indices", DuckDBPyCompute::SortIndices, R"(
//		Return the indices that would sort an array, record batch or table.

//		This function computes an array of indices that define a stable sort
//		of the input array, record batch or table.  By default, nNull values are
//		considered greater than any other value and are therefore sorted at the
//		end of the input. For floating-point types, NaNs are considered greater
//		than any other non-null value, but smaller than null values.

//		The handling of nulls and NaNs can be changed in SortOptions.

//		Parameters
//		----------
//		input : Array-like or scalar-like
//			Argument to compute function.
//		sort_keys : sequence of (name, order) tuples
//			Names of field/column keys to sort the input on,
//			along with the order each field/column is sorted in.
//			Accepted values for `order` are "ascending", "descending".
//		null_placement : str, default "at_end"
//			Where nulls in input should be sorted, only applying to
//			columns/fields mentioned in `sort_keys`.
//			Accepted values are "at_start", "at_end".
//		options : pyarrow.compute.SortOptions, optional
//			Alternative way of passing options.
//		memory_pool : pyarrow.MemoryPool, optional
//			If not passed, will allocate memory from the default memory pool.
//	)",
//		py::arg("input"),
//		py::arg("sort_keys") = py::tuple(),
//		py::kw_only(),
//		py::arg("null_placement") = "at_end",
//		py::arg("options") = py::none(),
//		py::arg("memory_pool") = py::none()
//	);
//}

//static py::list ConvertToSingleBatch(vector<LogicalType> &types, vector<string> &names, DataChunk &input,
//                                     const ClientProperties &options) {
//	ArrowSchema schema;
//	ArrowConverter::ToArrowSchema(&schema, types, names, options);

//	py::list single_batch;
//	ArrowAppender appender(types, STANDARD_VECTOR_SIZE, options);
//	appender.Append(input, 0, input.size(), input.size());
//	auto array = appender.Finalize();
//	TransformDuckToArrowChunk(schema, array, single_batch);
//	return single_batch;
//}

//static py::object ConvertDataChunkToPyArrowTable(DataChunk &input, const ClientProperties &options) {
//	auto types = input.GetTypes();
//	vector<string> names;
//	names.reserve(types.size());
//	for (idx_t i = 0; i < types.size(); i++) {
//		names.push_back(StringUtil::Format("c%d", i));
//	}

//	return pyarrow::ToArrowTable(types, names, ConvertToSingleBatch(types, names, input, options), options);
//}

//static void ConvertPyArrowToDataChunk(const py::object &table, Vector &out, ClientContext &context, idx_t count) {

//	// Create the stream factory from the Table object
//	auto stream_factory = make_uniq<PythonTableArrowArrayStreamFactory>(table.ptr(), context.GetClientProperties());
//	auto stream_factory_produce = PythonTableArrowArrayStreamFactory::Produce;
//	auto stream_factory_get_schema = PythonTableArrowArrayStreamFactory::GetSchema;

//	// Get the functions we need
//	auto function = ArrowTableFunction::ArrowScanFunction;
//	auto bind = ArrowTableFunction::ArrowScanBind;
//	auto init_global = ArrowTableFunction::ArrowScanInitGlobal;
//	auto init_local = ArrowTableFunction::ArrowScanInitLocalInternal;

//	// Prepare the inputs for the bind
//	vector<Value> children;
//	children.reserve(3);
//	children.push_back(Value::POINTER(CastPointerToValue(stream_factory.get())));
//	children.push_back(Value::POINTER(CastPointerToValue(stream_factory_produce)));
//	children.push_back(Value::POINTER(CastPointerToValue(stream_factory_get_schema)));
//	named_parameter_map_t named_params;
//	vector<LogicalType> input_types;
//	vector<string> input_names;

//	auto bind_input = TableFunctionBindInput(children, named_params, input_types, input_names, nullptr);
//	vector<LogicalType> return_types;
//	vector<string> return_names;

//	auto bind_data = bind(context, bind_input, return_types, return_names);

//	if (return_types.size() != 1) {
//		throw InvalidInputException(
//		    "The returned table from a pyarrow scalar udf should only contain one column, found %d",
//		    return_types.size());
//	}

//	DataChunk result;
//	// Reserve for STANDARD_VECTOR_SIZE instead of count, in case the returned table contains too many tuples
//	result.Initialize(context, return_types, STANDARD_VECTOR_SIZE);

//	vector<column_t> column_ids = {0};
//	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
//	auto global_state = init_global(context, input);
//	auto local_state = init_local(context, input, global_state.get());

//	TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
//	function(context, function_input, result);
//	if (result.size() != count) {
//		throw InvalidInputException("Returned pyarrow table should have %d tuples, found %d", count, result.size());
//	}

//	VectorOperations::Cast(context, result.data[0], out, count);
//}

//py::object DuckDBPyCompute::SortIndices(const py::object &array, const py::object &sort_keys, const string &null_placement, const py::object &options, const py::object &memory_pool) {
//	if (!py::none().is(options)) {
//		throw NotImplementedException("Explicit 'sort_options' are not supported yet");
//	}
//	if (!py::none().is(memory_pool)) {
//		throw NotImplementedException("Explicit 'memory_pool' is not supported yet");
//	}

//	auto array_class = py::module::import("pyarrow").attr("lib").attr("Array");

//	if (!py::isinstance(array, array_class)) {
//		throw InvalidInputException("Please provide a pyarrow Array");
//	}

//	// ----------- Convert the arrow array to a DataChunk -----------
//	py::list single_array(1);
//	py::list single_name(1);


//	single_array[0] = array;
//	single_name[0] = "c0";
//	auto python_object = py::module_::import("pyarrow").attr("lib").attr("Table").attr("from_arrays")(
//		single_array, py::arg("names") = single_name);

//	auto count = py::len(array);
//	DataChunk input_chunk;

//	// FIXME: we require the ClientContext to convert pyarrow to DataChunk.
//	// I think this is only so we can use the buffer manager and allocator, so it could potentially be faked?
//	DBInstance
//	ClientContext context;
//	DatabaseInstance::

//	ConvertPyArrowToDataChunk(python_object, input_chunk, state.GetContext(), count);

//	// ----------- Create the inputs to the PhysicalOrder operator -----------

//	return py::none();
//}

//} // namespace duckdb
