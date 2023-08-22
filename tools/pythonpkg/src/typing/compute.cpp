#include "duckdb_python/compute.hpp"
#include "duckdb_python/pytype.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/import_cache/python_import_cache.hpp"
#include "duckdb/common/printer.hpp"

#include "duckdb/main/query_result.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb_python/pandas/pandas_scan.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb_python/arrow/arrow_array_stream.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb_python/numpy/numpy_scan.hpp"
#include "duckdb_python/arrow/arrow_export_utils.hpp"
#include "duckdb/common/types/arrow_aux_data.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parser.hpp"

#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression.hpp"

#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/common/enums/pending_execution_result.hpp"

#include "duckdb/main/client_properties.hpp"

namespace duckdb {

void DuckDBPyCompute::Initialize(py::module_ &parent) {
	auto m = parent.def_submodule("compute", "This module contains classes and methods implementing the PyArrow Compute module");
	m.def("sort_indices", DuckDBPyCompute::SortIndices, R"(
		Return the indices that would sort an array, record batch or table.

		This function computes an array of indices that define a stable sort
		of the input array, record batch or table.  By default, nNull values are
		considered greater than any other value and are therefore sorted at the
		end of the input. For floating-point types, NaNs are considered greater
		than any other non-null value, but smaller than null values.

		The handling of nulls and NaNs can be changed in SortOptions.

		Parameters
		----------
		input : Array-like or scalar-like
			Argument to compute function.
		sort_keys : sequence of (name, order) tuples
			Names of field/column keys to sort the input on,
			along with the order each field/column is sorted in.
			Accepted values for `order` are "ascending", "descending".
		null_placement : str, default "at_end"
			Where nulls in input should be sorted, only applying to
			columns/fields mentioned in `sort_keys`.
			Accepted values are "at_start", "at_end".
		options : pyarrow.compute.SortOptions, optional
			Alternative way of passing options.
		memory_pool : pyarrow.MemoryPool, optional
			If not passed, will allocate memory from the default memory pool.
	)",
		py::arg("input"),
		py::arg("sort_keys") = py::none(),
		py::kw_only(),
		py::arg("null_placement") = "at_end",
		py::arg("options") = py::none(),
		py::arg("memory_pool") = py::none()
	);
}

static py::list ConvertToSingleBatch(vector<LogicalType> &types, vector<string> &names, DataChunk &input,
                                     const ClientProperties &options) {
	ArrowSchema schema;
	ArrowConverter::ToArrowSchema(&schema, types, names, options);

	py::list single_batch;
	ArrowAppender appender(types, STANDARD_VECTOR_SIZE, options);
	appender.Append(input, 0, input.size(), input.size());
	auto array = appender.Finalize();
	TransformDuckToArrowChunk(schema, array, single_batch);
	return single_batch;
}

static py::object ConvertDataChunkToPyArrowTable(DataChunk &input, const ClientProperties &options) {
	auto types = input.GetTypes();
	vector<string> names;
	names.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		names.push_back(StringUtil::Format("c%d", i));
	}

	return pyarrow::ToArrowTable(types, names, ConvertToSingleBatch(types, names, input, options), options);
}

static void ConvertPyArrowToDataChunk(const py::object &table, DataChunk &result, ClientContext &context, idx_t count) {

	// Create the stream factory from the Table object
	auto stream_factory = make_uniq<PythonTableArrowArrayStreamFactory>(table.ptr(), context.GetClientProperties());
	auto stream_factory_produce = PythonTableArrowArrayStreamFactory::Produce;
	auto stream_factory_get_schema = PythonTableArrowArrayStreamFactory::GetSchema;

	// Get the functions we need
	auto function = ArrowTableFunction::ArrowScanFunction;
	auto bind = ArrowTableFunction::ArrowScanBind;
	auto init_global = ArrowTableFunction::ArrowScanInitGlobal;
	auto init_local = ArrowTableFunction::ArrowScanInitLocalInternal;

	// Prepare the inputs for the bind
	vector<Value> children;
	children.reserve(3);
	children.push_back(Value::POINTER(CastPointerToValue(stream_factory.get())));
	children.push_back(Value::POINTER(CastPointerToValue(stream_factory_produce)));
	children.push_back(Value::POINTER(CastPointerToValue(stream_factory_get_schema)));
	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<string> input_names;

	auto bind_input = TableFunctionBindInput(children, named_params, input_types, input_names, nullptr);
	vector<LogicalType> return_types;
	vector<string> return_names;

	auto bind_data = bind(context, bind_input, return_types, return_names);

	if (return_types.size() != 1) {
		throw InvalidInputException(
		    "The returned table from a pyarrow scalar udf should only contain one column, found %d",
		    return_types.size());
	}
	// if (return_types[0] != out.GetType()) {
	//	throw InvalidInputException("The type of the returned array (%s) does not match the expected type: '%s'", )
	//}

	// Reserve for STANDARD_VECTOR_SIZE instead of count, in case the returned table contains too many tuples
	result.Initialize(context, return_types, STANDARD_VECTOR_SIZE);

	vector<column_t> column_ids = {0};
	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	auto global_state = init_global(context, input);
	auto local_state = init_local(context, input, global_state.get());

	TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
	function(context, function_input, result);
	if (result.size() != count) {
		throw InvalidInputException("Returned pyarrow table should have %d tuples, found %d", count, result.size());
	}
}

static OrderByNullType GetNullOrder(const string &null_order) {
	if (StringUtil::CIEquals(null_order, "at_end")) {
		return OrderByNullType::NULLS_LAST;
	} else if (StringUtil::CIEquals(null_order, "at_start")) {
		return OrderByNullType::NULLS_FIRST;
	} else {
		throw InvalidInputException("Please provide 'null_placement' as 'at_end' or 'at_start'");
	}
}

static OrderType GetOrderType(const py::object &order_p) {
	if (!py::isinstance<py::str>(order_p)) {
		throw InvalidInputException("'order' has to be provided as a string");
	}
	string order = py::str(order_p);
	auto order_l = StringUtil::Lower(order);

	if (order_l == "ascending") {
		return OrderType::ASCENDING;
	} else if (order_l == "descending") {
		return OrderType::DESCENDING;
	} else {
		throw InvalidInputException("Only 'ascending' or 'descending' are allowed options");
	}
}

py::object DuckDBPyCompute::SortIndices(const py::object &array, py::object &sort_keys_p, const string &null_placement, const py::object &options, const py::object &memory_pool) {
	if (!py::none().is(options)) {
		throw NotImplementedException("Explicit 'sort_options' are not supported yet");
	}
	if (!py::none().is(memory_pool)) {
		throw NotImplementedException("Explicit 'memory_pool' is not supported yet");
	}
	if (py::none().is(sort_keys_p)) {
		// Insert default order
		auto sort_keys = py::list(1);
		sort_keys[0] = py::make_tuple("dummy", "ascending");
		sort_keys_p = sort_keys;
	}

	if (!py::isinstance<py::list>(sort_keys_p)) {
		throw InvalidInputException("sort keys has to be provided as a list of tuples");
	}
	auto sort_keys = py::list(sort_keys_p);

	auto array_class = py::module::import("pyarrow").attr("lib").attr("Array");
	if (!py::isinstance(array, array_class)) {
		throw InvalidInputException("Please provide a pyarrow Array");
	}

	// ----------- Convert the arrow array to a DataChunk -----------
	py::list single_array(1);
	py::list single_name(1);

	single_array[0] = array;
	single_name[0] = "c0";
	auto python_object = py::module_::import("pyarrow").attr("lib").attr("Table").attr("from_arrays")(
		single_array, py::arg("names") = single_name);

	auto count = py::len(array);
	// FIXME: we require the ClientContext to convert pyarrow to DataChunk.
	auto &context = *DuckDBPyConnection::DefaultConnection()->connection->context;
	DataChunk intermediate;
	ConvertPyArrowToDataChunk(python_object, intermediate, context, count);

	// ----------- Add the indices array to the input -----------

	vector<LogicalType> types;
	auto intermediate_types = intermediate.GetTypes();
	types.assign(intermediate_types.begin(), intermediate_types.end());
	types.push_back(LogicalType::INTEGER);

	DataChunk input_chunk;
	input_chunk.Initialize(context, types, count);
	input_chunk.data[0].Reference(intermediate.data[0]);
	input_chunk.data[1].Sequence(0, 1, count);
	input_chunk.SetCardinality(count);

	// ----------- Create the order by nodes -----------

	vector<BoundOrderByNode> orders;
	for (auto &key_p : sort_keys) {
		if (!py::isinstance<py::tuple>(key_p)) {
			throw InvalidInputException("Elements of the sort_keys should be tuples");
		}
		auto key = py::cast<py::tuple>(key_p);
		if (key.size() != 2) {
			throw InvalidInputException("All provided tuples should have 2 elements");
		}
		auto name = key[0];
		auto order = key[1];

		OrderType order_type = GetOrderType(order);
		// default value
		OrderByNullType null_order = GetNullOrder(null_placement);
		// name is ignored for 'sort_indices', just create a BoundReferenceExpression
		auto expr = make_uniq_base<Expression, BoundReferenceExpression>(types[0], 0);
		orders.emplace_back(order_type, null_order, std::move(expr));
	}

	// ----------- Create a way for us to feed the data into the order operator -----------

	// Feed the chunk to a ColumnDataCollection
	auto data_collection = make_uniq<ColumnDataCollection>(context, types);
	ColumnDataAppendState append_state;
	data_collection->InitializeAppend(append_state);
	data_collection->Append(append_state, input_chunk);

	// Create a PhysicalColumnDataScan operator
	auto physical_scan = make_uniq<PhysicalColumnDataScan>(types, PhysicalOperatorType::COLUMN_DATA_SCAN, count, std::move(data_collection));

	// ----------- Create the Order operator -----------

	// Create the PhysicalOrder which will execute the sort
	vector<idx_t> projections = {0, 1};
	PhysicalOrder order(std::move(types), std::move(orders), projections, count);

	order.children.push_back(std::move(physical_scan));

	// ----------- Execute the sort -----------

	Executor executor(context);
	executor.Initialize(order);

	auto execution_result = PendingExecutionResult::RESULT_NOT_READY;
	while (execution_result != PendingExecutionResult::RESULT_READY) {
		execution_result = executor.ExecuteTask();
	}

	auto result = executor.FetchChunk();

	auto client_properties = context.GetClientProperties();
	vector<string> names = {"c0", "c1"};
	types = result->GetTypes();
	auto record_batch_list = ConvertToSingleBatch(types, names, *result, client_properties);
	return record_batch_list[0].attr("__getitem__")("c1");
}

} // namespace duckdb
