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
#include "duckdb/main/prepared_statement_data.hpp"
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
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/main/chunk_scan_state/query_result.hpp"

#include "duckdb/main/client_properties.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"

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

static unique_ptr<FunctionData> ConvertPyArrowToDataChunk(PythonTableArrowArrayStreamFactory &stream_factory, const py::object &table, ClientContext &context) {

	// Create the stream factory from the Table object
	auto stream_factory_produce = PythonTableArrowArrayStreamFactory::Produce;
	auto stream_factory_get_schema = PythonTableArrowArrayStreamFactory::GetSchema;

	// Get the functions we need
	auto bind = ArrowTableFunction::ArrowScanBind;

	// Prepare the inputs for the bind
	vector<Value> children;
	children.reserve(3);
	children.push_back(Value::POINTER(CastPointerToValue(&stream_factory)));
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
	return bind_data;
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

static LogicalType GetTypeFromArrow(const py::object &array) {
	auto obj_schema = array.attr("type");
	auto export_to_c = obj_schema.attr("_export_to_c");

	ArrowSchema schema;
	export_to_c(reinterpret_cast<uint64_t>(&schema));

	auto arrow_type = ArrowTableFunction::GetArrowLogicalType(schema);
	return arrow_type->GetDuckType();
}

unique_ptr<ColumnDataCollection> ConvertToColumnDataCollection(const vector<LogicalType> &types, ClientContext &context, const py::object &array) {
	py::list single_array(1);
	py::list single_name(1);

	auto data_collection = make_uniq<ColumnDataCollection>(context, types);
	ColumnDataAppendState append_state;
	data_collection->InitializeAppend(append_state);

	// Create a table we can scan

	single_array[0] = array;
	single_name[0] = "c0";
	auto python_object = py::module_::import("pyarrow").attr("lib").attr("Table").attr("from_arrays")(
		single_array, py::arg("names") = single_name);

	auto count = py::len(array);
	auto stream_factory = make_uniq<PythonTableArrowArrayStreamFactory>(python_object.ptr(), context.GetClientProperties());
	auto bind_data = ConvertPyArrowToDataChunk(*stream_factory, python_object, context);

	DataChunk input_chunk;
	input_chunk.Initialize(context, types, STANDARD_VECTOR_SIZE);
	vector<LogicalType> table_types = {types[0]};
	DataChunk table_columns;
	table_columns.Initialize(context, table_types, STANDARD_VECTOR_SIZE);

	auto init_global = ArrowTableFunction::ArrowScanInitGlobal;
	auto init_local = ArrowTableFunction::ArrowScanInitLocalInternal;
	auto function = ArrowTableFunction::ArrowScanFunction;

	idx_t processed = 0;

	vector<column_t> column_ids = {0};
	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	auto global_state = init_global(context, input);
	auto local_state = init_local(context, input, global_state.get());

	while (processed < count) {
		input_chunk.Reset();
		table_columns.Reset();

		auto to_process = MinValue<idx_t>(count - processed, STANDARD_VECTOR_SIZE);

		// Scan a chunk from the table


		TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
		function(context, function_input, table_columns);
		if (table_columns.size() != to_process) {
			throw InvalidInputException("Returned table should have %d tuples, found %d", to_process, table_columns.size());
		}

		// ----------- Add the indices array to the input -----------

		input_chunk.data[0].Reference(table_columns.data[0]);
		input_chunk.data[1].Sequence(processed, 1, to_process);
		input_chunk.SetCardinality(to_process);

		data_collection->Append(append_state, input_chunk);

		processed += to_process;
	}

	auto tuple_count = data_collection->Count();

	return data_collection;
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

	// FIXME: we require the ClientContext to convert pyarrow to DataChunk.
	auto &context = *DuckDBPyConnection::DefaultConnection()->connection->context;

	vector<LogicalType> types;
	types.push_back(GetTypeFromArrow(array));
	types.push_back(LogicalType::UBIGINT);

	// Create a PhysicalColumnDataScan operator
	auto data_collection = ConvertToColumnDataCollection(types, context, array);
	auto count = py::len(array);
	auto physical_scan = make_uniq<PhysicalColumnDataScan>(types, PhysicalOperatorType::COLUMN_DATA_SCAN, count, std::move(data_collection));

	// ----------- Create the Order operator -----------

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

	// Create the PhysicalOrder which will execute the sort
	vector<idx_t> projections = {0, 1};
	auto order = make_uniq<PhysicalOrder>(types, std::move(orders), projections, count);
	order->children.push_back(std::move(physical_scan));

	PreparedStatementData prepared_data(StatementType::SELECT_STATEMENT);
	prepared_data.plan = std::move(order);
	prepared_data.names = {"c0", "c1"};
	prepared_data.types = types;
	auto result_collector = make_uniq_base<PhysicalResultCollector, PhysicalMaterializedCollector>(prepared_data, false);

	Executor executor(context);
	executor.Initialize(std::move(result_collector));

	// Execute the plan

	auto execution_result = PendingExecutionResult::RESULT_NOT_READY;
	while (execution_result != PendingExecutionResult::RESULT_READY) {
		execution_result = executor.ExecuteTask();
	}

	// Fetch the query result and convert this to a single Array

	auto query_result = executor.GetResult();
	auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");

	py::list batches;
	QueryResultChunkScanState scan_state(*query_result);
	DuckDBPyResult::FetchArrowChunk(*query_result, scan_state, batches, count);
	return batches[0].attr("__getitem__")("c1");
}

} // namespace duckdb
