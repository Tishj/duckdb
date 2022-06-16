#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/list_aggregate_function.hpp"
#include "duckdb/function/aggregate/nested_functions.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

unique_ptr<BoundAggregateExpression> GetBoundUniqueAggregate(const LogicalType &type, unique_ptr<Expression> filter,
                                                             bool is_distinct, bool cast_parameters) {
	// Create and prepare the aggregate function that is used by the unique function
	auto aggr_function = HistogramFun::GetHistogramUnorderedMap(type);
	auto expr = make_unique<BoundConstantExpression>(Value(type));

	vector<unique_ptr<Expression>> children;
	children.push_back(move(expr));

	return AggregateFunction::BindAggregateFunctionNoContext(HistogramAggregateFunctionBinder, move(aggr_function),
	                                                         move(children));
}

bool AreKeysUnique(Vector &keys, idx_t row_count, BoundAggregateExpression &aggr) {
	// Get the type of the key and the size (row_count) of the list vector
	auto list_type = keys.GetType();
	D_ASSERT(list_type.id() == LogicalTypeId::LIST);
	auto key_type = ListType::GetChildType(list_type);

	// Create the vector that stores the result of the unique function
	Vector result(LogicalType::UBIGINT);
	result.Initialize(false, row_count);

	// Run the unique function, stores its result into `result`
	ListAggregatesFunctionStripped<UniqueFunctor>(keys, row_count, aggr, result);

	// Verify for every row that all the keys are unique
	auto keys_data = ListVector::GetData(keys);
	for (idx_t i = 0; i < row_count; i++) {
		auto keys_length = keys_data[i].length;
		auto unique_keys = FlatVector::GetValue<uint64_t>(result, i);
		if (unique_keys != keys_length) {
			return false;
		}
	}
	return true;
}

bool KeyListIsEmpty(list_entry_t *data, idx_t rows) {
	for (idx_t i = 0; i < rows; i++) {
		auto size = data[i].length;
		if (size != 0) {
			return false;
		}
	}
	return true;
}

static bool AreKeysNull(Vector &keys, idx_t row_count) {
	auto list_type = keys.GetType();
	auto key_type = ListType::GetChildType(list_type).id();
	auto arg_data = ListVector::GetData(keys);
	auto &entries = ListVector::GetEntry(keys);

	if (key_type == LogicalTypeId::SQLNULL) {
		if (KeyListIsEmpty(arg_data, row_count)) {
			return false;
		}
		return true;
	}

	VectorData list_data;
	keys.Orrify(row_count, list_data);
	auto validity = FlatVector::Validity(entries);
	return (!validity.CheckAllValid(row_count));
}

static void MapFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::MAP);

	//! Otherwise if its not a constant vector, this breaks the optimizer
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	auto &child_entries = StructVector::GetEntries(result);
	D_ASSERT(child_entries.size() == 2);
	auto &key_vector = child_entries[0];
	auto &value_vector = child_entries[1];
	// called signature is 'map()'
	if (args.data.empty()) {
		// no arguments: construct an empty map
		ListVector::SetListSize(*key_vector, 0);
		key_vector->SetVectorType(VectorType::CONSTANT_VECTOR);
		auto list_data = ConstantVector::GetData<list_entry_t>(*key_vector);
		list_data->offset = 0;
		list_data->length = 0;

		ListVector::SetListSize(*value_vector, 0);
		value_vector->SetVectorType(VectorType::CONSTANT_VECTOR);
		list_data = ConstantVector::GetData<list_entry_t>(*value_vector);
		list_data->offset = 0;
		list_data->length = 0;

		result.Verify(args.size());
		return;
	}

	// map([], []) || map([NULL], [5])
	if (AreKeysNull(args.data[0], args.size())) {
		throw InvalidInputException("Map keys can not be NULL");
	}

	auto key_type = ListType::GetChildType(args.data[0].GetType());
	if (key_type.id() != LogicalTypeId::SQLNULL) {
		// get the aggregate function
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (ListAggregatesBindData &)*func_expr.bind_info;
		auto &aggr = (BoundAggregateExpression &)*info.aggr_expr;
		if (!AreKeysUnique(args.data[0], args.size(), aggr)) {
			throw InvalidInputException("Map keys have to be unique");
		}
	}

	if (ListVector::GetListSize(args.data[0]) != ListVector::GetListSize(args.data[1])) {
		throw Exception("Key list has a different size from Value list");
	}
	key_vector->Reference(args.data[0]);
	value_vector->Reference(args.data[1]);

	result.Verify(args.size());
}

static unique_ptr<FunctionData> MapBind(ClientContext &context, ScalarFunction &bound_function,
                                        vector<unique_ptr<Expression>> &arguments) {
	child_list_t<LogicalType> child_types;

	if (arguments.size() != 2 && !arguments.empty()) {
		throw Exception("We need exactly two lists for a map");
	}
	if (arguments.size() == 2) {
		if (arguments[0]->return_type.id() != LogicalTypeId::LIST) {
			throw Exception("First argument is not a list");
		}
		if (arguments[1]->return_type.id() != LogicalTypeId::LIST) {
			throw Exception("Second argument is not a list");
		}
		child_types.push_back(make_pair("key", arguments[0]->return_type));
		child_types.push_back(make_pair("value", arguments[1]->return_type));
	}

	if (arguments.empty()) {
		auto empty = LogicalType::LIST(LogicalTypeId::SQLNULL);
		child_types.push_back(make_pair("key", empty));
		child_types.push_back(make_pair("value", empty));
	}

	//! this is more for completeness reasons
	auto key_type = ListType::GetChildType(child_types[0].second);
	bound_function.return_type = LogicalType::MAP(move(child_types));
	if (arguments.empty() || key_type.id() == LogicalTypeId::SQLNULL) {
		return make_unique<VariableReturnBindData>(bound_function.return_type);
	}
	auto aggr = GetBoundUniqueAggregate(key_type);
	return make_unique<ListAggregatesBindData>(bound_function.return_type, move(aggr));
}

void MapFun::RegisterFunction(BuiltinFunctions &set) {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun("map", {}, LogicalTypeId::MAP, MapFunction, false, MapBind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
