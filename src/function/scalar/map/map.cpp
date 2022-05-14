#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/main/config.hpp"

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/sort/list_sort.hpp"

namespace duckdb {

struct MapBindData : FunctionData {
public:
	MapBindData(const LogicalType &return_type_p, const LogicalType &child_type_p, ClientContext &context_p);
	~MapBindData() override;

	LogicalType return_type;
	LogicalType key_type;

	ListSortInfo info;

	ClientContext &context;
	RowLayout payload_layout;
	vector<BoundOrderByNode> orders;

public:
	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

bool MapBindData::Equals(const FunctionData &other_p) const {
	auto &other = (MapBindData &)other_p;
	return key_type == other.key_type && return_type == other.return_type;
}

MapBindData::MapBindData(const LogicalType &return_type_p, const LogicalType &child_type_p, ClientContext &context_p)
    : return_type(return_type_p), key_type(child_type_p), context(context_p) {

	// get the vector types
	info.types.emplace_back(LogicalType::USMALLINT);
	info.types.emplace_back(key_type);
	D_ASSERT(info.types.size() == 2);

	// get the payload types
	info.payload_types.emplace_back(LogicalType::UINTEGER);
	D_ASSERT(info.payload_types.size() == 1);

	// initialize the payload layout
	payload_layout.Initialize(info.payload_types);

	// get the BoundOrderByNode
	auto idx_col_expr = make_unique_base<Expression, BoundReferenceExpression>(LogicalType::USMALLINT, 0);
	auto lists_col_expr = make_unique_base<Expression, BoundReferenceExpression>(key_type, 1);
	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT, move(idx_col_expr));
	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, move(lists_col_expr));
}

unique_ptr<FunctionData> MapBindData::Copy() const {
	return make_unique<MapBindData>(return_type, key_type, context);
}

MapBindData::~MapBindData() {
}

static void SortKeysAndValues(Vector &keys, Vector &values, Vector &result, MapBindData &bind_data) {
	Vector &lists = keys;

	auto &result_validity = FlatVector::Validity(result);

	if (lists.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	// initialize the global and local sorting state
	auto &buffer_manager = BufferManager::GetBufferManager(bind_data.context);
	bind_data.info.global_sort_state =
	    make_unique<GlobalSortState>(buffer_manager, bind_data.orders, bind_data.payload_layout);

	auto &keys_vector = ListVector::GetEntry(keys);
	auto &values_vector = ListVector::GetEntry(values);
	auto apply_order_to_lists = [&](const SelectionVector &sel, idx_t sel_size) {
		keys_vector.Slice(sel, sel_size);
		keys_vector.Normalify(sel_size);
		values_vector.Slice(sel, sel_size);
		values_vector.Normalify(sel_size);
	};
	auto mark_result_entry_as_invalid = [&](idx_t index) {
		result_validity.SetInvalid(index);
	};

	auto count = ListVector::GetListSize(keys);

	SortLists(lists, count, apply_order_to_lists, mark_result_entry_as_invalid, bind_data.info, buffer_manager);
}

static void MapFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::MAP);

	auto &child_entries = StructVector::GetEntries(result);
	D_ASSERT(child_entries.size() == 2);
	auto &key_vector = child_entries[0];
	auto &value_vector = child_entries[1];
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

	if (ListVector::GetListSize(args.data[0]) != ListVector::GetListSize(args.data[1])) {
		throw Exception("Key list has a different size from Value list");
	}

	//! Sort the key and value list(s)
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &bind_data = (MapBindData &)*func_expr.bind_info;
	SortKeysAndValues(args.data[0], args.data[1], result, bind_data);

	//! Make sure they are unique

	key_vector->Reference(args.data[0]);
	value_vector->Reference(args.data[1]);

	//! Otherwise if its not a constant vector, this breaks the optimizer
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

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
		bound_function.return_type = LogicalType::MAP(move(child_types));
		return make_unique<VariableReturnBindData>(bound_function.return_type);
	}

	bound_function.return_type = LogicalType::MAP(move(child_types));
	auto child_type = ListType::GetChildType(arguments[0]->return_type);
	return make_unique<MapBindData>(bound_function.return_type, child_type, context);
}

void MapFun::RegisterFunction(BuiltinFunctions &set) {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun("map", {}, LogicalTypeId::MAP, MapFunction, false, MapBind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
