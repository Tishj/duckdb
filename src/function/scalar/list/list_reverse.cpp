#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static SelectionVector CreateReverseSelectionVector(Vector &list, idx_t count) {
	UnifiedVectorFormat input_list_data;
	list.ToUnifiedFormat(count, input_list_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(input_list_data);
	auto total_list_size = ListVector::GetListSize(list);

	SelectionVector rev_sel(total_list_size);

	for (idx_t i = 0; i < total_list_size; i++) {
		rev_sel.set_index(i, 0);
	}
	// Limit the rows if we're dealing with a constant vector
	count = list.GetVectorType() == VectorType::CONSTANT_VECTOR ? 1 : count;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto input_list_list_index = input_list_data.sel->get_index(row_idx);

		if (!input_list_data.validity.RowIsValid(input_list_list_index)) {
			continue;
		};

		D_ASSERT(input_list_data.validity.RowIsValid(input_list_list_index));
		const auto &input_list_entry = list_entries[input_list_list_index];

		// reverse the indices for a list
		// ex for list of length 4: previous indices = [5,6,7,8] -> after indices = [8,7,6,5]
		auto max_idx = input_list_entry.length - 1;

		idx_t offset = input_list_entry.offset;
		for (idx_t i = 0; i < input_list_entry.length; i++) {
			rev_sel.set_index(offset + i, offset + (max_idx - i));
		}
	}
	return rev_sel;
}

static void ListReverseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto count = args.size();

	Vector &input_list = args.data[0];
	if (input_list.GetType().id() == LogicalTypeId::SQLNULL) {
		// If the input is NULL, the output is also NULL
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);

	// create a selection vector for slicing the child vector
	auto rev_sel = CreateReverseSelectionVector(input_list, count);

	result.Reference(input_list);
	auto &result_child = ListVector::GetEntry(result);
	result_child.Slice(rev_sel, ListVector::GetListSize(input_list));

	if (input_list.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
}

static unique_ptr<FunctionData> ListReverseBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 1);

	auto &input_list = arguments[0]->return_type;
	if (input_list.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	// if the input is a NULL, we should just return a NULL
	else if (input_list.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = input_list;
		bound_function.return_type = input_list;
	} else {
		D_ASSERT(input_list.id() == LogicalTypeId::LIST);

		LogicalType child_type = LogicalType::SQLNULL;
		for (const auto &argument : arguments) {
			child_type = LogicalType::MaxLogicalType(child_type, ListType::GetChildType(argument->return_type));
		}
		auto list_type = LogicalType::LIST(child_type);

		bound_function.arguments[0] = list_type;
		bound_function.return_type = list_type;
	}
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<BaseStatistics> ListReverseStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	D_ASSERT(child_stats.size() == 1);

	auto &left_stats = child_stats[0];

	auto stats = left_stats.ToUnique();

	return stats;
}

ScalarFunction ListReverseFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	auto fun = ScalarFunction({LogicalType::LIST(LogicalType::ANY)}, LogicalType::LIST(LogicalType::ANY),
	                          ListReverseFunction, ListReverseBind, nullptr, ListReverseStats);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

void ListReverseFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_reverse", "array_reverse"}, GetFunction());
}

} // namespace duckdb
