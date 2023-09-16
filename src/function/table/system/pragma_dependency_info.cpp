#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/function/function_set.hpp"
namespace duckdb {

struct PragmaDependencyFunctionData : public TableFunctionData {
	explicit PragmaDependencyFunctionData() {
	}
};

struct PragmaDependencyOperatorData : public GlobalTableFunctionState {
	PragmaDependencyOperatorData() : offset(0) {
	}

	idx_t offset;
};

static unique_ptr<FunctionData> PragmaDependencyInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("connections");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema");
	return_types.emplace_back(LogicalType::VARCHAR);

	string db_name =
	    input.inputs.empty() ? DatabaseManager::GetDefaultDatabase(context) : StringValue::Get(input.inputs[0]);
	auto &catalog = Catalog::GetCatalog(context, db_name);
	auto result = make_uniq<PragmaDependencyFunctionData>();
	result->metadata_info = catalog.GetMetadataInfo(context);
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> PragmaDependencyInfoInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<PragmaDependencyOperatorData>();
}

static void PragmaDependencyInfoFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<PragmaDependencyFunctionData>();
	auto &data = data_p.global_state->Cast<PragmaDependencyOperatorData>();
	idx_t count = 0;
	while (data.offset < bind_data.metadata_info.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = bind_data.metadata_info[data.offset++];

		idx_t col_idx = 0;
		// block_id
		output.SetValue(col_idx++, count, Value::BIGINT(entry.block_id));
		// total_blocks
		output.SetValue(col_idx++, count, Value::BIGINT(entry.total_blocks));
		// free_blocks
		output.SetValue(col_idx++, count, Value::BIGINT(entry.free_list.size()));
		// free_list
		vector<Value> list_values;
		for (auto &free_id : entry.free_list) {
			list_values.push_back(Value::BIGINT(free_id));
		}
		output.SetValue(col_idx++, count, Value::LIST(LogicalType::BIGINT, std::move(list_values)));
		count++;
	}
	output.SetCardinality(count);
}

void PragmaDependencyInfo::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet metadata_info("pragma_metadata_info");
	metadata_info.AddFunction(
	    TableFunction({}, PragmaDependencyInfoFunction, PragmaDependencyInfoBind, PragmaDependencyInfoInit));
	metadata_info.AddFunction(TableFunction({LogicalType::VARCHAR}, PragmaDependencyInfoFunction,
	                                        PragmaDependencyInfoBind, PragmaDependencyInfoInit));
	set.AddFunction(metadata_info);
}

} // namespace duckdb
