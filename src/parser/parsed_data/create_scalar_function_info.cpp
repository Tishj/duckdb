#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/alter_scalar_function_info.hpp"

namespace duckdb {

CreateScalarFunctionInfo::CreateScalarFunctionInfo(ScalarFunction function)
    : CreateFunctionInfo(CatalogType::SCALAR_FUNCTION_ENTRY, function.name), functions(function.name) {
	functions.AddFunction(std::move(function));
	internal = true;
}

CreateScalarFunctionInfo::CreateScalarFunctionInfo(ScalarFunctionSet set)
    : CreateFunctionInfo(CatalogType::SCALAR_FUNCTION_ENTRY, set.name), functions(std::move(set)) {
	for (auto &func : functions.functions) {
		func.name = functions.name;
	}
	internal = true;
}

unique_ptr<CreateInfo> CreateScalarFunctionInfo::Copy() const {
	ScalarFunctionSet set(name);
	set.functions = functions.functions;
	auto result = make_uniq<CreateScalarFunctionInfo>(std::move(set));
	CopyProperties(*result);
	return std::move(result);
}

unique_ptr<AlterInfo> CreateScalarFunctionInfo::GetAlterInfo() const {
	return make_uniq_base<AlterInfo, AddScalarFunctionOverloadInfo>(
	    AlterEntryData(catalog, schema, name, OnEntryNotFound::RETURN_NULL), functions);
}

} // namespace duckdb
