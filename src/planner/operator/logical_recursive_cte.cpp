#include "duckdb/planner/operator/logical_recursive_cte.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

InsertionOrderPreservingMap<string> LogicalRecursiveCTE::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Name"] = ctename;
	result["Table Index"] = StringUtil::Format("%llu", table_index);
	SetParamsEstimatedCardinality(result);
	return result;
}

vector<idx_t> LogicalRecursiveCTE::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalRecursiveCTE::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
