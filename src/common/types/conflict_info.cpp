#include "duckdb/common/types/constraint_conflict_info.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

bool ConflictInfo::ConflictTargetIsEmpty() const {
	return expressions.empty() && only_check_unique;
}

bool ConflictInfo::ConflictTargetMatches(Index &index) const {
	if (only_check_unique && !index.IsUnique()) {
		// We only support checking ON CONFLICT for Unique/Primary key constraints
		return false;
	}
	if (ConflictTargetIsEmpty()) {
		return true;
	}
	if (column_ids != index.column_id_set) {
		return false;
	}
	// TODO: find a way to make this more work
	// the 'column_index' of the expressions aren't guaranteed to line up currently..
	// for (auto& expr : expressions) {
	//	bool match = false;
	//	for (auto& index_expr : index.unbound_expressions) {
	//		if (expr->Equals(index_expr.get())) {
	//			match = true;
	//			break;
	//		}
	//	}
	//	if (!match) {
	//		// None of the expressions of the Index matched with this expression of our target
	//		return false;
	//	}
	//}
	return true;
}

} // namespace duckdb
