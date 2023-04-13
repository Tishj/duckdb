#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class Index;

class ConflictInfo {
public:
	ConflictInfo(const unordered_set<column_t> &column_ids, vector<unique_ptr<Expression>> expressions_p,
	             bool only_check_unique = true)
	    : column_ids(column_ids), expressions(std::move(expressions_p)), only_check_unique(only_check_unique) {
	}
	unordered_set<column_t> column_ids;
	vector<unique_ptr<Expression>> expressions;

public:
	bool ConflictTargetMatches(Index &index) const;
	bool ConflictTargetIsEmpty() const;
	void VerifyAllConflictsMeetCondition() const;

public:
	bool only_check_unique = true;
};

} // namespace duckdb
