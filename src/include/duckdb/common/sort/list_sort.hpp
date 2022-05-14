//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sort/list_sort.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sort/list_sort.hpp"

namespace duckdb {

struct ListSortInfo {
	vector<LogicalType> types;
	vector<LogicalType> payload_types;
	unique_ptr<GlobalSortState> global_sort_state;
};

void SortLists(Vector &lists, idx_t count,
               const std::function<void(const SelectionVector &sel, idx_t sel_size)> &apply_sort,
               const std::function<void(idx_t index)> &invalid_list_callback, ListSortInfo &info,
               BufferManager &buffer_manager);

struct ListSorter {
public:
public:
private:
private:
};

} // namespace duckdb
