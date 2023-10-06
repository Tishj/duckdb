#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/arrow/arrow_query_result.hpp"

namespace duckdb {

namespace pyarrow {

py::object ToArrowTable(const vector<LogicalType> &types, const vector<string> &names, const py::list &batches,
                        const ClientProperties &options);
py::object ToArrowTable(ArrowQueryResult &result);

} // namespace pyarrow

} // namespace duckdb
