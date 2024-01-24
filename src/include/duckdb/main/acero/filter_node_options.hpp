#pragma once

#include "duckdb/main/acero/dataset/exec_node_options.hpp"
#include "duckdb/main/acero/dataset/dataset.hpp"
#include "duckdb/main/acero/dataset/scan_options.hpp"
#include "duckdb/main/acero/compute/expression.hpp"

namespace duckdb {
namespace ac {

using Schema = ArrowSchemaWrapper;

// NOTE: this is modified from the original, because it's using c++17 features and async functionality
class FilterNodeOptions : public arrow::dataset::ExecNodeOptions {
	using base = arrow::dataset::ExecNodeOptions;

public:
	/// \brief create an instance from values
	explicit FilterNodeOptions(duckdb::cp::Expression filter_expression)
	    : base(base::OptionType::FILTER_NODE), filter_expression(std::move(filter_expression)) {
	}

	/// \brief the expression to filter batches
	///
	/// The return type of this expression must be boolean
	duckdb::cp::Expression filter_expression;
};

} // namespace ac
} // namespace duckdb
