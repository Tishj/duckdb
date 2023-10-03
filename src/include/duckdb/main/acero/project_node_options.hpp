#pragma once

#include "duckdb/main/acero/dataset/exec_node_options.hpp"
#include "duckdb/main/acero/compute/expression.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
namespace ac {

struct ProjectNodeOptions : arrow::dataset::ExecNodeOptions {
	using base = arrow::dataset::ExecNodeOptions;
	explicit ProjectNodeOptions(std::vector<cp::Expression> expressions, std::vector<std::string> names = {})
	    : base(base::OptionType::PROJECT_NODE), expressions(std::move(expressions)), names(std::move(names)) {
	}

	/// \brief the expressions to run on the batches
	///
	/// The output will have one column for each expression.  If you wish to keep any of
	/// the columns from the input then you should create a simple field_ref expression
	/// for that column.
	vector<cp::Expression> expressions;
	/// \brief the names of the output columns
	///
	/// If this is not specified then the result of calling ToString on the expression will
	/// be used instead
	///
	/// This list should either be empty or have the same length as `expressions`
	vector<std::string> names;
};

} // namespace ac
} // namespace duckdb
