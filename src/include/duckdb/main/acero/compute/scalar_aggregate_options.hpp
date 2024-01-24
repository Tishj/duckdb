#pragma once

#include "duckdb/main/acero/dataset/exec_node_options.hpp"
#include "duckdb/main/acero/dataset/dataset.hpp"
#include "duckdb/main/acero/dataset/scan_options.hpp"
#include "duckdb/main/acero/compute/expression.hpp"
#include "duckdb/main/acero/compute/function_options.hpp"

namespace duckdb {
namespace cp {

using Schema = ArrowSchemaWrapper;

// NOTE: this is modified from the original, because it's using c++17 features and async functionality
class ScalarAggregateOptions : public FunctionOptions {
	using base = FunctionOptions;

public:
	explicit ScalarAggregateOptions(bool skip_nulls = true, uint32_t min_count = 1)
	    : base(base::OptionType::SCALAR_AGGREGATE), skip_nulls(skip_nulls), min_count(min_count) {
	}
	static constexpr char const kTypeName[] = "ScalarAggregateOptions";
	static ScalarAggregateOptions Defaults() {
		return ScalarAggregateOptions {};
	}

	/// If true (the default), null values are ignored. Otherwise, if any value is null,
	/// emit null.
	bool skip_nulls;
	/// If less than this many non-null values are observed, emit null.
	uint32_t min_count;
};

} // namespace cp
} // namespace duckdb
