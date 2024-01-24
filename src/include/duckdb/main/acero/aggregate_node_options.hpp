#pragma once

#include "duckdb/main/acero/dataset/exec_node_options.hpp"
#include "duckdb/main/acero/dataset/dataset.hpp"
#include "duckdb/main/acero/dataset/scan_options.hpp"
#include "duckdb/main/acero/compute/expression.hpp"

#include "duckdb/main/acero/compute/aggregate.hpp"

namespace duckdb {
namespace ac {

using cp::Aggregate;
using arrow::FieldRef;

using Schema = ArrowSchemaWrapper;

// NOTE: this is modified from the original, because it's using c++17 features and async functionality
class AggregateNodeOptions : public arrow::dataset::ExecNodeOptions {
	using base = arrow::dataset::ExecNodeOptions;
public:
	/// \brief create an instance from values
	explicit AggregateNodeOptions(std::vector<Aggregate> aggregates,
									std::vector<FieldRef> keys = {},
									std::vector<FieldRef> segment_keys = {})
		: base(base::OptionType::AGGREGATE_NODE),
			aggregates(std::move(aggregates)),
			keys(std::move(keys)),
			segment_keys(std::move(segment_keys)) {}

	// aggregations which will be applied to the targeted fields
	std::vector<Aggregate> aggregates;
	// keys by which aggregations will be grouped (optional)
	std::vector<FieldRef> keys;
	// keys by which aggregations will be segmented (optional)
	std::vector<FieldRef> segment_keys;
};

} // namespace ac
} // namespace duckdb
