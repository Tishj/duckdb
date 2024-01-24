#pragma once

#include "duckdb/main/acero/dataset/exec_node_options.hpp"
#include "duckdb/main/acero/dataset/dataset.hpp"
#include "duckdb/main/acero/dataset/scan_options.hpp"
#include "duckdb/main/acero/compute/expression.hpp"

namespace duckdb {
namespace ac {

using Schema = ArrowSchemaWrapper;

// NOTE: this is modified from the original, because it's using c++17 features and async functionality
class TableSourceNodeOptions : public arrow::dataset::ExecNodeOptions {
	using base = arrow::dataset::ExecNodeOptions;


public:
	static constexpr int64_t kDefaultMaxBatchSize = 1 << 20;

	/// Create an instance from values
	TableSourceNodeOptions(std::shared_ptr<arrow::Table> table, int64_t max_batch_size = kDefaultMaxBatchSize)
		: base(base::OptionType::TABLE_SOURCE_NODE), table(std::move(table)), max_batch_size(max_batch_size) {}

	/// \brief a table which acts as the data source
	std::shared_ptr<arrow::Table> table;
	/// \brief size of batches to emit from this node
	/// If the table is larger the node will emit multiple batches from the
	/// the table to be processed in parallel.
	int64_t max_batch_size;
};

} // namespace ac
} // namespace duckdb
