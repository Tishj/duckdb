#pragma once

#include "duckdb/main/acero/dataset/exec_node_options.hpp"
#include "duckdb/main/acero/dataset/dataset.hpp"
#include "duckdb/main/acero/dataset/scan_options.hpp"

namespace duckdb {
namespace arrow {
namespace dataset {

struct ScanNodeOptions : public ExecNodeOptions {
public:
	explicit ScanNodeOptions(std::shared_ptr<Dataset> dataset, std::shared_ptr<ScanOptions> scan_options,
	                         bool require_sequenced_output = false)
	    : ExecNodeOptions(ExecNodeOptions::OptionType::SCAN_NODE), dataset(std::move(dataset)),
	      scan_options(std::move(scan_options)), require_sequenced_output(require_sequenced_output) {
	}

	std::shared_ptr<Dataset> dataset;
	std::shared_ptr<ScanOptions> scan_options;
	bool require_sequenced_output;
};

} // namespace dataset
} // namespace arrow
} // namespace duckdb
