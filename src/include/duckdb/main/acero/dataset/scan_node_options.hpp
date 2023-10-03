#pragma once

#include "duckdb/main/acero/dataset/exec_node_options.hpp"

namespace duckdb {
namespace arrow {
namespace dataset {

struct ScanNodeOptions : public ExecNodeOptions {
public:
	explicit ScanNodeOptions(std::shared_ptr<Dataset> dataset, std::shared_ptr<ScanOptions> scan_options,
	                         bool require_sequenced_output = false)
	    : dataset(std::move(dataset)), scan_options(std::move(scan_options)),
	      require_sequenced_output(require_sequenced_output) {
	}

	std::shared_ptr<Dataset> dataset;
	std::shared_ptr<ScanOptions> scan_options;
	bool require_sequenced_output;
};

} // namespace dataset
} // namespace arrow
} // namespace duckdb
