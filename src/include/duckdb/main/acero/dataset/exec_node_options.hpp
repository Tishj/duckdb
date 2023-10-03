#pragma once

#include "duckdb/main/acero/options.hpp"

namespace duckdb {
namespace arrow {
namespace dataset {

struct ExecNodeOptions {
public:
	enum class OptionType { SCAN_NODE, PROJECT_NODE };

public:
	virtual ~ExecNodeOptions() = default;
	OptionType Type() {
		return type;
	}
	template <class T>
	const T &Cast() {
		return dynamic_cast<const T &>(*this);
	}

protected:
	ExecNodeOptions(OptionType type) : type(type) {
	}

private:
	OptionType type;
};

} // namespace dataset
} // namespace arrow
} // namespace duckdb
