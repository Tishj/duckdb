#pragma once

#include "duckdb/main/acero/dataset/exec_node_options.hpp"
#include "duckdb/main/acero/dataset/dataset.hpp"
#include "duckdb/main/acero/dataset/scan_options.hpp"
#include "duckdb/main/acero/compute/expression.hpp"

namespace duckdb {
namespace cp {

struct FunctionOptions {
public:
	enum class OptionType { SCALAR_AGGREGATE };

public:
	virtual ~FunctionOptions() = default;
	OptionType Type() {
		return type;
	}
	template <class T>
	const T &Cast() {
		return dynamic_cast<const T &>(*this);
	}

protected:
	FunctionOptions(OptionType type) : type(type) {
	}

private:
	OptionType type;
};

} // namespace cp
} // namespace duckdb
