#pragma once

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
namespace arrow {

struct FieldRef {
public:
	FieldRef(std::string name) : name(std::move(name)) {
	} // NOLINT runtime/explicit
	FieldRef(const char *name) : name(std::string(name)) {
	} // NOLINT runtime/explicit
public:
	std::string name;
};

} // namespace arrow
} // namespace duckdb
