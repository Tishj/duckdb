#pragma once

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/types/date.hpp"

namespace duckdb {
namespace arrow {

struct Date32Scalar{
public:
	Date32Scalar(int32_t days) : days(days) {}
public:
	Value GetDuckValue() const {
		return Value::DATE(date_t(days));
	}
public:
	int32_t days;
};

} // namespace arrow
} // namespace duckdb
