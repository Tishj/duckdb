#pragma once

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/acero/dataset/date32_scalar.hpp"

#include "duckdb/main/acero/compute/function_options.hpp"
#include "duckdb/main/acero/dataset/field_ref.hpp"

namespace duckdb {
namespace cp {

using arrow::FieldRef;

struct Aggregate {
	Aggregate() = default;

	Aggregate(std::string function, std::shared_ptr<FunctionOptions> options,
				std::vector<FieldRef> target, std::string name = "")
		: function(std::move(function)),
			options(std::move(options)),
			target(std::move(target)),
			name(std::move(name)) {}

	Aggregate(std::string function, std::shared_ptr<FunctionOptions> options,
				FieldRef target, std::string name = "")
		: Aggregate(std::move(function), std::move(options),
					std::vector<FieldRef>{std::move(target)}, std::move(name)) {}

	Aggregate(std::string function, FieldRef target, std::string name)
		: Aggregate(std::move(function), /*options=*/nullptr,
					std::vector<FieldRef>{std::move(target)}, std::move(name)) {}

	Aggregate(std::string function, std::string name)
		: Aggregate(std::move(function), /*options=*/nullptr,
					/*target=*/std::vector<FieldRef>{}, std::move(name)) {}

	/// the name of the aggregation function
	std::string function;

	/// options for the aggregation function
	std::shared_ptr<FunctionOptions> options;

	/// zero or more fields to which aggregations will be applied
	std::vector<FieldRef> target;

	/// optional output field name for aggregations
	std::string name;
};

} // namespace cp
} // namespace duckdb
