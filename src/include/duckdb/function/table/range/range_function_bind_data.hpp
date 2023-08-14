#pragma once

#include "duckdb/function/function.hpp"

namespace duckdb {

struct RangeFunctionBindData : public FunctionData {
public:
	RangeFunctionBindData(vector<LogicalType> &input_types) : input_types(input_types) {
	}
	~RangeFunctionBindData() {
	}

public:
	virtual unique_ptr<FunctionData> Copy() const {
		return make_uniq<RangeFunctionBindData>(input_types);
	}
	bool Equals(const FunctionData &other_p) const {
		// FIXME: This should be hidden behind a TryCast method
		auto other_ptr = dynamic_cast<const RangeFunctionBindData *>(&other_p);

		if (!other_ptr) {
			return false;
		}
		auto other = other_p.Cast<RangeFunctionBindData>();
		if (other.input_types.size() != input_types.size()) {
			return false;
		}
		for (idx_t i = 0; i < input_types.size(); i++) {
			auto &a = input_types[i];
			auto &b = other.input_types[i];
			if (a != b) {
				return false;
			}
		}
		return true;
	}

public:
	const vector<LogicalType> &input_types;
};

} // namespace duckdb
