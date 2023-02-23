#pragma once

#include "duckdb/common/extra_type_info.hpp"

namespace duckdb {

class AggregateStateTypeInfo : public ExtraTypeInfo {
public:
	explicit AggregateStateTypeInfo(aggregate_state_t state_type_p);

	aggregate_state_t state_type;

public:
	void Serialize(FieldWriter &writer) const override;

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;
};

} // namespace duckdb
