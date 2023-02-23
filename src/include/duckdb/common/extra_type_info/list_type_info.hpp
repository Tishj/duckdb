#pragma once

#include "duckdb/common/extra_type_info.hpp"

namespace duckdb {

class ListTypeInfo : public ExtraTypeInfo {
public:
	explicit ListTypeInfo(LogicalType child_type_p);

	LogicalType child_type;

public:
	void Serialize(FieldWriter &writer) const override;

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;
};

} // namespace duckdb
