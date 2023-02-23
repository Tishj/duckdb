#pragma once

#include "duckdb/common/extra_type_info.hpp"

namespace duckdb {

class StructTypeInfo : public ExtraTypeInfo {
public:
	explicit StructTypeInfo(child_list_t<LogicalType> child_types_p);

	child_list_t<LogicalType> child_types;

public:
	void Serialize(FieldWriter &writer) const override;

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;
};
} // namespace duckdb
