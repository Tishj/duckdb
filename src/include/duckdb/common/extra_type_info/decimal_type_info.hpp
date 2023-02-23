#pragma once

#include "duckdb/common/extra_type_info.hpp"

namespace duckdb {

class DecimalTypeInfo : public ExtraTypeInfo {
public:
	DecimalTypeInfo(uint8_t width_p, uint8_t scale_p);
	uint8_t width;
	uint8_t scale;

public:
	void Serialize(FieldWriter &writer) const override;

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;
};

} // namespace duckdb
