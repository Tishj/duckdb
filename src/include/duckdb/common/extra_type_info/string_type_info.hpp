#pragma once

#include "duckdb/common/extra_type_info.hpp"

namespace duckdb {

class StringTypeInfo : public ExtraTypeInfo {
public:
	explicit StringTypeInfo(string collation_p);

	string collation;

public:
	void Serialize(FieldWriter &writer) const override;

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;
};

} // namespace duckdb
