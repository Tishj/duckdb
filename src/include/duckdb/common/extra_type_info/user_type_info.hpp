#pragma once

#include "duckdb/common/extra_type_info.hpp"

namespace duckdb {

struct UserTypeInfo : public ExtraTypeInfo {
	explicit UserTypeInfo(string name_p);

	string user_type_name;

public:
	void Serialize(FieldWriter &writer) const override;

	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;
};

} // namespace duckdb
