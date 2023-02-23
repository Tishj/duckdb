#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

class TypeCatalogEntry;

enum class ExtraTypeInfoType : uint8_t {
	INVALID_TYPE_INFO = 0,
	GENERIC_TYPE_INFO = 1,
	DECIMAL_TYPE_INFO = 2,
	STRING_TYPE_INFO = 3,
	LIST_TYPE_INFO = 4,
	STRUCT_TYPE_INFO = 5,
	ENUM_TYPE_INFO = 6,
	USER_TYPE_INFO = 7,
	AGGREGATE_STATE_TYPE_INFO = 8
};

class ExtraTypeInfo {
public:
	explicit ExtraTypeInfo(ExtraTypeInfoType type);
	explicit ExtraTypeInfo(ExtraTypeInfoType type, string alias);
	virtual ~ExtraTypeInfo();

	ExtraTypeInfoType type;
	string alias;
	TypeCatalogEntry *catalog_entry = nullptr;

public:
	bool Equals(ExtraTypeInfo *other_p) const;
	//! Serializes a ExtraTypeInfo to a stand-alone binary blob
	virtual void Serialize(FieldWriter &writer) const;
	//! Serializes a ExtraTypeInfo to a stand-alone binary blob
	static void Serialize(ExtraTypeInfo *info, FieldWriter &writer);
	//! Deserializes a blob back into an ExtraTypeInfo
	static shared_ptr<ExtraTypeInfo> Deserialize(FieldReader &reader);

protected:
	virtual bool EqualsInternal(ExtraTypeInfo *other_p) const;
};

} // namespace duckdb
