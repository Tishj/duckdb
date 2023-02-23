#pragma once

#include "duckdb/common/extra_type_info.hpp"

namespace duckdb {

enum EnumDictType : uint8_t { INVALID = 0, VECTOR_DICT = 1 };

class EnumTypeInfo : public ExtraTypeInfo {
public:
	explicit EnumTypeInfo(string enum_name_p, Vector &values_insert_order_p, idx_t dict_size_p);
	EnumDictType dict_type;
	string enum_name;
	Vector values_insert_order;
	idx_t dict_size;

protected:
	// Equalities are only used in enums with different catalog entries
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;

	void Serialize(FieldWriter &writer) const override;
};

template <class T>
class EnumTypeInfoTemplated : public EnumTypeInfo {
public:
	explicit EnumTypeInfoTemplated(const string &enum_name_p, Vector &values_insert_order_p, idx_t size_p)
	    : EnumTypeInfo(enum_name_p, values_insert_order_p, size_p) {
		D_ASSERT(values_insert_order_p.GetType().InternalType() == PhysicalType::VARCHAR);

		UnifiedVectorFormat vdata;
		values_insert_order.ToUnifiedFormat(size_p, vdata);

		auto data = (string_t *)vdata.data;
		for (idx_t i = 0; i < size_p; i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				throw InternalException("Attempted to create ENUM type with NULL value");
			}
			if (values.count(data[idx]) > 0) {
				throw InvalidInputException("Attempted to create ENUM type with duplicate value %s",
				                            data[idx].GetString());
			}
			values[data[idx]] = i;
		}
	}

	static shared_ptr<EnumTypeInfoTemplated> Deserialize(FieldReader &reader, uint32_t size) {
		auto enum_name = reader.ReadRequired<string>();
		Vector values_insert_order(LogicalType::VARCHAR, size);
		values_insert_order.Deserialize(size, reader.GetSource());
		return make_shared<EnumTypeInfoTemplated>(std::move(enum_name), values_insert_order, size);
	}

	string_map_t<T> values;
};

} // namespace duckdb
