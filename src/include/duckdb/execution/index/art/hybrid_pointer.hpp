//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/hybrid_pointer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

//! This class does nothing more than represent two different types with the same underlying data
//! These types are distinguished from eachother using the most significant bit

//! The 'Primary' type is defined by the flag being SET
//! The 'Secondary' type is defined by flag NOT being set
template <class PRIMARY, class SECONDARY>
class HybridPointer {
public:
	virtual ~HybridPointer() {
	}
	HybridPointer() : pointer(0) {
		D_ASSERT(sizeof(PRIMARY) <= POINTER_SIZE);
		D_ASSERT(sizeof(SECONDARY) <= POINTER_SIZE);
	};
	inline bool IsPrimary() const {
		return (pointer >> (POINTER_SIZE - 1)) & 1;
	}
	PRIMARY GetPrimary() const {
		D_ASSERT(IsPrimary());
		return (PRIMARY)pointer;
	}
	SECONDARY GetSecondary() const {
		D_ASSERT(!IsPrimary());
		return (SECONDARY)pointer;
	}

protected:
protected:
	uint64_t pointer;
	static constexpr uint8_t POINTER_SIZE = sizeof(pointer);
	static constexpr uint8_t POINTER_BIT_SIZE = POINTER_SIZE * 8;
	//! If this mask is set, the underlying type is 'Primary'
	static constexpr uint64_t PRIMARY_MASK = (uint64_t)1 << (POINTER_BIT_SIZE - 1);
};

} // namespace duckdb
