//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/flagged_pointer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

//! Flagged Pointer distinguishes multiple values inside the same uint64_t by using bit fields
class FlaggedPointer {
public:
	FlaggedPointer() : pointer(0) {};
	virtual ~FlaggedPointer() {
	}
	FlaggedPointer(uint64_t pointer) : pointer(pointer) {
	}
	template <class T>
	FlaggedPointer &operator=(const T *ptr) {
		if (sizeof(ptr) == 4) {
			pointer = (uint32_t)(size_t)ptr;
		} else {
			pointer = (uint64_t)ptr;
		}
		return *this;
	}

public:
	void SetSwizzled() {
		Set<0>();
	}
	void SetRowid() {
		Set<1>();
	}
	void UnsetSwizzled() {
		Unset<0>();
	}
	void UnsetRowid() {
		Unset<1>();
	}

	bool IsSwizzled() const {
		return IsSet<0>();
	}
	bool IsRowid() const {
		return IsSet<1>();
	}
	uint64_t Pointer() const {
		return pointer;
	}

protected:
	uint64_t pointer;

private:
	static constexpr uint8_t POINTER_BYTE_SIZE = sizeof(pointer);
	static constexpr uint8_t POINTER_BIT_SIZE = POINTER_BYTE_SIZE * 8;
	template <uint8_t BIT>
	bool IsSet() const {
		return ((pointer << BIT) >> (POINTER_BIT_SIZE - 1)) & 1;
	}
	template <uint8_t BIT>
	uint64_t Mask() {
		return 1ULL << (POINTER_BIT_SIZE - 1 - BIT);
	}
	template <uint8_t BIT>
	void Set() {
		pointer |= Mask<BIT>();
	}
	template <uint8_t BIT>
	void Unset() {
		pointer &= ~(Mask<BIT>());
	}
};

} // namespace duckdb
