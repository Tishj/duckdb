//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/flagged_pointer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

//! Flagged Pointer distinguishes multiple values that all use the same underlying size
//! By checking and setting the most significant bit(s)
class FlaggedPointer {
public:
	FlaggedPointer() : pointer(0) {};
	virtual ~FlaggedPointer() {
	}
	FlaggedPointer(uint64_t pointer) : pointer(pointer) {
	}
	template <class T>
	FlaggedPointer &operator=(const T *ptr) {
		// If the object already has a non-swizzled pointer, this will leak memory.
		//
		// TODO: If enabled, this assert will fire, indicating a possible leak. If an exception
		// is thrown here, it will cause a double-free. There is some work to do to make all this safer.
		// D_ASSERT(empty() || IsSwizzled());
		if (sizeof(ptr) == 4) {
			pointer = (uint32_t)(size_t)ptr;
		} else {
			pointer = (uint64_t)ptr;
		}
		return *this;
	}

public:
	uint64_t Pointer() const {
		return pointer;
	}

protected:
	uint64_t pointer;

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
