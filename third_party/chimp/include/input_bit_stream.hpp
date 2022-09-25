//===----------------------------------------------------------------------===//
//                         DuckDB
//
// third_party/chimp/include/input_bit_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>
#include "bit_utils.hpp"
#include <assert.h>
#include <exception>
#include <stdexcept>
#include <string>

namespace duckdb_chimp {

//! Every byte read touches at most 2 bytes (1 if it's perfectly aligned)
//! Within a byte we need to mask off the bytes that we're interested in
//! And then we need to shift these to the start of the byte
//! I.E we want 4 bits, but our bit index is 3, our mask becomes:
//! 0111 1000
//! With a shift of 3

//! Align the masks to the right
static const uint8_t masks[] = {
	0b00000000,
	0b10000000,
	0b11000000,
	0b11100000,
	0b11110000,
	0b11111000,
	0b11111100,
	0b11111110,
	0b11111111,
	//! These later masks are for the cases where bit_index + SIZE exceeds 8
	0b11111110,
	0b11111100,
	0b11111000,
	0b11110000,
	0b11100000,
	0b11000000,
	0b10000000,
};

static const uint8_t remainder_masks[] = {
	0,
	0,
	0,
	0,
	0,
	0,
	0,
	0,
	0,
	0b10000000,
	0b11000000,
	0b11100000,
	0b11110000,
	0b11111000,
	0b11111100,
	0b11111110,
	0b11111111,
};

//! Left shifts
static const uint8_t shifts[] = {
	0, //unused
	7,
	6,
	5,
	4,
	3,
	2,
	1,
	0,
	0,
	0,
	0,
	0,
	0,
	0,
	0
};

//! Right shifts
//! Right shift the values to cut off the mask when SIZE + bit_index exceeds 8
static const uint8_t right_shifts[] = {
	0,
	0,
	0,
	0,
	0,
	0,
	0,
	0,
	0,
	1,
	2,
	3,
	4,
	5,
	6,
	7,
	8,
};

struct InputBitStream {
public:
public:
	InputBitStream() : input(nullptr), bit_index(0), byte_index(0) {}
	uint8_t *input;
	uint8_t bit_index; //Index in the current byte, starting from the right
	uint64_t byte_index;
public:
	void SetStream(uint8_t* input) {
		this->input = input;
		bit_index = 0;
		byte_index = 0;
	}

	//static inline uint8_t CreateMask(uint8_t size, uint8_t bit_index) {
	static inline uint8_t CreateMask(const uint8_t &size, const uint8_t &bit_index) {
		return (masks[size] >> bit_index);
	}

	static inline uint8_t InnerReadByte(uint8_t* input, const uint64_t& byte_index, const uint8_t &bit_index) {
		// Create a mask given the size and bit_index
		uint8_t result = ((input[byte_index] & CreateMask(8, bit_index)) << bit_index) | ((input[byte_index + 1] & remainder_masks[8 + bit_index]) >> (8 - bit_index));
		return result;
	}

	static inline uint8_t InnerRead(uint8_t* input, const uint64_t& byte_index, const uint8_t &bit_index, const uint8_t &size) {
		const uint8_t left_shift = 8 - size;
		const uint8_t bit_remainder = (size + bit_index) - 8;
		// Create a mask given the size and bit_index
		uint8_t result = ((input[byte_index] & CreateMask(size, bit_index)) << bit_index) >> left_shift | ((input[byte_index + (size + bit_index >= 8)] & remainder_masks[size + bit_index]) >> ((8 - bit_remainder) & 7));
		return result;
	}

	template <class T, uint8_t BYTES>
	inline T ReadBytes(const uint8_t &remainder) {
		throw std::runtime_error("ReadBytes not implemented for BYTES");
	}
	//! 1-7 bits
	template <>
	inline uint8_t ReadBytes<uint8_t, 0>(const uint8_t &remainder) {
		const uint8_t result = InnerRead(input, byte_index, bit_index, remainder);

		byte_index += (remainder + bit_index >= 8);
		bit_index = (remainder + bit_index) & 7;
	
		return result;
	}
	//! 8-15 bits
	template <>
	inline uint16_t ReadBytes<uint16_t, 1>(const uint8_t &remainder) {
		const uint16_t result =	InnerReadByte(input, byte_index, bit_index)		<< remainder | \
							InnerRead(input, byte_index + 1, bit_index, remainder);

		byte_index += 1 + (bit_index + remainder >= 8);
		bit_index = (bit_index + remainder) & 7;

		return result;
	}
	//! 16-23 bits
	template <>
	inline uint32_t ReadBytes<uint32_t, 2>(const uint8_t &remainder) {
		const uint32_t result =	InnerReadByte(input, byte_index, bit_index)		<< (8 + remainder) | \
							InnerReadByte(input, byte_index + 1, bit_index)		<< remainder | \
							InnerRead(input, byte_index + 2, bit_index, remainder);

		byte_index += 2 + (bit_index + remainder >= 8);
		bit_index = (bit_index + remainder) & 7;

		return result;
	}
	//! 24-31 bits
	template <>
	inline uint32_t ReadBytes<uint32_t, 3>(const uint8_t &remainder) {
		const uint32_t result =	InnerReadByte(input, byte_index, bit_index)		<< (16 + remainder) | \
							InnerReadByte(input, byte_index + 1, bit_index)		<< (8 + remainder) | \
							InnerReadByte(input, byte_index + 2, bit_index)		<< remainder | \
							InnerRead(input, byte_index + 3, bit_index, remainder);

		byte_index += 3 + (bit_index + remainder >= 8);
		bit_index = (bit_index + remainder) & 7;

		return result;
	}
	//! 32-39 bits
	template <>
	inline uint64_t ReadBytes<uint64_t, 4>(const uint8_t &remainder) {
		const uint64_t result =	(uint64_t)InnerReadByte(input, byte_index, bit_index)		<< (24 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 1, bit_index)		<< (16 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 2, bit_index)		<< (8 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 3, bit_index)		<< remainder | \
							InnerRead(input, byte_index + 4, bit_index, remainder);

		byte_index += 4 + (bit_index + remainder >= 8);
		bit_index = (bit_index + remainder) & 7;

		return result;
	}
	//! 40-47 bits
	template <>
	inline uint64_t ReadBytes<uint64_t, 5>(const uint8_t &remainder) {
		const uint64_t result =	(uint64_t)InnerReadByte(input, byte_index, bit_index)		<< (32 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 1, bit_index)		<< (24 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 2, bit_index)		<< (16 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 3, bit_index)		<< (8 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 4, bit_index)		<< remainder | \
							InnerRead(input, byte_index + 5, bit_index, remainder);

		byte_index += 5 + (bit_index + remainder >= 8);
		bit_index = (bit_index + remainder) & 7;

		return result;
	}
	//! 48-55 bits
	template <>
	inline uint64_t ReadBytes<uint64_t, 6>(const uint8_t &remainder) {
		const uint64_t result =	(uint64_t)InnerReadByte(input, byte_index, bit_index)		<< (40 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 1, bit_index)		<< (32 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 2, bit_index)		<< (24 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 3, bit_index)		<< (16 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 4, bit_index)		<< (8 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 5, bit_index)		<< remainder | \
							InnerRead(input, byte_index + 6, bit_index, remainder);

		byte_index += 6 + (bit_index + remainder >= 8);
		bit_index = (bit_index + remainder) & 7;

		return result;
	}
	//! 56-63 bits
	template <>
	inline uint64_t ReadBytes<uint64_t, 7>(const uint8_t &remainder) {
		const uint64_t result =	(uint64_t)InnerReadByte(input, byte_index, bit_index)		<< (48 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 1, bit_index)		<< (40 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 2, bit_index)		<< (32 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 3, bit_index)		<< (24 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 4, bit_index)		<< (16 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 5, bit_index)		<< (8 + remainder) | \
							(uint64_t)InnerReadByte(input, byte_index + 6, bit_index)		<< remainder | \
							InnerRead(input, byte_index + 7, bit_index, remainder);

		byte_index += 7 + (bit_index + remainder >= 8);
		bit_index = (bit_index + remainder) & 7;

		return result;
	}
	//! 64 bits
	template <>
	inline uint64_t ReadBytes<uint64_t, 8>(const uint8_t &remainder) {
		const uint64_t result =	(uint64_t)InnerReadByte(input, byte_index, bit_index)		<< 56 | \
							(uint64_t)InnerReadByte(input, byte_index + 1, bit_index)		<< 48 | \
							(uint64_t)InnerReadByte(input, byte_index + 2, bit_index)		<< 40 | \
							(uint64_t)InnerReadByte(input, byte_index + 3, bit_index)		<< 32 | \
							(uint64_t)InnerReadByte(input, byte_index + 4, bit_index)		<< 24 | \
							(uint64_t)InnerReadByte(input, byte_index + 5, bit_index)		<< 16 | \
							(uint64_t)InnerReadByte(input, byte_index + 6, bit_index)		<< 8 | \
							InnerReadByte(input, byte_index + 7, bit_index);

		byte_index += 8;

		return result;
	}

	template <class T, uint8_t BYTES, uint8_t REMAINDER>
	inline T ReadValue() {
		return ReadBytes<T, BYTES>(REMAINDER);
	}

	template <class T>
	inline T ReadValue(uint8_t size = sizeof(T) * __CHAR_BIT__) {
		const uint8_t bytes = size >> 3; //divide by 8;
		const uint8_t remainder = size & 7;
		switch (bytes) {
			case 0: return ReadBytes<uint8_t, 0>(remainder);
			case 1: return ReadBytes<uint16_t, 1>(remainder);
			case 2: return ReadBytes<uint32_t, 2>(remainder);
			case 3: return ReadBytes<uint32_t, 3>(remainder);
			case 4: return ReadBytes<uint64_t, 4>(remainder);
			case 5: return ReadBytes<uint64_t, 5>(remainder);
			case 6: return ReadBytes<uint64_t, 6>(remainder);
			case 7: return ReadBytes<uint64_t, 7>(remainder);
			case 8: return ReadBytes<uint64_t, 8>(remainder);
			default: throw std::runtime_error("ReadValue reports that it needs to read " + std::to_string(bytes) + " bytes");
		}
	}
};

} //namespace duckdb_chimp
