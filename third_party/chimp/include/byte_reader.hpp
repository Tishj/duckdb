#pragma once

#include <stdint.h>
#include <iostream>
#include "duckdb/common/fast_mem.hpp"

namespace duckdb_chimp {

class ByteReader {
public:
	ByteReader() : buffer(nullptr) {

	}
public:
	void SetStream(uint8_t* buffer) {
		this->buffer = buffer;
	}

	template <class T>
	T ReadValue() {
		throw std::runtime_error("Unsupported type for ReadValue");
	}

	template <>
	uint8_t ReadValue<uint8_t>() {
		auto result = duckdb::Load<uint8_t>(buffer);
		buffer++;
		return result;
	}
	template <>
	uint16_t ReadValue<uint16_t>() {
		auto result = duckdb::Load<uint16_t>(buffer);
		buffer += 2;
		return result;
	}
	template <>
	uint32_t ReadValue<uint32_t>() {
		auto result = duckdb::Load<uint32_t>(buffer);
		buffer += 4;
		return result;
	}
	template <>
	uint64_t ReadValue<uint64_t>() {
		auto result = duckdb::Load<uint64_t>(buffer);
		buffer += 8;
		return result;
	}

	template <class T> 
	T ReadValue(uint8_t size) {
		T result;
		switch(size) {
		case 1:
		case 2:
		case 3:
		case 4:
		case 5:
		case 6:
		case 7:
		case 8:
			result = duckdb::Load<uint8_t>(buffer);
			buffer++;
			return result;
		case 9:
		case 10:
		case 11:
		case 12:
		case 13:
		case 14:
		case 15:
		case 16:
			result = duckdb::Load<uint16_t>(buffer);
			buffer += 2;
			return result;
		case 17:
		case 18:
		case 19:
		case 20:
		case 21:
		case 22:
		case 23:
		case 24:
			result = duckdb::Load<uint8_t>(buffer) + (duckdb::Load<uint16_t>(buffer + 1) << 8);
			buffer += 3;
			return result;
		case 25:
		case 26:
		case 27:
		case 28:
		case 29:
		case 30:
		case 31:
		case 32:
			result = duckdb::Load<uint32_t>(buffer);
			buffer+=4;
			return result;
		case 33:
		case 34:
		case 35:
		case 36:
		case 37:
		case 38:
		case 39:
		case 40:
			result = duckdb::Load<uint8_t>(buffer) + (duckdb::Load<uint32_t>(buffer + 1) << 8);
			buffer+=5;
			return result;
		case 41:
		case 42:
		case 43:
		case 44:
		case 45:
		case 46:
		case 47:
		case 48:
			result = duckdb::Load<uint16_t>(buffer) + (duckdb::Load<uint32_t>(buffer + 2) << 16);
			buffer+=6;
			return result;
		case 49:
		case 50:
		case 51:
		case 52:
		case 53:
		case 54:
		case 55:
		case 56:
			result = 0;
			memcpy(&result, (void *)(buffer), 7);
			buffer+=7;
			return result;
		default:
			result = duckdb::Load<uint64_t>(buffer);
			buffer+=8;
			return result;
		}
	}
private:
	uint8_t *buffer;
};

} //namespace duckdb
