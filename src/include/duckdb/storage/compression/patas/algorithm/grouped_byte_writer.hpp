#pragma once

#include "duckdb/storage/compression/patas/shared.hpp"
#include "duckdb.h"
#include "duckdb/common/helper.hpp"

#ifdef DEBUG
#include "duckdb/storage/compression/chimp/algorithm/byte_reader.hpp"
#endif

namespace duckdb {

//! Writes bytes to separate groups based on how big the value is

template <bool EMPTY>
class GroupedByteWriter {
public:
	GroupedByteWriter() {
		value_arrays[0] = ones;
		value_arrays[1] = twos;
		value_arrays[2] = threes;
		value_arrays[3] = fours;
		value_arrays[4] = fives;
		value_arrays[5] = sixes;
		value_arrays[6] = sevens;
		value_arrays[7] = eights;
		ResetGroup();
	}

public:
	void Reset() {
		ResetGroup();
	}

	void ResetGroup() {
		for (idx_t i = 0; i < 8; i++) {
			counters[i] = 0;
		}
		index = 0;
	}

	void SetStream(uint8_t *buffer) {
		(void)buffer;
	}

	idx_t BytesWritten() const {
		idx_t bytes_written = 0;
		for (idx_t i = 0; i < 8; i++) {
			bytes_written += counters[i] * (i + 1);
		}
		return bytes_written;
	}

	template <class T, uint8_t SIZE>
	void WriteValue(T value) {
		// if (EMPTY) {
		//	counters[SIZE-1]++;
		// }
		// else {
		value_arrays[SIZE - 1][counters[SIZE - 1]++] = value;
		printf("[%llu] - INSERTED VALUE: %llu | SIZE: %u\n", index++, value, (uint32_t)SIZE);
		//}
	}

	template <class T>
	void WriteValue(T value, uint8_t size) {
		// if (EMPTY) {
		//	counters[size-1]++;
		// }
		// else {
		value_arrays[size - 1][counters[size - 1]++] = value;
		printf("[%llu] - INSERTED VALUE: %llu | SIZE: %u\n", index++, value, (uint32_t)size);
		//}
	}

	// FIXME: This probably isn't super fast
	idx_t Serialize(uint8_t *dest) {
		idx_t bytes_written = 0;

		for (idx_t length = 0; length < 8; length++) {
			const idx_t count = counters[length];
			const uint8_t byte_size = length + 1;
			auto values = value_arrays[length];
			for (idx_t i = 0; i < count; i++) {
				printf("SERIALIZING: SIZE %llu | INDEX(of length) %llu | VALUE %llu\n", length + 1, i, values[i]);
				memcpy(dest + bytes_written + (i * byte_size), values + i, byte_size);
			}
			bytes_written += (count * byte_size);
			// FIXME: add padding to align to the next-length byte-boundary ?
		}
#ifdef DEBUG
		// Verify that the serialize is non-destructive
		duckdb_chimp::ByteReader byte_reader;
		byte_reader.SetStream(dest);

		idx_t total_count = 0;
		for (idx_t length = 0; length < 8; length++) {
			const idx_t count = counters[length];
			printf("LENGTH: %llu | COUNT: %llu\n", length, count);
			for (idx_t i = 0; i < count; i++) {
				const uint8_t byte_size = length + 1;
				auto deserialized_value = byte_reader.ReadValue<uint64_t>(byte_size);
				D_ASSERT(deserialized_value == value_arrays[length][i]);
			}
			total_count += count;
		}

#endif
		return bytes_written;
	}

private:
private:
	// 65536 bytes, might be better on heap?
	uint64_t ones[PatasPrimitives::PATAS_GROUP_SIZE];
	uint64_t twos[PatasPrimitives::PATAS_GROUP_SIZE];
	uint64_t threes[PatasPrimitives::PATAS_GROUP_SIZE];
	uint64_t fours[PatasPrimitives::PATAS_GROUP_SIZE];
	uint64_t fives[PatasPrimitives::PATAS_GROUP_SIZE];
	uint64_t sixes[PatasPrimitives::PATAS_GROUP_SIZE];
	uint64_t sevens[PatasPrimitives::PATAS_GROUP_SIZE];
	uint64_t eights[PatasPrimitives::PATAS_GROUP_SIZE];

	uint64_t *value_arrays[8];
	idx_t counters[8] = {0};
	idx_t index = 0;
};

} // namespace duckdb
