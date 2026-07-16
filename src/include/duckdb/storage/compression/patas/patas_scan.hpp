//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/storage/compression/patas/patas_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/chimp/chimp.hpp"
#include "duckdb/storage/compression/chimp/algorithm/packed_data.hpp"
#include "duckdb/storage/compression/chimp/algorithm/byte_reader.hpp"
#include "duckdb/storage/compression/patas/shared.hpp"
#include "duckdb/storage/compression/patas/algorithm/patas.hpp"
#include "duckdb/storage/compression/patas/patas.hpp"
#include "duckdb/storage/compression/compression_segment_reader.hpp"

#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

//! Do not change order of these variables
struct PatasUnpackedValueStats {
	uint8_t significant_bytes;
	uint8_t trailing_zeros;
	uint8_t index_diff;
};

template <class EXACT_TYPE>
struct PatasGroupState {
public:
	void Init(uint8_t *data, idx_t size) {
		byte_reader.SetStream(data, size);
	}

	idx_t BytesRead() const {
		return byte_reader.Index();
	}

	void Reset() {
		index = 0;
	}

	void LoadPackedData(uint16_t *packed_data, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			auto &unpacked = unpacked_data[i];
			PackedDataUtils<EXACT_TYPE>::Unpack(packed_data[i], (UnpackedData &)unpacked);
		}
	}

	template <bool SKIP = false>
	void Scan(uint8_t *dest, idx_t count) {
		if (!SKIP) {
			memcpy(dest, (void *)(values + index), sizeof(EXACT_TYPE) * count);
		}
		index += count;
	}

	template <bool SKIP>
	void LoadValues(EXACT_TYPE *value_buffer, idx_t count) {
		if (SKIP) {
			return;
		}
		value_buffer[0] = (EXACT_TYPE)0;
		for (idx_t i = 0; i < count; i++) {
			if (unpacked_data[i].index_diff > i) {
				throw IOException("Corrupted Patas segment: invalid backward reference");
			}
			if (unpacked_data[i].significant_bytes > sizeof(EXACT_TYPE) ||
			    unpacked_data[i].trailing_zeros >= sizeof(EXACT_TYPE) * 8) {
				throw IOException("Corrupted Patas segment: invalid packed value metadata");
			}

			value_buffer[i] = patas::PatasDecompression<EXACT_TYPE>::DecompressValue(
			    byte_reader, unpacked_data[i].significant_bytes, unpacked_data[i].trailing_zeros,
			    value_buffer[i - unpacked_data[i].index_diff]);
		}
	}

public:
	idx_t index;
	PatasUnpackedValueStats unpacked_data[PatasPrimitives::PATAS_GROUP_SIZE];
	EXACT_TYPE values[PatasPrimitives::PATAS_GROUP_SIZE];

private:
	ByteReader byte_reader;
};

template <class T>
struct PatasScanState : public SegmentScanState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;

	explicit PatasScanState(ColumnSegment &segment)
	    : reader(nullptr, 0, "Patas segment"), segment(segment), count(segment.count) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());

		handle = buffer_manager.Pin(segment.GetBlockHandle());
		// ScanStates never exceed the boundaries of a Segment,
		// but are not guaranteed to start at the beginning of the Block
		reader = CompressionSegmentReader(handle, segment, "Patas segment");
		segment_data = reader.GetSpan(0, reader.Size());
		auto metadata_offset = reader.ReadAt<uint32_t>(0);
		auto group_count = count / PatasPrimitives::PATAS_GROUP_SIZE;
		group_count += count % PatasPrimitives::PATAS_GROUP_SIZE != 0;
		if (group_count > reader.Size() / sizeof(uint32_t) || count > reader.Size() / sizeof(uint16_t)) {
			throw IOException("Corrupted Patas segment: metadata size is out of range");
		}
		auto metadata_size = group_count * sizeof(uint32_t) + count * sizeof(uint16_t);
		if (metadata_offset > reader.Size() || metadata_size > metadata_offset ||
		    metadata_offset - metadata_size < PatasPrimitives::HEADER_SIZE) {
			throw IOException("Corrupted Patas segment: metadata is out of range");
		}
		metadata_start = metadata_offset - metadata_size;
		metadata_position = metadata_offset;
	}

	BufferHandle handle;
	CompressionSegmentReader reader;
	idx_t metadata_start;
	idx_t metadata_position;
	data_ptr_t segment_data;
	idx_t total_value_count = 0;
	PatasGroupState<EXACT_TYPE> group_state;

	ColumnSegment &segment;
	idx_t count;

	idx_t LeftInGroup() const {
		return PatasPrimitives::PATAS_GROUP_SIZE - (total_value_count % PatasPrimitives::PATAS_GROUP_SIZE);
	}

	inline bool GroupFinished() const {
		return (total_value_count % PatasPrimitives::PATAS_GROUP_SIZE) == 0;
	}

	// Scan up to a group boundary
	template <class EXACT_TYPE, bool SKIP = false>
	void ScanGroup(EXACT_TYPE *values, idx_t group_size) {
		D_ASSERT(group_size <= PatasPrimitives::PATAS_GROUP_SIZE);
		D_ASSERT(group_size <= LeftInGroup());

		if (GroupFinished() && total_value_count < count) {
			if (group_size == PatasPrimitives::PATAS_GROUP_SIZE) {
				LoadGroup<SKIP>(values);
				total_value_count += group_size;
				return;
			} else {
				// Even if SKIP is given, group size is not big enough to be able to fully skip the entire group
				LoadGroup<false>(group_state.values);
			}
		}
		group_state.template Scan<SKIP>((uint8_t *)values, group_size);

		total_value_count += group_size;
	}

	// Using the metadata, we can avoid loading any of the data if we don't care about the group at all
	void SkipGroup() {
		// Skip the offset indicating where the data starts
		if (metadata_position < metadata_start + sizeof(uint32_t)) {
			throw IOException("Corrupted Patas segment: metadata is out of range");
		}
		metadata_position -= sizeof(uint32_t);
		auto data_byte_offset = reader.ReadAt<uint32_t>(metadata_position);
		if (data_byte_offset < PatasPrimitives::HEADER_SIZE || data_byte_offset >= metadata_start) {
			throw IOException("Corrupted Patas segment: data offset is out of range");
		}
		idx_t group_size = MinValue((idx_t)PatasPrimitives::PATAS_GROUP_SIZE, count - total_value_count);
		// Skip the blocks of packed data
		auto packed_size = sizeof(uint16_t) * group_size;
		if (packed_size > metadata_position - metadata_start) {
			throw IOException("Corrupted Patas segment: packed metadata is out of range");
		}
		metadata_position -= packed_size;

		total_value_count += group_size;
	}

	template <bool SKIP = false>
	void LoadGroup(EXACT_TYPE *value_buffer) {
		group_state.Reset();

		// Load the offset indicating where a groups data starts
		if (metadata_position < metadata_start + sizeof(uint32_t)) {
			throw IOException("Corrupted Patas segment: metadata is out of range");
		}
		metadata_position -= sizeof(uint32_t);
		auto data_byte_offset = reader.ReadAt<uint32_t>(metadata_position);

		idx_t group_size = MinValue((idx_t)PatasPrimitives::PATAS_GROUP_SIZE, (count - total_value_count));

		// Read the compacted blocks of (7 + 6 + 3 bits) value stats
		auto packed_size = sizeof(uint16_t) * group_size;
		if (packed_size > metadata_position - metadata_start) {
			throw IOException("Corrupted Patas segment: packed metadata is out of range");
		}
		metadata_position -= packed_size;
		group_state.LoadPackedData(reader.GetArray<uint16_t>(metadata_position, group_size), group_size);

		idx_t data_end = metadata_start;
		if (total_value_count + group_size < count) {
			if (metadata_position < metadata_start + sizeof(uint32_t)) {
				throw IOException("Corrupted Patas segment: next group metadata is out of range");
			}
			data_end = reader.ReadAt<uint32_t>(metadata_position - sizeof(uint32_t));
		}
		if (data_byte_offset < PatasPrimitives::HEADER_SIZE || data_end <= data_byte_offset ||
		    data_end > metadata_start) {
			throw IOException("Corrupted Patas segment: group data is out of range");
		}

		// Initialize the byte_reader with the data values for the group
		group_state.Init(reader.GetSpan(data_byte_offset, data_end - data_byte_offset), data_end - data_byte_offset);

		// Read all the values to the specified 'value_buffer'
		group_state.template LoadValues<SKIP>(value_buffer, group_size);
	}

public:
	//! Skip the next 'skip_count' values, we don't store the values
	void Skip(ColumnSegment &segment, idx_t skip_count) {
		using EXACT_TYPE = typename FloatingToExact<T>::TYPE;

		if (total_value_count != 0 && !GroupFinished()) {
			// Finish skipping the current group
			idx_t to_skip = LeftInGroup();
			skip_count -= to_skip;
			ScanGroup<EXACT_TYPE, true>(nullptr, to_skip);
		}
		// Figure out how many entire groups we can skip
		// For these groups, we don't even need to process the metadata or values
		idx_t groups_to_skip = skip_count / PatasPrimitives::PATAS_GROUP_SIZE;
		for (idx_t i = 0; i < groups_to_skip; i++) {
			SkipGroup();
		}
		skip_count -= PatasPrimitives::PATAS_GROUP_SIZE * groups_to_skip;
		if (skip_count == 0) {
			return;
		}
		// For the last group that this skip (partially) touches, we do need to
		// load the metadata and values into the group_state
		ScanGroup<EXACT_TYPE, true>(nullptr, skip_count);
	}
};

template <class T>
unique_ptr<SegmentScanState> PatasInitScan(const QueryContext &context, ColumnSegment &segment) {
	auto result = make_uniq_base<SegmentScanState, PatasScanState<T>>(segment);
	return result;
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void PatasScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                      idx_t result_offset) {
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;
	auto &scan_state = (PatasScanState<T> &)*state.scan_state;

	// Get the pointer to the result values
	auto current_result_ptr = FlatVector::GetDataMutableUnsafe<EXACT_TYPE>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	current_result_ptr += result_offset;

	idx_t scanned = 0;
	while (scanned < scan_count) {
		const auto remaining = scan_count - scanned;
		const idx_t to_scan = MinValue(remaining, scan_state.LeftInGroup());

		scan_state.template ScanGroup<EXACT_TYPE>(current_result_ptr + scanned, to_scan);
		scanned += to_scan;
	}
}

template <class T>
void PatasSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = (PatasScanState<T> &)*state.scan_state;
	scan_state.Skip(segment, skip_count);
}

template <class T>
void PatasScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	PatasScanPartial<T>(segment, state, scan_count, result, 0);
}

} // namespace duckdb
