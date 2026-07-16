//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alprd/alprd_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/alprd/algorithm/alprd.hpp"
#include "duckdb/storage/compression/alprd/alprd_constants.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/compression/compression_segment_reader.hpp"

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

template <class T>
struct AlpRDVectorState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;

	void Reset() {
		index = 0;
	}

	// Scan of the data itself
	template <bool SKIP = false>
	void Scan(uint8_t *dest, idx_t count) {
		if (!SKIP) {
			memcpy(dest, (void *)(decoded_values + index), sizeof(T) * count);
		}
		index += count;
	}

	template <bool SKIP>
	void LoadValues(EXACT_TYPE *values_buffer, idx_t count) {
		if (SKIP) {
			return;
		}
		values_buffer[0] = (EXACT_TYPE)0;
		alp::AlpRDDecompression<T>::Decompress(left_encoded, right_encoded, left_parts_dict, values_buffer, count,
		                                       exceptions_count, exceptions, exceptions_positions, left_bit_width,
		                                       right_bit_width);
	}

public:
	idx_t index;
	uint8_t left_encoded[AlpRDConstants::ALP_VECTOR_SIZE * 8];
	uint8_t right_encoded[AlpRDConstants::ALP_VECTOR_SIZE * 8];
	EXACT_TYPE decoded_values[AlpRDConstants::ALP_VECTOR_SIZE];
	uint16_t exceptions[AlpRDConstants::ALP_VECTOR_SIZE];
	uint16_t exceptions_positions[AlpRDConstants::ALP_VECTOR_SIZE];
	uint16_t exceptions_count;
	uint8_t right_bit_width;
	uint8_t left_bit_width;
	uint16_t left_parts_dict[AlpRDConstants::MAX_DICTIONARY_SIZE];
};

template <class T>
struct AlpRDScanState : public SegmentScanState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;

	explicit AlpRDScanState(ColumnSegment &segment)
	    : reader(nullptr, 0, "ALPRD segment"), metadata_reader(nullptr, 0, "ALPRD metadata"), segment(segment),
	      count(segment.count) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());

		handle = buffer_manager.Pin(segment.GetBlockHandle());
		// ScanStates never exceed the boundaries of a Segment,
		// but are not guaranteed to start at the beginning of the Block
		reader = CompressionSegmentReader(handle, segment, "ALPRD segment");
		auto metadata_offset = reader.ReadAt<uint32_t>(0);
		auto vector_count = count / AlpRDConstants::ALP_VECTOR_SIZE;
		vector_count += count % AlpRDConstants::ALP_VECTOR_SIZE != 0;
		if (vector_count > reader.Size() / AlpRDConstants::METADATA_POINTER_SIZE) {
			throw IOException("Corrupted ALPRD segment: metadata size is out of range");
		}
		auto metadata_size = vector_count * AlpRDConstants::METADATA_POINTER_SIZE;
		if (metadata_offset > reader.Size() || metadata_size > metadata_offset ||
		    metadata_offset - metadata_size < AlpRDConstants::HEADER_SIZE) {
			throw IOException("Corrupted ALPRD segment: metadata is out of range");
		}
		metadata_start = metadata_offset - metadata_size;
		metadata_reader = reader.SubReader(metadata_start, metadata_size, "ALPRD metadata");
		metadata_reader.SetPosition(metadata_reader.Size());

		reader.SetPosition(AlpRDConstants::METADATA_POINTER_SIZE);

		// Load the Right Bit Width which is in the segment header after the pointer to the first metadata
		vector_state.right_bit_width = reader.Read<uint8_t>();
		vector_state.left_bit_width = reader.Read<uint8_t>();
		uint8_t actual_dictionary_size = reader.Read<uint8_t>();
		if (vector_state.left_bit_width == 0 ||
		    vector_state.left_bit_width > AlpRDConstants::MAX_DICTIONARY_BIT_WIDTH ||
		    vector_state.right_bit_width == 0 || vector_state.right_bit_width >= sizeof(EXACT_TYPE) * 8) {
			throw IOException("Corrupted ALPRD segment: invalid bit width");
		}

		if (actual_dictionary_size > AlpRDConstants::MAX_DICTIONARY_SIZE) {
			throw IOException("Corrupt database file: ALPRD dictionary size exceeds maximum");
		}
		idx_t actual_dictionary_size_bytes =
		    static_cast<idx_t>(actual_dictionary_size) * AlpRDConstants::DICTIONARY_ELEMENT_SIZE;

		const idx_t left_parts_dict_max_size = sizeof(vector_state.left_parts_dict);
		if (actual_dictionary_size_bytes > left_parts_dict_max_size ||
		    actual_dictionary_size_bytes > metadata_start - reader.Position()) {
			throw IOException("Corrupted ALPRD segment: actual_dictionary_size is corrupted");
		}
		// Load the left parts dictionary which is after the segment header and is of a fixed size
		reader.CopyTo(data_ptr_cast(vector_state.left_parts_dict), actual_dictionary_size_bytes);
		data_start = reader.Position();
	}

	BufferHandle handle;
	CompressionSegmentReader reader;
	CompressionSegmentReader metadata_reader;
	idx_t metadata_start;
	idx_t data_start;
	idx_t total_value_count = 0;
	AlpRDVectorState<T> vector_state;

	ColumnSegment &segment;
	idx_t count;

	idx_t LeftInVector() const {
		return AlpRDConstants::ALP_VECTOR_SIZE - (total_value_count % AlpRDConstants::ALP_VECTOR_SIZE);
	}

	inline bool VectorFinished() const {
		return (total_value_count % AlpRDConstants::ALP_VECTOR_SIZE) == 0;
	}

	// Scan up to a vector boundary
	template <class EXACT_TYPE, bool SKIP = false>
	void ScanVector(EXACT_TYPE *values, idx_t vector_size) {
		D_ASSERT(vector_size <= AlpRDConstants::ALP_VECTOR_SIZE);
		D_ASSERT(vector_size <= LeftInVector());
		if (VectorFinished() && total_value_count < count) {
			if (vector_size == AlpRDConstants::ALP_VECTOR_SIZE) {
				LoadVector<SKIP>(values);
				total_value_count += vector_size;
				return;
			} else {
				// Even if SKIP is given, the vector size is not big enough to be able to fully skip the entire vector
				LoadVector<false>(vector_state.decoded_values);
			}
		}
		vector_state.template Scan<SKIP>((uint8_t *)values, vector_size);

		total_value_count += vector_size;
	}

	// Using the metadata, we can avoid loading any of the data if we don't care about the vector at all
	void SkipVector() {
		// Skip the offset indicating where the data starts
		auto data_byte_offset = metadata_reader.ReadBackward<uint32_t>();
		if (data_byte_offset < data_start || data_byte_offset >= metadata_start) {
			throw IOException("Corrupted ALPRD segment: vector data offset is out of range");
		}
		idx_t vector_size = MinValue((idx_t)AlpRDConstants::ALP_VECTOR_SIZE, count - total_value_count);
		total_value_count += vector_size;
	}

	template <bool SKIP = false>
	void LoadVector(EXACT_TYPE *value_buffer) {
		vector_state.Reset();

		// Load the offset (metadata) indicating where the vector data starts
		auto data_byte_offset = metadata_reader.ReadBackward<uint32_t>();

		idx_t vector_size = MinValue((idx_t)AlpRDConstants::ALP_VECTOR_SIZE, (count - total_value_count));
		idx_t data_end = metadata_start;
		if (total_value_count + vector_size < count) {
			if (metadata_reader.Position() < sizeof(uint32_t)) {
				throw IOException("Corrupted ALPRD segment: next vector metadata is out of range");
			}
			data_end = metadata_reader.ReadAt<uint32_t>(metadata_reader.Position() - sizeof(uint32_t));
		}
		if (data_byte_offset < data_start || data_end <= data_byte_offset || data_end > metadata_start) {
			throw IOException("Corrupted ALPRD segment: vector data is out of range");
		}
		auto vector_reader = reader.SubReader(data_byte_offset, data_end - data_byte_offset, "ALPRD vector");

		// Load the vector data
		vector_state.exceptions_count = vector_reader.template Read<uint16_t>();

		const bool uncompressed_mode = vector_state.exceptions_count == AlpRDConstants::UNCOMPRESSED_MODE_SENTINEL;
		if (uncompressed_mode) {
			const idx_t value_buffer_copy_size = sizeof(T) * vector_size;
			auto source = vector_reader.ReadSpan(value_buffer_copy_size);
			if (!SKIP) {
				memcpy(value_buffer, source, value_buffer_copy_size);
			}
			return;
		}
		if (vector_state.exceptions_count > vector_size) {
			throw IOException("Corrupted ALPRD segment: exceptions payload too large");
		}

		auto left_bp_size = BitpackingPrimitives::GetRequiredSize(vector_size, vector_state.left_bit_width);
		auto right_bp_size = BitpackingPrimitives::GetRequiredSize(vector_size, vector_state.right_bit_width);

		const idx_t max_left_encoded_size = sizeof(vector_state.left_encoded);
		if (left_bp_size > max_left_encoded_size) {
			throw IOException("Corrupted ALPRD segment: left_encoded payload too large");
		}
		vector_reader.CopyTo(vector_state.left_encoded, left_bp_size);

		const idx_t max_right_encoded_size = sizeof(vector_state.right_encoded);
		if (right_bp_size > max_right_encoded_size) {
			throw IOException("Corrupted ALPRD segment: right_encoded payload too large");
		}
		vector_reader.CopyTo(vector_state.right_encoded, right_bp_size);

		if (vector_state.exceptions_count > 0) {
			//! Load the exceptions
			const idx_t max_exceptions_size = sizeof(vector_state.exceptions);
			const idx_t exceptions_copy_size = AlpRDConstants::EXCEPTION_SIZE * vector_state.exceptions_count;
			if (exceptions_copy_size > max_exceptions_size) {
				throw IOException("Corrupted ALPRD segment: exceptions payload too large");
			}
			vector_reader.CopyTo(data_ptr_cast(vector_state.exceptions), exceptions_copy_size);

			//! Load the exceptions_positions
			const idx_t max_exceptions_positions_size = sizeof(vector_state.exceptions_positions);
			const idx_t exceptions_positions_copy_size =
			    AlpRDConstants::EXCEPTION_POSITION_SIZE * vector_state.exceptions_count;
			if (exceptions_positions_copy_size > max_exceptions_positions_size) {
				throw IOException("Corrupted ALPRD segment: exceptions_positions payload too large");
			}
			vector_reader.CopyTo(data_ptr_cast(vector_state.exceptions_positions), exceptions_positions_copy_size);
			for (idx_t i = 0; i < vector_state.exceptions_count; i++) {
				if (vector_state.exceptions_positions[i] >= vector_size) {
					throw IOException("Corrupted ALPRD segment: exception position is out of range");
				}
			}
		}

		// Decode all the vector values to the specified 'value_buffer'
		vector_state.template LoadValues<SKIP>(value_buffer, vector_size);
	}

public:
	//! Skip the next 'skip_count' values, we don't store the values
	void Skip(ColumnSegment &col_segment, idx_t skip_count) {
		if (total_value_count != 0 && !VectorFinished()) {
			// Finish skipping the current vector
			idx_t to_skip = MinValue<idx_t>(skip_count, LeftInVector());
			ScanVector<EXACT_TYPE, true>(nullptr, to_skip);
			skip_count -= to_skip;
		}
		// Figure out how many entire vectors we can skip
		// For these vectors, we don't even need to process the metadata or values
		idx_t vectors_to_skip = skip_count / AlpRDConstants::ALP_VECTOR_SIZE;
		for (idx_t i = 0; i < vectors_to_skip; i++) {
			SkipVector();
		}
		skip_count -= AlpRDConstants::ALP_VECTOR_SIZE * vectors_to_skip;
		if (skip_count == 0) {
			return;
		}
		// For the last vector that this skip (partially) touches, we do need to
		// load the metadata and values into the vector_state because
		// we don't know exactly how many they are
		ScanVector<EXACT_TYPE, true>(nullptr, skip_count);
	}
};

template <class T>
unique_ptr<SegmentScanState> AlpRDInitScan(const QueryContext &context, ColumnSegment &segment) {
	auto result = make_uniq_base<SegmentScanState, AlpRDScanState<T>>(segment);
	return result;
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void AlpRDScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                      idx_t result_offset) {
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;
	auto &scan_state = (AlpRDScanState<T> &)*state.scan_state;

	// Get the pointer to the result values
	auto current_result_ptr = FlatVector::GetDataMutableUnsafe<EXACT_TYPE>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	current_result_ptr += result_offset;

	idx_t scanned = 0;
	while (scanned < scan_count) {
		const auto remaining = scan_count - scanned;
		const idx_t to_scan = MinValue(remaining, scan_state.LeftInVector());

		scan_state.template ScanVector<EXACT_TYPE>(current_result_ptr + scanned, to_scan);
		scanned += to_scan;
	}
}

template <class T>
void AlpRDSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = (AlpRDScanState<T> &)*state.scan_state;
	scan_state.Skip(segment, skip_count);
}

template <class T>
void AlpRDScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	AlpRDScanPartial<T>(segment, state, scan_count, result, 0);
}

} // namespace duckdb
