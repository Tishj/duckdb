#include "duckdb/storage/compression/dictionary/decompression.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

void CompressedStringScanState::ValidateDictionaryIndex(sel_t index) {
	if (index >= index_buffer_count) {
		throw IOException("Failed to scan dictionary string - dictionary index was out of range. Database file appears "
		                  "to be corrupted.");
	}
}

void CompressedStringScanState::ValidateDictionaryOffset(uint32_t dict_offset) {
	if (dict_offset > dict.size) {
		throw IOException(
		    "Failed to scan dictionary string - dictionary offset was out of range. Database file appears "
		    "to be corrupted.");
	}
}

uint16_t CompressedStringScanState::GetStringLength(sel_t index) {
	ValidateDictionaryIndex(index);
	if (index == 0) {
		return 0;
	} else {
		auto dict_offset = index_buffer_ptr[index];
		auto previous_dict_offset = index_buffer_ptr[index - 1];
		ValidateDictionaryOffset(dict_offset);
		ValidateDictionaryOffset(previous_dict_offset);
		if (dict_offset < previous_dict_offset) {
			throw IOException("Failed to scan dictionary string - dictionary offset was out of range. Database file "
			                  "appears to be corrupted.");
		}
		auto string_length = dict_offset - previous_dict_offset;
		if (string_length > NumericLimits<uint16_t>::Maximum()) {
			throw IOException("Failed to scan dictionary string - dictionary offset was out of range. Database file "
			                  "appears to be corrupted.");
		}
		return UnsafeNumericCast<uint16_t>(string_length);
	}
}

string_t CompressedStringScanState::FetchStringFromDict(int32_t dict_offset, uint16_t string_len) {
	if (dict_offset < 0) {
		throw IOException(
		    "Failed to scan dictionary string - dictionary offset was out of range. Database file appears "
		    "to be corrupted.");
	}
	ValidateDictionaryOffset(UnsafeNumericCast<uint32_t>(dict_offset));
	if (dict_offset == 0) {
		return string_t(nullptr, 0);
	}

	// normal string: read string from this block
	auto dict_pos = dictionary_data + dict.size - dict_offset;

	auto str_ptr = char_ptr_cast(dict_pos);
	return string_t(str_ptr, string_len);
}

void CompressedStringScanState::Initialize(ColumnSegment &segment, bool initialize_dictionary) {
	reader = CompressionSegmentReader(*handle, segment, "dictionary-compressed string segment");
	block_size = reader.Size();
	reader.GetSpan(0, DictionaryCompression::DICTIONARY_HEADER_SIZE);

	// Load header values
	auto index_buffer_offset = reader.ReadAt<uint32_t>(offsetof(dictionary_compression_header_t, index_buffer_offset));
	index_buffer_count = reader.ReadAt<uint32_t>(offsetof(dictionary_compression_header_t, index_buffer_count));
	auto stored_width = reader.ReadAt<uint32_t>(offsetof(dictionary_compression_header_t, bitpacking_width));
	if (index_buffer_count == 0) {
		throw IOException(
		    "Failed to scan dictionary string - dictionary was out of range. Database file appears to be corrupted.");
	}
	auto expected_width = BitpackingPrimitives::MinimumBitWidth(index_buffer_count - 1);
	if (stored_width != expected_width) {
		throw IOException(
		    "Failed to scan dictionary string - bitpacking width was invalid. Database file appears to be "
		    "corrupted.");
	}
	current_width = expected_width;
	selection_buffer_size = BitpackingPrimitives::GetRequiredSize(segment.count.load(), current_width);
	auto expected_index_buffer_offset = DictionaryCompression::DICTIONARY_HEADER_SIZE + selection_buffer_size;
	if (index_buffer_offset != expected_index_buffer_offset) {
		throw IOException("Failed to scan dictionary string - selection buffer was out of range. Database file appears "
		                  "to be corrupted.");
	}
	base_data = reader.GetSpan(DictionaryCompression::DICTIONARY_HEADER_SIZE, selection_buffer_size);
	index_buffer_ptr = reader.GetArray<uint32_t>(index_buffer_offset, index_buffer_count);

	dict.size = reader.ReadAt<uint32_t>(offsetof(dictionary_compression_header_t, dict_size));
	dict.end = reader.ReadAt<uint32_t>(offsetof(dictionary_compression_header_t, dict_end));
	auto index_buffer_end = index_buffer_offset + sizeof(uint32_t) * index_buffer_count;
	if (dict.end > reader.Size() || dict.size > dict.end || dict.end - dict.size < index_buffer_end) {
		throw IOException(
		    "Failed to scan dictionary string - dictionary was out of range. Database file appears to be corrupted.");
	}
	dictionary_data = reader.GetSpan(dict.end - dict.size, dict.size);
	if (!initialize_dictionary) {
		// Used by fetch, as fetch will never produce a DictionaryVector
		return;
	}

	dictionary = DictionaryVector::CreateReusableDictionary(segment.GetType(), index_buffer_count);
	dictionary_size = index_buffer_count;
	auto dict_child_data = FlatVector::Writer<string_t>(dictionary->data, index_buffer_count);
	dict_child_data.WriteNull();
	for (uint32_t i = 1; i < index_buffer_count; i++) {
		// NOTE: the passing of dict_child_vector, will not be used, its for big strings
		uint16_t str_len = GetStringLength(i);
		dict_child_data.WriteStringRef(FetchStringFromDict(UnsafeNumericCast<int32_t>(index_buffer_ptr[i]), str_len));
	}
}

void CompressedStringScanState::ScanToFlatVector(Vector &result, idx_t result_offset, idx_t start, idx_t scan_count) {
	// Handling non-bitpacking-group-aligned start values;
	idx_t start_offset = start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

	// We will scan in blocks of BITPACKING_ALGORITHM_GROUP_SIZE, so we may scan some extra values.
	idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count + start_offset);

	// Create a decompression buffer of sufficient size if we don't already have one.
	if (!sel_vec || sel_vec_size < decompress_count) {
		sel_vec_size = decompress_count;
		sel_vec = make_buffer<SelectionVector>(decompress_count);
	}

	auto source_offset = ((start - start_offset) * current_width) / 8;
	auto source_size = BitpackingPrimitives::GetRequiredSize(decompress_count, current_width);
	data_ptr_t src = reader.GetSpan(DictionaryCompression::DICTIONARY_HEADER_SIZE + source_offset, source_size);
	sel_t *sel_vec_ptr = sel_vec->data();

	BitpackingPrimitives::UnPackBuffer<sel_t>(data_ptr_cast(sel_vec_ptr), src, decompress_count, current_width);

	auto result_data = FlatVector::Writer<string_t>(result, scan_count, result_offset);
	for (idx_t i = 0; i < scan_count; i++) {
		// Lookup dict offset in index buffer
		auto string_number = sel_vec->get_index(i + start_offset);
		ValidateDictionaryIndex(string_number);
		auto dict_offset = index_buffer_ptr[string_number];
		auto str_len = GetStringLength(UnsafeNumericCast<sel_t>(string_number));
		result_data.WriteStringRef(FetchStringFromDict(UnsafeNumericCast<int32_t>(dict_offset), str_len));
	}
}

void CompressedStringScanState::ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset,
                                                       idx_t start, idx_t scan_count) {
	D_ASSERT(scan_count == STANDARD_VECTOR_SIZE);
	D_ASSERT(result_offset == 0);

	idx_t start_offset = start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
	idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count + start_offset);

	// Create a selection vector of sufficient size if we don't already have one.
	if (!sel_vec || sel_vec_size < decompress_count) {
		sel_vec_size = decompress_count;
		sel_vec = make_buffer<SelectionVector>(decompress_count);
	}

	// Scanning 2048 values, emitting a dict vector
	data_ptr_t dst = data_ptr_cast(sel_vec->data());
	auto source_offset = ((start - start_offset) * current_width) / 8;
	auto source_size = BitpackingPrimitives::GetRequiredSize(decompress_count, current_width);
	data_ptr_t src = reader.GetSpan(DictionaryCompression::DICTIONARY_HEADER_SIZE + source_offset, source_size);

	BitpackingPrimitives::UnPackBuffer<sel_t>(dst, src, decompress_count, current_width);

	if (start_offset != 0) {
		for (idx_t i = 0; i < scan_count; i++) {
			sel_vec->set_index(i, sel_vec->get_index(i + start_offset));
		}
	}
	for (idx_t i = 0; i < scan_count; i++) {
		ValidateDictionaryIndex(sel_vec->get_index(i));
	}

	result.Dictionary(dictionary, *sel_vec, scan_count);
}

} // namespace duckdb
