#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/storage/compression/dict_fsst/decompression.hpp"
#include "fsst.h"
#include "duckdb/common/fsst.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"

namespace duckdb {
namespace dict_fsst {

CompressedStringScanState::~CompressedStringScanState() {
	delete reinterpret_cast<duckdb_fsst_decoder_t *>(decoder);
}

string_t CompressedStringScanState::FetchStringFromDict(Vector &result, uint32_t dict_offset, idx_t dict_idx) {
	if (dict_idx >= dict_count) {
		throw IOException("Corrupted DICT_FSST string segment: dictionary index is out of range");
	}
	if (dict_idx == 0) {
		return string_t(nullptr, 0);
	}
	uint32_t string_len = string_lengths[dict_idx];
	if (dict_offset > dictionary_size || string_len > dictionary_size - dict_offset) {
		throw IOException("Corrupted DICT_FSST string segment: dictionary string is out of range");
	}

	// normal string: read string from this block
	auto dict_pos = dict_ptr + dict_offset;

	auto str_ptr = char_ptr_cast(dict_pos);
	switch (mode) {
	case DictFSSTMode::FSST_ONLY:
	case DictFSSTMode::DICT_FSST: {
		if (string_len == 0) {
			return string_t(nullptr, 0);
		}
		if (all_values_inlined) {
			return FSSTPrimitives::DecompressInlinedValue(decoder, str_ptr, string_len);
		} else {
			return FSSTPrimitives::DecompressValue(decoder, StringVector::GetStringAllocator(result), str_ptr,
			                                       string_len);
		}
	}
	default:
		// FIXME: the Vector doesn't seem to take ownership of the non-inlined string data???
		return string_t(str_ptr, string_len);
	}
}

void CompressedStringScanState::Initialize(bool initialize_dictionary) {
	reader = CompressionSegmentReader(*handle, segment, "DICT_FSST string segment");
	reader.GetSpan(0, DictFSSTCompression::DICTIONARY_HEADER_SIZE);

	// Load header values
	mode = reader.ReadAt<DictFSSTMode>(offsetof(dict_fsst_compression_header_t, mode));
	if (mode >= DictFSSTMode::COUNT) {
		throw IOException("Corrupted DICT_FSST string segment: invalid compression mode");
	}

	dict_count = reader.ReadAt<uint32_t>(offsetof(dict_fsst_compression_header_t, dict_count));
	auto symbol_table_size = reader.ReadAt<uint32_t>(offsetof(dict_fsst_compression_header_t, symbol_table_size));
	dictionary_size = reader.ReadAt<uint32_t>(offsetof(dict_fsst_compression_header_t, dict_size));
	if (dict_count == 0) {
		throw IOException("Corrupted DICT_FSST string segment: dictionary count is zero");
	}

	dictionary_indices_width =
	    reader.ReadAt<uint8_t>(offsetof(dict_fsst_compression_header_t, dictionary_indices_width));
	string_lengths_width = reader.ReadAt<uint8_t>(offsetof(dict_fsst_compression_header_t, string_lengths_width));
	if (string_lengths_width > sizeof(uint32_t) * 8) {
		throw IOException("Corrupted DICT_FSST string segment: invalid string-length bit width");
	}
	if (mode == DictFSSTMode::FSST_ONLY) {
		if (dictionary_indices_width != 0 || dict_count != segment.count + 1) {
			throw IOException("Corrupted DICT_FSST string segment: invalid FSST-only dictionary metadata");
		}
	} else if (dictionary_indices_width != BitpackingPrimitives::MinimumBitWidth(dict_count - 1)) {
		throw IOException("Corrupted DICT_FSST string segment: invalid dictionary-index bit width");
	}

	auto string_lengths_space = BitpackingPrimitives::GetRequiredSize(dict_count, string_lengths_width);
	auto dictionary_indices_space =
	    BitpackingPrimitives::GetRequiredSize(segment.count.load(), dictionary_indices_width);

	auto layout = reader;
	layout.SetPosition(DictFSSTCompression::DICTIONARY_HEADER_SIZE);
	layout.Align(8);
	dict_ptr = layout.ReadSpan(dictionary_size);
	layout.Align(8);
	auto symbol_table_ptr = layout.ReadSpan(symbol_table_size);
	layout.Align(8);
	auto string_lengths_ptr = layout.ReadSpan(string_lengths_space);
	layout.Align(8);
	dictionary_indices_offset = layout.Position();
	dictionary_indices_size = dictionary_indices_space;
	layout.ReadSpan(dictionary_indices_space);
	if ((mode == DictFSSTMode::DICTIONARY && symbol_table_size != 0) ||
	    (mode != DictFSSTMode::DICTIONARY && symbol_table_size == 0)) {
		throw IOException("Corrupted DICT_FSST string segment: invalid FSST symbol-table size");
	}

	switch (mode) {
	case DictFSSTMode::FSST_ONLY:
	case DictFSSTMode::DICT_FSST: {
		decoder = new duckdb_fsst_decoder_t;
		auto ret = duckdb_fsst_import(reinterpret_cast<duckdb_fsst_decoder_t *>(decoder), symbol_table_ptr);
		if (ret == 0) {
			throw IOException("Failed to scan DICT_FSST string segment: invalid FSST symbol table. Database file "
			                  "appears to be corrupted.");
		}
		break;
	}
	default:
		break;
	}

	string_lengths.resize(AlignValue<uint32_t, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE>(dict_count));
	BitpackingPrimitives::UnPackBuffer<uint32_t>(data_ptr_cast(string_lengths.data()),
	                                             data_ptr_cast(string_lengths_ptr), dict_count, string_lengths_width);
	if (string_lengths[0] != 0) {
		throw IOException("Corrupted DICT_FSST string segment: NULL dictionary entry has a nonzero length");
	}
	uint32_t dictionary_bytes = 0;
	for (idx_t i = 0; i < dict_count; i++) {
		if (string_lengths[i] > dictionary_size - dictionary_bytes) {
			throw IOException("Corrupted DICT_FSST string segment: dictionary lengths are out of range");
		}
		dictionary_bytes += string_lengths[i];
	}
	if (dictionary_bytes != dictionary_size) {
		throw IOException("Corrupted DICT_FSST string segment: dictionary lengths do not match dictionary size");
	}
	if (!initialize_dictionary || mode == DictFSSTMode::FSST_ONLY) {
		// Used by fetch, as fetch will never produce a DictionaryVector
		return;
	}

	dictionary = DictionaryVector::CreateReusableDictionary(segment.GetType(), dict_count);
	auto &dict_data = dictionary->data;
	auto dict_child_data = FlatVector::GetDataMutable<string_t>(dict_data);
	auto &validity = FlatVector::ValidityMutable(dict_data);
	D_ASSERT(dict_count >= 1);
	validity.SetInvalid(0);

	uint32_t offset = 0;
	for (uint32_t i = 0; i < dict_count; i++) {
		//! We can uncompress during fetching, we need the length of the string inside the dictionary
		auto string_len = string_lengths[i];
		dict_child_data[i] = FetchStringFromDict(dict_data, offset, i);
		offset += string_len;
	}
}

const SelectionVector &CompressedStringScanState::GetSelVec(idx_t start, idx_t scan_count) {
	if (start > segment.count || scan_count > segment.count - start) {
		throw IOException("Corrupted DICT_FSST string segment: scan range is out of bounds");
	}
	switch (mode) {
	case DictFSSTMode::FSST_ONLY: {
		return *FlatVector::IncrementalSelectionVector();
	}
	default: {
		// Handling non-bitpacking-group-aligned start values;
		idx_t start_offset = start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		// We will scan in blocks of BITPACKING_ALGORITHM_GROUP_SIZE, so we may scan some extra values.
		idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count + start_offset);

		if (!sel_vec || sel_vec_size < decompress_count) {
			sel_vec_size = decompress_count;
			sel_vec = make_buffer<SelectionVector>(decompress_count);
		}

		auto source_offset = ((start - start_offset) * dictionary_indices_width) / 8;
		auto source_size = BitpackingPrimitives::GetRequiredSize(decompress_count, dictionary_indices_width);
		if (source_offset > dictionary_indices_size || source_size > dictionary_indices_size - source_offset) {
			throw IOException("Corrupted DICT_FSST string segment: selection data is out of range");
		}
		data_ptr_t sel_buf_src = reader.GetSpan(dictionary_indices_offset + source_offset, source_size);
		sel_t *sel_vec_ptr = sel_vec->data();
		BitpackingPrimitives::UnPackBuffer<sel_t>(data_ptr_cast(sel_vec_ptr), sel_buf_src, decompress_count,
		                                          dictionary_indices_width);

		if (start_offset != 0) {
			for (idx_t i = 0; i < scan_count; i++) {
				sel_vec->set_index(i, sel_vec->get_index(i + start_offset));
			}
		}
		for (idx_t i = 0; i < scan_count; i++) {
			if (sel_vec->get_index(i) >= dict_count) {
				throw IOException("Corrupted DICT_FSST string segment: selection index is out of range");
			}
		}

		return *sel_vec;
	}
	}
}

void CompressedStringScanState::ScanToFlatVector(Vector &result, idx_t result_offset, idx_t start, idx_t scan_count) {
	// Create a decompression buffer of sufficient size if we don't already have one.
	auto &selvec = GetSelVec(start, scan_count);

	//! (index 0 is reserved for NULL, which we don't have in this mode)
	const idx_t start_offset = mode == DictFSSTMode::FSST_ONLY ? start + 1 : 0;

	auto result_data = FlatVector::Writer<string_t>(result, scan_count, result_offset);
	if (dictionary) {
		// We have prepared the full dictionary, we can reference these strings directly
		auto dictionary_values = FlatVector::GetData<string_t>(dictionary->data);
		for (idx_t i = 0; i < scan_count; i++) {
			// Lookup dict offset in index buffer
			auto string_number = selvec.get_index(i + start_offset);
			if (string_number == 0) {
				result_data.WriteNull();
				continue;
			}
			result_data.WriteStringRef(dictionary_values[string_number]);
		}
	} else {
		for (idx_t i = 0; i < scan_count; i++) {
			// Lookup dict offset in index buffer
			auto string_number = selvec.get_index(start_offset + i);
			if (string_number == 0) {
				result_data.WriteNull();
				continue;
			}
			if (decompress_position > string_number) {
				throw InternalException("DICT_FSST: not performing a sequential scan?");
			}
			for (; decompress_position < string_number; decompress_position++) {
				decompress_offset += string_lengths[decompress_position];
			}
			result_data.WriteStringRef(FetchStringFromDict(result, decompress_offset, string_number));
		}
	}
	result.Verify();
}

void CompressedStringScanState::Select(Vector &result, idx_t start, const SelectionVector &sel, idx_t sel_count) {
	D_ASSERT(!dictionary);
	D_ASSERT(mode == DictFSSTMode::FSST_ONLY);
	idx_t start_offset = start + 1;
	auto result_data = FlatVector::Writer<string_t>(result, sel_count);
	for (idx_t i = 0; i < sel_count; i++) {
		// Lookup dict offset in index buffer
		auto string_number = start_offset + sel.get_index(i);
		if (decompress_position > string_number) {
			throw InternalException("DICT_FSST: not performing a sequential scan?");
		}
		for (; decompress_position < string_number; decompress_position++) {
			decompress_offset += string_lengths[decompress_position];
		}
		result_data.WriteValue(FetchStringFromDict(result, decompress_offset, string_number));
	}
}

bool CompressedStringScanState::AllowDictionaryScan(idx_t scan_count) {
	if (mode == DictFSSTMode::FSST_ONLY) {
		return false;
	}
	if (scan_count != STANDARD_VECTOR_SIZE) {
		return false;
	}
	if (!dictionary) {
		return false;
	}
	return true;
}

void CompressedStringScanState::ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset,
                                                       idx_t start, idx_t scan_count) {
	D_ASSERT(scan_count == STANDARD_VECTOR_SIZE);
	D_ASSERT(result_offset == 0);

	auto &selvec = GetSelVec(start, scan_count);
	result.Dictionary(dictionary, selvec, scan_count);
	result.Verify();
}

} // namespace dict_fsst
} // namespace duckdb
