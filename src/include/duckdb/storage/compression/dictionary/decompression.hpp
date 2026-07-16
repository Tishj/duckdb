#pragma once

#include "duckdb/storage/compression/dictionary/common.hpp"
#include "duckdb/storage/compression/compression_segment_reader.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
// FIXME: why is this StringScanState when we also define: `BufferHandle handle` ???
struct CompressedStringScanState : public StringScanState {
public:
	explicit CompressedStringScanState(BufferHandle &&handle_p)
	    : StringScanState(), owned_handle(std::move(handle_p)), handle(owned_handle),
	      reader(nullptr, 0, "dictionary-compressed string segment") {
	}
	explicit CompressedStringScanState(BufferHandle &handle_p)
	    : StringScanState(), owned_handle(), handle(handle_p),
	      reader(nullptr, 0, "dictionary-compressed string segment") {
	}

public:
	void Initialize(ColumnSegment &segment, bool initialize_dictionary = true);
	void ScanToFlatVector(Vector &result, idx_t result_offset, idx_t start, idx_t scan_count);
	void ScanToDictionaryVector(ColumnSegment &segment, Vector &result, idx_t result_offset, idx_t start,
	                            idx_t scan_count);

private:
	string_t FetchStringFromDict(int32_t dict_offset, uint16_t string_len);
	uint16_t GetStringLength(sel_t index);
	void ValidateDictionaryIndex(sel_t index);
	void ValidateDictionaryOffset(uint32_t dict_offset);

public:
	BufferHandle owned_handle;
	optional_ptr<BufferHandle> handle;
	CompressionSegmentReader reader;

	bitpacking_width_t current_width;
	buffer_ptr<SelectionVector> sel_vec;
	idx_t sel_vec_size = 0;
	idx_t selection_buffer_size = 0;

	//! Start of the data (pointing to the start of the selection buffer)
	data_ptr_t base_data;
	data_ptr_t dictionary_data;
	uint32_t *index_buffer_ptr;
	uint32_t index_buffer_count;

	buffer_ptr<DictionaryEntry> dictionary;
	idx_t dictionary_size;
	StringDictionaryContainer dict;
	idx_t block_size;
};

} // namespace duckdb
