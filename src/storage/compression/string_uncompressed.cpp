#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/storage/string_uncompressed.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/compression/compression_segment_reader.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

struct UncompressedStringLayout {
	UncompressedStringLayout(BufferHandle &handle, ColumnSegment &segment)
	    : reader(handle, segment, "uncompressed string segment"), base(reader.GetSpan(0, reader.Size())) {
		reader.GetSpan(0, UncompressedStringStorage::DICTIONARY_HEADER_SIZE);
		dictionary.size = reader.ReadAt<uint32_t>(0);
		dictionary.end = reader.ReadAt<uint32_t>(sizeof(uint32_t));
		offsets = reader.GetArray<int32_t>(UncompressedStringStorage::DICTIONARY_HEADER_SIZE, segment.count);
		auto offsets_end = UncompressedStringStorage::DICTIONARY_HEADER_SIZE + segment.count * sizeof(int32_t);
		if (dictionary.end > reader.Size() || dictionary.size > dictionary.end ||
		    dictionary.end - dictionary.size < offsets_end) {
			throw IOException("Corrupted uncompressed string segment: dictionary is out of range");
		}
		reader.GetSpan(dictionary.end - dictionary.size, dictionary.size);
	}

	uint32_t AbsoluteOffset(int32_t offset) const {
		if (offset == NumericLimits<int32_t>::Minimum()) {
			throw IOException("Corrupted uncompressed string segment: invalid dictionary offset");
		}
		return offset < 0 ? UnsafeNumericCast<uint32_t>(-static_cast<int64_t>(offset))
		                  : UnsafeNumericCast<uint32_t>(offset);
	}

	uint32_t StringLength(idx_t index) const {
		auto current = AbsoluteOffset(offsets[index]);
		auto previous = index == 0 ? 0 : AbsoluteOffset(offsets[index - 1]);
		if (current < previous || current > dictionary.size) {
			throw IOException("Corrupted uncompressed string segment: dictionary offset is out of range");
		}
		auto length = current - previous;
		if (offsets[index] < 0 && length != 0 && length != UncompressedStringStorage::BIG_STRING_MARKER_SIZE) {
			throw IOException("Corrupted uncompressed string segment: invalid overflow string marker");
		}
		return length;
	}

	CompressionSegmentReader reader;
	data_ptr_t base;
	int32_t *offsets;
	StringDictionaryContainer dictionary;
};

//===--------------------------------------------------------------------===//
// Storage Class
//===--------------------------------------------------------------------===//
UncompressedStringSegmentState::~UncompressedStringSegmentState() {
	while (head) {
		// prevent deep recursion here
		head = std::move(head->next);
	}
}

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct StringAnalyzeState : public AnalyzeState {
	explicit StringAnalyzeState(BlockManager &block_manager)
	    : AnalyzeState(block_manager), count(0), total_string_size(0), overflow_strings(0) {
	}

	idx_t count;
	idx_t total_string_size;
	idx_t overflow_strings;
};

unique_ptr<AnalyzeState> UncompressedStringStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<StringAnalyzeState>(col_data.GetBlockManager());
}

bool UncompressedStringStorage::StringAnalyze(AnalyzeState &state_p, const Vector &input) {
	auto &state = state_p.Cast<StringAnalyzeState>();
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(vdata);

	const auto count = input.size();
	state.count += count;
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx)) {
			auto string_size = data[idx].GetSize();
			state.total_string_size += string_size;
			if (string_size >= StringUncompressed::GetStringBlockLimit(state.info.GetBlockSize())) {
				state.overflow_strings++;
			}
		}
	}
	return true;
}

idx_t UncompressedStringStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<StringAnalyzeState>();
	return state.count * sizeof(int32_t) + state.total_string_size + state.overflow_strings * BIG_STRING_MARKER_SIZE;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void UncompressedStringInitPrefetch(ColumnSegment &segment, PrefetchState &prefetch_state) {
	prefetch_state.AddBlock(segment.GetBlockHandle());
	auto segment_state = segment.GetSegmentState();
	if (segment_state) {
		auto &state = segment_state->Cast<UncompressedStringSegmentState>();
		auto &block_manager = segment.GetBlockHandle()->GetBlockManager();
		for (auto &block_id : state.on_disk_blocks) {
			auto block_handle = state.GetHandle(block_manager, block_id);
			prefetch_state.AddBlock(block_handle);
		}
	}
}

unique_ptr<SegmentScanState> UncompressedStringStorage::StringInitScan(const QueryContext &context,
                                                                       ColumnSegment &segment) {
	auto result = make_uniq<StringScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	result->handle = buffer_manager.Pin(segment.GetBlockHandle());
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void UncompressedStringStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                                  Vector &result, idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = state.scan_state->Cast<StringScanState>();
	auto start = state.GetPositionInSegment();

	UncompressedStringLayout layout(scan_state.handle, segment);
	if (start > segment.count || scan_count > segment.count - start) {
		throw IOException("Corrupted uncompressed string segment: scan is out of range");
	}
	auto result_data = FlatVector::GetDataMutable<string_t>(result);

	for (idx_t i = 0; i < scan_count; i++) {
		auto index = start + i;
		auto current_offset = layout.offsets[index];
		auto string_length = layout.StringLength(index);
		result_data[result_offset + i] =
		    FetchStringFromDict(segment, layout.dictionary.end, result, layout.base, current_offset, string_length);
	}
}

void UncompressedStringStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                           Vector &result) {
	StringScanPartial(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
void UncompressedStringStorage::Select(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count,
                                       Vector &result, const SelectionVector &sel, idx_t sel_count) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = state.scan_state->Cast<StringScanState>();
	auto start = state.GetPositionInSegment();

	UncompressedStringLayout layout(scan_state.handle, segment);
	if (start > segment.count || vector_count > segment.count - start) {
		throw IOException("Corrupted uncompressed string segment: selection is out of range");
	}
	auto result_data = FlatVector::GetDataMutable<string_t>(result);

	for (idx_t i = 0; i < sel_count; i++) {
		auto selected_index = sel.get_index(i);
		if (selected_index >= vector_count) {
			throw IOException("Corrupted uncompressed string segment: selection index is out of range");
		}
		idx_t index = start + selected_index;
		auto current_offset = layout.offsets[index];
		auto string_length = layout.StringLength(index);
		result_data[i] =
		    FetchStringFromDict(segment, layout.dictionary.end, result, layout.base, current_offset, string_length);
	}
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
BufferHandle &ColumnFetchState::GetOrInsertHandle(ColumnSegment &segment) {
	auto primary_id = segment.GetBlockHandle()->BlockId();

	auto entry = handles.find(primary_id);
	if (entry == handles.end()) {
		// not pinned yet: pin it
		auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
		auto handle = buffer_manager.Pin(segment.GetBlockHandle());
		auto pinned_entry = handles.insert(make_pair(primary_id, std::move(handle)));
		return pinned_entry.first->second;
	} else {
		// already pinned: use the pinned handle
		return entry->second;
	}
}

void UncompressedStringStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id,
                                               Vector &result, idx_t result_idx) {
	// fetch a single row from the string segment
	// first pin the main buffer if it is not already pinned
	auto &handle = state.GetOrInsertHandle(segment);

	UncompressedStringLayout layout(handle, segment);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);

	if (row_id < 0 || NumericCast<idx_t>(row_id) >= segment.count) {
		throw IOException("Corrupted uncompressed string segment: row id is out of range");
	}
	auto index = NumericCast<idx_t>(row_id);
	auto dict_offset = layout.offsets[index];
	auto string_length = layout.StringLength(index);
	result_data[result_idx] =
	    FetchStringFromDict(segment, layout.dictionary.end, result, layout.base, dict_offset, string_length);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
SerializedStringSegmentState::SerializedStringSegmentState() {
}

SerializedStringSegmentState::SerializedStringSegmentState(vector<block_id_t> blocks_p) {
	blocks = std::move(blocks_p);
}

void SerializedStringSegmentState::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(1, "overflow_blocks", blocks);
}

unique_ptr<CompressedSegmentState>
UncompressedStringStorage::StringInitSegment(ColumnSegment &segment, block_id_t block_id,
                                             optional_ptr<ColumnSegmentState> segment_state) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	if (block_id == INVALID_BLOCK) {
		auto handle = buffer_manager.Pin(segment.GetBlockHandle());
		StringDictionaryContainer dictionary;
		dictionary.size = 0;
		dictionary.end = UnsafeNumericCast<uint32_t>(segment.SegmentSize());
		SetDictionary(segment, handle, dictionary);
	}
	auto result = make_uniq<UncompressedStringSegmentState>();
	if (segment_state) {
		auto &serialized_state = segment_state->Cast<SerializedStringSegmentState>();
		result->on_disk_blocks = std::move(serialized_state.blocks);
	}
	return std::move(result);
}

idx_t UncompressedStringStorage::FinalizeAppend(ColumnSegment &segment, BaseStatistics &) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto handle = buffer_manager.Pin(segment.GetBlockHandle());
	auto dict = GetDictionary(segment, handle);
	D_ASSERT(dict.end == segment.SegmentSize());
	// compute the total size required to store this segment
	auto offset_size = DICTIONARY_HEADER_SIZE + segment.count * sizeof(int32_t);
	auto total_size = offset_size + dict.size;

	CompressionInfo info(segment.GetBlockHandle()->GetBlockManager());
	if (total_size >= info.GetCompactionFlushLimit()) {
		// the block is full enough, don't bother moving around the dictionary
		return segment.SegmentSize();
	}

	// the block has space left: figure out how much space we can save
	auto move_amount = segment.SegmentSize() - total_size;
	// move the dictionary so it lines up exactly with the offsets
	auto dataptr = handle.GetDataMutable();
	memmove(dataptr + offset_size, dataptr + dict.end - dict.size, dict.size);
	dict.end -= move_amount;
	D_ASSERT(dict.end == total_size);
	// write the new dictionary (with the updated "end")
	SetDictionary(segment, handle, dict);
	return total_size;
}

//===--------------------------------------------------------------------===//
// Serialization & Cleanup
//===--------------------------------------------------------------------===//
unique_ptr<ColumnSegmentState> UncompressedStringStorage::SerializeState(ColumnSegment &segment) {
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
	if (state.on_disk_blocks.empty()) {
		// no on-disk blocks - nothing to write
		return nullptr;
	}
	return make_uniq<SerializedStringSegmentState>(state.on_disk_blocks);
}

unique_ptr<ColumnSegmentState> UncompressedStringStorage::DeserializeState(Deserializer &deserializer) {
	auto result = make_uniq<SerializedStringSegmentState>();
	deserializer.ReadProperty(1, "overflow_blocks", result->blocks);
	return std::move(result);
}

void UncompressedStringStorage::VisitBlockIds(const ColumnSegment &segment, BlockIdVisitor &visitor) {
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
	for (auto &block_id : state.on_disk_blocks) {
		visitor.Visit(block_id);
	}
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction StringUncompressed::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::VARCHAR);
	return CompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, data_type,
	                           UncompressedStringStorage::StringInitAnalyze, UncompressedStringStorage::StringAnalyze,
	                           UncompressedStringStorage::StringFinalAnalyze, UncompressedFunctions::InitCompression,
	                           UncompressedFunctions::Compress, UncompressedFunctions::FinalizeCompress,
	                           UncompressedStringStorage::StringInitScan, UncompressedStringStorage::StringScan,
	                           UncompressedStringStorage::StringScanPartial, UncompressedStringStorage::StringFetchRow,
	                           UncompressedFunctions::EmptySkip, UncompressedStringStorage::StringInitSegment,
	                           UncompressedStringStorage::StringInitAppend, UncompressedStringStorage::StringAppend,
	                           UncompressedStringStorage::FinalizeAppend, UncompressedStringStorage::StringRevertAppend,
	                           UncompressedStringStorage::SerializeState, UncompressedStringStorage::DeserializeState,
	                           UncompressedStringStorage::VisitBlockIds, UncompressedStringInitPrefetch,
	                           UncompressedStringStorage::Select);
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
void UncompressedStringStorage::SetDictionary(ColumnSegment &segment, BufferHandle &handle,
                                              StringDictionaryContainer container) {
	auto startptr = handle.GetDataMutable() + segment.GetBlockOffset();
	Store<uint32_t>(container.size, startptr);
	Store<uint32_t>(container.end, startptr + sizeof(uint32_t));
}

StringDictionaryContainer UncompressedStringStorage::GetDictionary(ColumnSegment &segment, BufferHandle &handle) {
	auto startptr = handle.GetDataMutable() + segment.GetBlockOffset();
	StringDictionaryContainer container;
	container.size = Load<uint32_t>(startptr);
	container.end = Load<uint32_t>(startptr + sizeof(uint32_t));
	return container;
}

uint32_t UncompressedStringStorage::GetDictionaryEnd(ColumnSegment &segment, BufferHandle &handle) {
	auto startptr = handle.GetDataMutable() + segment.GetBlockOffset();
	return Load<uint32_t>(startptr + sizeof(uint32_t));
}

idx_t UncompressedStringStorage::RemainingSpace(ColumnSegment &segment, BufferHandle &handle) {
	auto dictionary = GetDictionary(segment, handle);
	D_ASSERT(dictionary.end == segment.SegmentSize());
	idx_t used_space = dictionary.size + segment.count * sizeof(int32_t) + DICTIONARY_HEADER_SIZE;
	D_ASSERT(segment.SegmentSize() >= used_space);
	return segment.SegmentSize() - used_space;
}

void UncompressedStringStorage::WriteString(ColumnSegment &segment, string_t string, block_id_t &result_block,
                                            int32_t &result_offset) {
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
	if (state.overflow_writer) {
		// overflow writer is set: write string there
		state.overflow_writer->WriteString(state, string, result_block, result_offset);
	} else {
		// default overflow behavior: use in-memory buffer to store the overflow string
		WriteStringMemory(segment, string, result_block, result_offset);
	}
}

void UncompressedStringStorage::WriteStringMemory(ColumnSegment &segment, string_t string, block_id_t &result_block,
                                                  int32_t &result_offset) {
	auto total_length = UnsafeNumericCast<uint32_t>(string.GetSize() + sizeof(uint32_t));
	shared_ptr<BlockHandle> block;
	BufferHandle handle;

	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
	// check if the string fits in the current block
	if (!state.head || state.head->offset + total_length >= state.head->size) {
		// string does not fit, allocate space for it
		// create a new string block
		auto alloc_size = MaxValue<idx_t>(total_length, segment.GetBlockSize());
		auto new_block = make_uniq<StringBlock>();
		new_block->offset = 0;
		new_block->size = alloc_size;
		// allocate an in-memory buffer for it
		handle = buffer_manager.Allocate(MemoryTag::OVERFLOW_STRINGS, alloc_size, false);
		block = handle.GetBlockHandle();
		state.InsertOverflowBlock(block->BlockId(), reference<StringBlock>(*new_block));
		new_block->block = std::move(block);
		new_block->next = std::move(state.head);
		state.head = std::move(new_block);
	} else {
		// string fits, copy it into the current block
		handle = buffer_manager.Pin(state.head->block);
	}

	result_block = state.head->block->BlockId();
	result_offset = UnsafeNumericCast<int32_t>(state.head->offset);

	// copy the string and the length there
	auto ptr = handle.GetDataMutable() + state.head->offset;
	Store<uint32_t>(UnsafeNumericCast<uint32_t>(string.GetSize()), ptr);
	ptr += sizeof(uint32_t);
	memcpy(ptr, string.GetData(), string.GetSize());
	state.head->offset += total_length;
}

string_t UncompressedStringStorage::ReadOverflowString(ColumnSegment &segment, Vector &result, block_id_t block,
                                                       int32_t offset) {
	auto &buffer_manager = segment.GetBlockHandle()->GetMemory().GetBufferManager();
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();

	if (block == INVALID_BLOCK || offset < 0 || NumericCast<idx_t>(offset) >= segment.GetBlockSize()) {
		throw IOException("Corrupted overflow string marker: block or offset is invalid");
	}

	if (block < MAXIMUM_BLOCK) {
		// read the overflow string from disk
		// pin the initial handle and read the length
		auto block_handle = state.GetHandle(segment.GetBlockHandle()->GetBlockManager(), block);
		auto handle = buffer_manager.Pin(block_handle);
		CompressionSegmentReader overflow_reader(handle.GetDataMutable(), segment.GetBlockSize(),
		                                         "overflow string block");

		// read header
		uint32_t length = overflow_reader.ReadAt<uint32_t>(UnsafeNumericCast<idx_t>(offset));
		uint32_t remaining = length;
		offset += sizeof(uint32_t);

		BufferHandle target_handle;
		string_t overflow_string;
		data_ptr_t target_ptr;
		bool allocate_block = length >= segment.GetBlockSize();
		if (allocate_block) {
			// overflow string is bigger than a block - allocate a temporary buffer for it
			target_handle = buffer_manager.Allocate(MemoryTag::OVERFLOW_STRINGS, length);
			target_ptr = target_handle.GetDataMutable();
		} else {
			// overflow string is smaller than a block - add it to the vector directly
			overflow_string = StringVector::EmptyString(result, length);
			target_ptr = data_ptr_cast(overflow_string.GetDataWriteable());
		}

		// now append the string to the single buffer
		while (remaining > 0) {
			auto current_offset = UnsafeNumericCast<idx_t>(offset);
			if (current_offset > segment.GetBlockSize() - sizeof(block_id_t)) {
				throw IOException("Corrupted overflow string block: payload offset is out of range");
			}
			idx_t to_write = MinValue<idx_t>(remaining, segment.GetBlockSize() - sizeof(block_id_t) - current_offset);
			memcpy(target_ptr, overflow_reader.GetSpan(current_offset, to_write), to_write);
			remaining -= to_write;
			offset += UnsafeNumericCast<int32_t>(to_write);
			target_ptr += to_write;
			if (remaining > 0) {
				// read the next block
				block_id_t next_block = overflow_reader.ReadAt<block_id_t>(UnsafeNumericCast<idx_t>(offset));
				block_handle = state.GetHandle(segment.GetBlockHandle()->GetBlockManager(), next_block);
				handle = buffer_manager.Pin(block_handle);
				overflow_reader =
				    CompressionSegmentReader(handle.GetDataMutable(), segment.GetBlockSize(), "overflow string block");
				offset = 0;
			}
		}
		if (allocate_block) {
			auto final_buffer = target_handle.GetDataMutable();
			StringVector::AddHandle(result, std::move(target_handle));
			return ReadString(final_buffer, 0, length);
		} else {
			overflow_string.Finalize();
			return overflow_string;
		}
	}

	// read the overflow string from memory
	// first pin the handle, if it is not pinned yet
	auto string_block = state.FindOverflowBlock(block);
	auto handle = buffer_manager.Pin(string_block.get().block);
	CompressionSegmentReader overflow_reader(handle.GetDataMutable(), string_block.get().size,
	                                         "in-memory overflow string block");
	auto string_offset = UnsafeNumericCast<idx_t>(offset);
	auto string_length = overflow_reader.ReadAt<uint32_t>(string_offset);
	overflow_reader.GetSpan(string_offset + sizeof(uint32_t), string_length);
	auto final_buffer = overflow_reader.GetSpan(0, overflow_reader.Size());
	StringVector::AddHandle(result, std::move(handle));
	return ReadStringWithLength(final_buffer, offset);
}

string_t UncompressedStringStorage::ReadString(data_ptr_t target, int32_t offset, uint32_t string_length) {
	auto ptr = target + offset;
	auto str_ptr = char_ptr_cast(ptr);
	return string_t(str_ptr, string_length);
}

string_t UncompressedStringStorage::ReadStringWithLength(data_ptr_t target, int32_t offset) {
	auto ptr = target + offset;
	auto str_length = Load<uint32_t>(ptr);
	auto str_ptr = char_ptr_cast(ptr + sizeof(uint32_t));
	return string_t(str_ptr, str_length);
}

void UncompressedStringStorage::WriteStringMarker(data_ptr_t target, block_id_t block_id, int32_t offset) {
	memcpy(target, &block_id, sizeof(block_id_t));
	target += sizeof(block_id_t);
	memcpy(target, &offset, sizeof(int32_t));
}

void UncompressedStringStorage::ReadStringMarker(data_ptr_t target, block_id_t &block_id, int32_t &offset) {
	memcpy(&block_id, target, sizeof(block_id_t));
	target += sizeof(block_id_t);
	memcpy(&offset, target, sizeof(int32_t));
}

} // namespace duckdb
