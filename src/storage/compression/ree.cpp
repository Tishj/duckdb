#include "duckdb/function/compression/compression.hpp"

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include <functional>

namespace duckdb {

using ree_count_t = uint16_t;

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct EmptyREEWriter {
	template <class VALUE_TYPE>
	static void Operation(VALUE_TYPE value, ree_count_t count, void *dataptr, bool is_null) {
	}
};

template <class T>
struct REEState {
	REEState() : seen_count(0), last_value(NullValue<T>()), last_seen_count(0), dataptr(nullptr) {
	}

	idx_t seen_count;
	T last_value;
	ree_count_t last_seen_count;
	void *dataptr;
	bool all_null = true;

public:
	template <class OP>
	void Flush() {
		OP::template Operation<T>(last_value, last_seen_count, dataptr, all_null);
	}

	template <class OP = EmptyREEWriter>
	void Update(const T *data, ValidityMask &validity, idx_t idx) {
		if (validity.RowIsValid(idx)) {
			if (all_null) {
				// no value seen yet
				// assign the current value, and increment the seen_count
				// note that we increment last_seen_count rather than setting it to 1
				// this is intentional: this is the first VALID value we see
				// but it might not be the first value in case of nulls!
				last_value = data[idx];
				seen_count++;
				last_seen_count++;
				all_null = false;
			} else if (last_value == data[idx]) {
				// the last value is identical to this value: increment the last_seen_count
				last_seen_count++;
			} else {
				// the values are different
				// issue the callback on the last value
				Flush<OP>();

				// increment the seen_count and put the new value into the REE slot
				last_value = data[idx];
				seen_count++;
				last_seen_count = 1;
			}
		} else {
			// NULL value: we merely increment the last_seen_count
			last_seen_count++;
		}
		if (last_seen_count == NumericLimits<ree_count_t>::Maximum()) {
			// we have seen the same value so many times in a row we are at the limit of what fits in our count
			// write away the value and move to the next value
			Flush<OP>();
			last_seen_count = 0;
			seen_count++;
		}
	}
};

template <class T>
struct REEAnalyzeState : public AnalyzeState {
	REEAnalyzeState() {
	}

	REEState<T> state;
};

template <class T>
unique_ptr<AnalyzeState> REEInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<REEAnalyzeState<T>>();
}

template <class T>
bool REEAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &ree_state = state.template Cast<REEAnalyzeState<T>>();
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	auto data = UnifiedVectorFormat::GetData<T>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		ree_state.state.Update(data, vdata.validity, idx);
	}
	return true;
}

template <class T>
idx_t REEFinalAnalyze(AnalyzeState &state) {
	auto &ree_state = state.template Cast<REEAnalyzeState<T>>();
	return (sizeof(ree_count_t) + sizeof(T)) * ree_state.state.seen_count;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct REEConstants {
	static constexpr const idx_t REE_HEADER_SIZE = sizeof(uint64_t);
};

template <class T, bool WRITE_STATISTICS>
struct REECompressState : public CompressionState {
	struct REEWriter {
		template <class VALUE_TYPE>
		static void Operation(VALUE_TYPE value, ree_count_t count, void *dataptr, bool is_null) {
			auto state = reinterpret_cast<REECompressState<T, WRITE_STATISTICS> *>(dataptr);
			state->WriteValue(value, count, is_null);
		}
	};

	static idx_t MaxREECount() {
		auto entry_size = sizeof(T) + sizeof(ree_count_t);
		auto entry_count = (Storage::BLOCK_SIZE - REEConstants::REE_HEADER_SIZE) / entry_size;
		auto max_vector_count = entry_count / STANDARD_VECTOR_SIZE;
		return max_vector_count * STANDARD_VECTOR_SIZE;
	}

	explicit REECompressState(ColumnDataCheckpointer &checkpointer_p)
	    : checkpointer(checkpointer_p),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_REE)) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		state.dataptr = (void *)this;
		max_ree_count = MaxREECount();
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto column_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		column_segment->function = function;
		current_segment = std::move(column_segment);
		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);
	}

	void Append(UnifiedVectorFormat &vdata, idx_t count) {
		auto data = UnifiedVectorFormat::GetData<T>(vdata);
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			state.template Update<REECompressState<T, WRITE_STATISTICS>::REEWriter>(data, vdata.validity, idx);
		}
	}

	void WriteValue(T value, ree_count_t count, bool is_null) {
		// write the REE entry
		auto handle_ptr = handle.Ptr() + REEConstants::REE_HEADER_SIZE;
		auto data_pointer = (T *)handle_ptr;
		auto index_pointer = (ree_count_t *)(handle_ptr + max_ree_count * sizeof(T));
		data_pointer[entry_count] = value;
		index_pointer[entry_count] = count;
		entry_count++;

		// update meta data
		if (WRITE_STATISTICS && !is_null) {
			NumericStats::Update<T>(current_segment->stats.statistics, value);
		}
		current_segment->count += count;

		if (entry_count == max_ree_count) {
			// we have finished writing this segment: flush it and create a new segment
			auto row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
			entry_count = 0;
		}
	}

	void FlushSegment() {
		// flush the segment
		// we compact the segment by moving the counts so they are directly next to the values
		idx_t counts_size = sizeof(ree_count_t) * entry_count;
		idx_t original_ree_offset = REEConstants::REE_HEADER_SIZE + max_ree_count * sizeof(T);
		idx_t minimal_ree_offset = AlignValue(REEConstants::REE_HEADER_SIZE + sizeof(T) * entry_count);
		idx_t total_segment_size = minimal_ree_offset + counts_size;
		auto data_ptr = handle.Ptr();
		memmove(data_ptr + minimal_ree_offset, data_ptr + original_ree_offset, counts_size);
		// store the final REE offset within the segment
		Store<uint64_t>(minimal_ree_offset, data_ptr);
		handle.Destroy();

		auto &state = checkpointer.GetCheckpointState();
		state.FlushSegment(std::move(current_segment), total_segment_size);
	}

	void Finalize() {
		state.template Flush<REECompressState<T, WRITE_STATISTICS>::REEWriter>();

		FlushSegment();
		current_segment.reset();
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;

	REEState<T> state;
	idx_t entry_count = 0;
	idx_t max_ree_count;
};

template <class T, bool WRITE_STATISTICS>
unique_ptr<CompressionState> REEInitCompression(ColumnDataCheckpointer &checkpointer, unique_ptr<AnalyzeState> state) {
	return make_uniq<REECompressState<T, WRITE_STATISTICS>>(checkpointer);
}

template <class T, bool WRITE_STATISTICS>
void REECompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (REECompressState<T, WRITE_STATISTICS> &)state_p;
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);

	state.Append(vdata, count);
}

template <class T, bool WRITE_STATISTICS>
void REEFinalizeCompress(CompressionState &state_p) {
	auto &state = (REECompressState<T, WRITE_STATISTICS> &)state_p;
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
template <class T>
struct REEScanState : public SegmentScanState {
	explicit REEScanState(ColumnSegment &segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);
		entry_pos = 0;
		position_in_entry = 0;
		ree_count_offset = Load<uint64_t>(handle.Ptr() + segment.GetBlockOffset());
		D_ASSERT(ree_count_offset <= Storage::BLOCK_SIZE);
	}

	void Skip(ColumnSegment &segment, idx_t skip_count) {
		auto data = handle.Ptr() + segment.GetBlockOffset();
		auto index_pointer = (ree_count_t *)(data + ree_count_offset);

		for (idx_t i = 0; i < skip_count; i++) {
			// assign the current value
			position_in_entry++;
			if (position_in_entry >= index_pointer[entry_pos]) {
				// handled all entries in this REE value
				// move to the next entry
				entry_pos++;
				position_in_entry = 0;
			}
		}
	}

	BufferHandle handle;
	idx_t entry_pos;
	idx_t position_in_entry;
	uint32_t ree_count_offset;
};

template <class T>
unique_ptr<SegmentScanState> REEInitScan(ColumnSegment &segment) {
	auto result = make_uniq<REEScanState<T>>(segment);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void REESkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = state.scan_state->Cast<REEScanState<T>>();
	scan_state.Skip(segment, skip_count);
}

template <class T>
void REEScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                    idx_t result_offset) {
	auto &scan_state = state.scan_state->Cast<REEScanState<T>>();

	auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto data_pointer = (T *)(data + REEConstants::REE_HEADER_SIZE);
	auto index_pointer = (ree_count_t *)(data + scan_state.ree_count_offset);

	auto result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	for (idx_t i = 0; i < scan_count; i++) {
		// assign the current value
		result_data[result_offset + i] = data_pointer[scan_state.entry_pos];
		scan_state.position_in_entry++;
		if (scan_state.position_in_entry >= index_pointer[scan_state.entry_pos]) {
			// handled all entries in this REE value
			// move to the next entry
			scan_state.entry_pos++;
			scan_state.position_in_entry = 0;
		}
	}
}

template <class T>
void REEScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	// FIXME: emit constant vector if repetition of single value is >= scan_count
	REEScanPartial<T>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void REEFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	REEScanState<T> scan_state(segment);
	scan_state.Skip(segment, row_id);

	auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto data_pointer = (T *)(data + REEConstants::REE_HEADER_SIZE);
	auto result_data = FlatVector::GetData<T>(result);
	result_data[result_idx] = data_pointer[scan_state.entry_pos];
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T, bool WRITE_STATISTICS = true>
CompressionFunction GetREEFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_REE, data_type, REEInitAnalyze<T>, REEAnalyze<T>,
	                           REEFinalAnalyze<T>, REEInitCompression<T, WRITE_STATISTICS>,
	                           REECompress<T, WRITE_STATISTICS>, REEFinalizeCompress<T, WRITE_STATISTICS>,
	                           REEInitScan<T>, REEScan<T>, REEScanPartial<T>, REEFetchRow<T>, REESkip<T>);
}

CompressionFunction REEFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return GetREEFunction<int8_t>(type);
	case PhysicalType::INT16:
		return GetREEFunction<int16_t>(type);
	case PhysicalType::INT32:
		return GetREEFunction<int32_t>(type);
	case PhysicalType::INT64:
		return GetREEFunction<int64_t>(type);
	case PhysicalType::INT128:
		return GetREEFunction<hugeint_t>(type);
	case PhysicalType::UINT8:
		return GetREEFunction<uint8_t>(type);
	case PhysicalType::UINT16:
		return GetREEFunction<uint16_t>(type);
	case PhysicalType::UINT32:
		return GetREEFunction<uint32_t>(type);
	case PhysicalType::UINT64:
		return GetREEFunction<uint64_t>(type);
	case PhysicalType::FLOAT:
		return GetREEFunction<float>(type);
	case PhysicalType::DOUBLE:
		return GetREEFunction<double>(type);
	case PhysicalType::LIST:
		return GetREEFunction<uint64_t, false>(type);
	default:
		throw InternalException("Unsupported type for REE");
	}
}

bool REEFun::TypeIsSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	case PhysicalType::LIST:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
