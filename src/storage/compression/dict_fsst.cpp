#include "duckdb/storage/compression/dict_fsst/common.hpp"
#include "duckdb/storage/compression/dict_fsst/analyze.hpp"
#include "duckdb/storage/compression/dict_fsst/compression.hpp"
#include "duckdb/storage/compression/dict_fsst/decompression.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"

/*
Data layout per segment:
+-----------------------------------------------------+
|                  Header                             |
|   +---------------------------------------------+   |
|   |   dict_fsst_compression_header_t  header    |   |
|   +---------------------------------------------+   |
|                                                     |
+-----------------------------------------------------+
|             Selection Buffer               |
|   +------------------------------------+   |
|   |   uint16_t index_buffer_idx[]      |   |
|   +------------------------------------+   |
|      tuple index -> index buffer idx       |
|                                            |
+--------------------------------------------+
|               Index Buffer                 |
|   +------------------------------------+   |
|   |   uint16_t  dictionary_offset[]    |   |
|   +------------------------------------+   |
|  string_index -> offset in the dictionary  |
|                                            |
+--------------------------------------------+
|                Dictionary                  |
|   +------------------------------------+   |
|   |   uint8_t *raw_string_data         |   |
|   +------------------------------------+   |
|      the string data without lengths       |
|                                            |
+--------------------------------------------+
|             FSST Symbol Table (opt)        |
|   +------------------------------------+   |
|   |   duckdb_fsst_decoder_t table      |   |
|   +------------------------------------+   |
|                                            |
+--------------------------------------------+
*/

namespace duckdb {
namespace dict_fsst {

struct DictFSSTCompressionStorage {
	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointData &checkpoint_data,
	                                                    unique_ptr<AnalyzeState> state);
	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);

	static unique_ptr<SegmentScanState> StringInitScan(ColumnSegment &segment);
	template <bool ALLOW_DICT_VECTORS>
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
unique_ptr<AnalyzeState> DictFSSTCompressionStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	CompressionInfo info(col_data.GetBlockManager().GetBlockSize());
	return make_uniq<DictFSSTCompressionAnalyzeState>(info);
}

bool DictFSSTCompressionStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<DictFSSTCompressionAnalyzeState>();
	return state.analyze_state->UpdateState(input, count);
}

idx_t DictFSSTCompressionStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &analyze_state = state_p.Cast<DictFSSTCompressionAnalyzeState>();
	auto &state = *analyze_state.analyze_state;

	auto width = BitpackingPrimitives::MinimumBitWidth(state.current_unique_count + 1);
	auto req_space = DictFSSTCompression::RequiredSpace(state.current_tuple_count, state.current_unique_count,
	                                                    state.current_dict_size, width);

	const auto total_space = state.segment_count * state.info.GetBlockSize() + req_space;
	return LossyNumericCast<idx_t>(DictFSSTCompression::MINIMUM_COMPRESSION_RATIO * float(total_space));
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
unique_ptr<CompressionState> DictFSSTCompressionStorage::InitCompression(ColumnDataCheckpointData &checkpoint_data,
                                                                         unique_ptr<AnalyzeState> state) {
	return make_uniq<DictFSSTCompressionCompressState>(checkpoint_data, state->info);
}

void DictFSSTCompressionStorage::Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = state_p.Cast<DictFSSTCompressionCompressState>();
	auto res = state.UpdateState(scan_vector, count);
	(void)(res);
	D_ASSERT(res);
}

void DictFSSTCompressionStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<DictFSSTCompressionCompressState>();
	state.Flush(true);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
unique_ptr<SegmentScanState> DictFSSTCompressionStorage::StringInitScan(ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto state = make_uniq<CompressedStringScanState>(buffer_manager.Pin(segment.block));
	state->Initialize(segment, true);
	return std::move(state);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <bool ALLOW_DICT_VECTORS>
void DictFSSTCompressionStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                                   Vector &result, idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = state.scan_state->Cast<CompressedStringScanState>();

	auto start = segment.GetRelativeIndex(state.row_index);
	if (!ALLOW_DICT_VECTORS || scan_count != STANDARD_VECTOR_SIZE ||
	    start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE != 0) {
		scan_state.ScanToFlatVector(result, result_offset, start, scan_count);
	} else {
		scan_state.ScanToDictionaryVector(segment, result, result_offset, start, scan_count);
	}
}

void DictFSSTCompressionStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                            Vector &result) {
	StringScanPartial<true>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void DictFSSTCompressionStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id,
                                                Vector &result, idx_t result_idx) {
	// fetch a single row from the string segment
	CompressedStringScanState scan_state(state.GetOrInsertHandle(segment));
	scan_state.Initialize(segment, false);
	scan_state.ScanToFlatVector(result, result_idx, NumericCast<idx_t>(row_id), 1);
}

} // namespace dict_fsst

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction DictFSSTCompressionFun::GetFunction(PhysicalType data_type) {
	auto res = CompressionFunction(
	    CompressionType::COMPRESSION_DICT_FSST, data_type, dict_fsst::DictFSSTCompressionStorage::StringInitAnalyze,
	    dict_fsst::DictFSSTCompressionStorage::StringAnalyze, dict_fsst::DictFSSTCompressionStorage::StringFinalAnalyze,
	    dict_fsst::DictFSSTCompressionStorage::InitCompression, dict_fsst::DictFSSTCompressionStorage::Compress,
	    dict_fsst::DictFSSTCompressionStorage::FinalizeCompress, dict_fsst::DictFSSTCompressionStorage::StringInitScan,
	    dict_fsst::DictFSSTCompressionStorage::StringScan,
	    dict_fsst::DictFSSTCompressionStorage::StringScanPartial<false>,
	    dict_fsst::DictFSSTCompressionStorage::StringFetchRow, UncompressedFunctions::EmptySkip,
	    UncompressedStringStorage::StringInitSegment);
	res.validity = CompressionValidity::NO_VALIDITY_REQUIRED;
	return res;
}

bool DictFSSTCompressionFun::TypeIsSupported(const PhysicalType physical_type) {
	return physical_type == PhysicalType::VARCHAR;
}

} // namespace duckdb