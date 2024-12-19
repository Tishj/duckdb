#include "duckdb/storage/table/validity_column_data.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/validity_checkpoint_state.hpp"

namespace duckdb {

ValidityColumnData::ValidityColumnData(BlockManager &block_manager, const DataTableInfo &info, idx_t column_index,
                                       idx_t start_row, ColumnData &parent)
    : ColumnData(block_manager, info, column_index, start_row, LogicalType(LogicalTypeId::VALIDITY), &parent) {
}

FilterPropagateResult ValidityColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) const {
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

ValidityColumnCheckpointState::ValidityColumnCheckpointState(RowGroup &row_group, ColumnData &column_data,
                                                             PartialBlockManager &partial_block_manager,
                                                             SegmentLock &&lock)
    : ColumnCheckpointState(row_group, column_data, partial_block_manager, std::move(lock)) {
}

unique_ptr<ColumnCheckpointState> ValidityColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                            PartialBlockManager &partial_block_manager,
                                                                            SegmentLock &&lock) {
	return make_uniq<ValidityColumnCheckpointState>(row_group, *this, partial_block_manager, std::move(lock));
}

void ValidityColumnData::AppendData(BaseStatistics &stats, ColumnAppendState &state, UnifiedVectorFormat &vdata,
                                    idx_t count) {
	lock_guard<mutex> l(stats_lock);
	ColumnData::AppendData(stats, state, vdata, count);
}

static void AnalyzeParentCompression(ValidityColumnCheckpointState &state) {
	D_ASSERT(state.parent_state);
	if (state.analyzed_compression) {
		return;
	}
	state.analyzed_compression = true;
	auto &parent_state = *state.parent_state;
	auto segments = parent_state.new_tree.Segments();
	optional_ptr<CompressionFunction> function;
	bool initialized = false;
	for (auto &segment : segments) {
		if (!initialized) {
			function = (CompressionFunction &)segment.GetCompressionFunction(); // NOLINT fixme, should be const
		} else if (function) {
			if (function->type != segment.GetCompressionFunction().type) {
				function = nullptr;
			}
		}
	}
	if (function && function->validity == CompressionValidity::NO_VALIDITY_REQUIRED) {
		state.no_validity_required = true;
	}
}

LogicalType ValidityColumnData::GetTypeForScan(ColumnCheckpointState &state_p) {
	auto &state = state_p.Cast<ValidityColumnCheckpointState>();
	if (state.parent_state) {
		bool contains_empty_segment = false;
		auto &segments = data.ReferenceSegments(state.lock);
		for (auto &segment : segments) {
			if (segment.node->GetCompressionFunction().type == CompressionType::COMPRESSION_EMPTY) {
				contains_empty_segment = true;
				break;
			}
		}
		if (contains_empty_segment) {
			// The intermediate vector has to have the type of the parent, because we'll need to scan the parent segment
			// to get the validity, it's not stored separately
			return Parent().type;
		}
	}
	return LogicalType::BOOLEAN;
}

vector<optional_ptr<CompressionFunction>> ValidityColumnData::GetCompressionFunctions(DBConfig &config,
                                                                                      ColumnCheckpointState &state_p) {
	auto &state = state_p.Cast<ValidityColumnCheckpointState>();
	if (state.parent_state) {
		// Check what the type of the new segments are, as that will (possibly) contain the validity data
		AnalyzeParentCompression(state);
		if (state.no_validity_required) {
			// All segments are compressed with the same function, and that function does not require a separate
			// validity segment
			return {config.GetCompressionFunction(CompressionType::COMPRESSION_EMPTY, type.InternalType())};
		}
	}
	return ColumnData::GetCompressionFunctions(config, state_p);
}

void ValidityColumnData::CheckpointScan(ColumnSegment &segment, ColumnCheckpointState &checkpoint_state_p,
                                        ColumnScanState &state, idx_t row_group_start, idx_t count,
                                        Vector &scan_vector) {
	auto &validity_state = checkpoint_state_p.Cast<ValidityColumnCheckpointState>();
	idx_t offset_in_row_group = state.row_index - row_group_start;
	bool scan_parent = false;
#ifdef ALTERNATIVE_VERIFY
	if (validity_state.parent_state) {
		scan_parent = true;
	}
#endif
	if (segment.GetCompressionFunction().type == CompressionType::COMPRESSION_EMPTY) {
		D_ASSERT(scan_vector.GetType() == Parent().type);
		scan_parent = true;
	}
	if (scan_parent) {
		D_ASSERT(validity_state.parent_state);
		D_ASSERT(HasParent());
		auto &parent = Parent();
		auto &parent_state = *validity_state.parent_state;

		parent.ScanCommittedRange(row_group_start, offset_in_row_group, count, scan_vector, parent_state.lock);
	}
	ScanCommittedRange(row_group_start, offset_in_row_group, count, scan_vector, validity_state.lock);
}

} // namespace duckdb
