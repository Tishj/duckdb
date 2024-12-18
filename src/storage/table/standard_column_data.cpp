#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/validity_checkpoint_state.hpp"
#include "duckdb/storage/table/standard_checkpoint_state.hpp"

namespace duckdb {

StandardColumnData::StandardColumnData(BlockManager &block_manager, const DataTableInfo &info, idx_t column_index,
                                       idx_t start_row, LogicalType type, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, start_row, std::move(type), parent),
      validity(block_manager, info, 0, start_row, *this) {
}

void StandardColumnData::SetStart(idx_t new_start) {
	ColumnData::SetStart(new_start);
	validity.SetStart(new_start);
}

ScanVectorType StandardColumnData::GetVectorScanType(ColumnScanState &state, idx_t scan_count, Vector &result) const {
	// if either the current column data, or the validity column data requires flat vectors, we scan flat vectors
	auto scan_type = ColumnData::GetVectorScanType(state, scan_count, result);
	if (scan_type == ScanVectorType::SCAN_FLAT_VECTOR) {
		return ScanVectorType::SCAN_FLAT_VECTOR;
	}
	if (state.child_states.empty()) {
		return scan_type;
	}
	return validity.GetVectorScanType(state.child_states[0], scan_count, result);
}

void StandardColumnData::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state,
                                            idx_t rows) const {
	ColumnData::InitializePrefetch(prefetch_state, scan_state, rows);
	validity.InitializePrefetch(prefetch_state, scan_state.child_states[0], rows);
}

void StandardColumnData::InitializeScan(ColumnScanState &state) const {
	ColumnData::InitializeScan(state);

	// initialize the validity segment
	D_ASSERT(state.child_states.size() == 1);
	validity.InitializeScan(state.child_states[0]);
}

void StandardColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) const {
	ColumnData::InitializeScanWithOffset(state, row_idx);

	// initialize the validity segment
	D_ASSERT(state.child_states.size() == 1);
	validity.InitializeScanWithOffset(state.child_states[0], row_idx);
}

idx_t StandardColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                               idx_t target_count) const {
	D_ASSERT(state.row_index == state.child_states[0].row_index);
	auto scan_type = GetVectorScanType(state, target_count, result);
	auto mode = ScanVectorMode::REGULAR_SCAN;
	auto scan_count = ScanVector(transaction, vector_index, state, result, target_count, scan_type, mode);
	validity.ScanVector(transaction, vector_index, state.child_states[0], result, target_count, scan_type, mode);
	return scan_count;
}

idx_t StandardColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
                                        idx_t target_count) const {
	D_ASSERT(state.row_index == state.child_states[0].row_index);
	auto scan_count = ColumnData::ScanCommitted(vector_index, state, result, allow_updates, target_count);
	validity.ScanCommitted(vector_index, state.child_states[0], result, allow_updates, target_count);
	return scan_count;
}

idx_t StandardColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) const {
	auto scan_count = ColumnData::ScanCount(state, result, count);
	validity.ScanCount(state.child_states[0], result, count);
	return scan_count;
}

void StandardColumnData::Filter(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                                SelectionVector &sel, idx_t &count, const TableFilter &filter) const {
	// check if we can do a specialized select
	// the compression functions need to support this
	bool has_filter = HasCompressionFunction() && GetCompressionFunction().filter;
	bool validity_has_filter = validity.HasCompressionFunction() && validity.GetCompressionFunction().filter;
	auto target_count = GetVectorCount(vector_index);
	auto scan_type = GetVectorScanType(state, target_count, result);
	bool scan_entire_vector = scan_type == ScanVectorType::SCAN_ENTIRE_VECTOR;
	bool verify_fetch_row = state.scan_options && state.scan_options->force_fetch_row;
	if (!has_filter || !validity_has_filter || !scan_entire_vector || verify_fetch_row) {
		// we are not scanning an entire vector - this can have several causes (updates, etc)
		ColumnData::Filter(transaction, vector_index, state, result, sel, count, filter);
		return;
	}
	FilterVector(state, result, target_count, sel, count, filter);
	validity.FilterVector(state.child_states[0], result, target_count, sel, count, filter);
}

void StandardColumnData::Select(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                                SelectionVector &sel, idx_t sel_count) const {
	// check if we can do a specialized select
	// the compression functions need to support this
	bool has_select = HasCompressionFunction() && GetCompressionFunction().select;
	bool validity_has_select = validity.HasCompressionFunction() && validity.GetCompressionFunction().select;
	auto target_count = GetVectorCount(vector_index);
	auto scan_type = GetVectorScanType(state, target_count, result);
	bool scan_entire_vector = scan_type == ScanVectorType::SCAN_ENTIRE_VECTOR;
	if (!has_select || !validity_has_select || !scan_entire_vector) {
		// we are not scanning an entire vector - this can have several causes (updates, etc)
		ColumnData::Select(transaction, vector_index, state, result, sel, sel_count);
		return;
	}
	SelectVector(state, result, target_count, sel, sel_count);
	validity.SelectVector(state.child_states[0], result, target_count, sel, sel_count);
}

void StandardColumnData::InitializeAppend(ColumnAppendState &state) {
	ColumnData::InitializeAppend(state);
	ColumnAppendState child_append;
	validity.InitializeAppend(child_append);
	state.child_appends.push_back(std::move(child_append));
}

void StandardColumnData::AppendData(BaseStatistics &stats, ColumnAppendState &state, UnifiedVectorFormat &vdata,
                                    idx_t count) {
	ColumnData::AppendData(stats, state, vdata, count);
	validity.AppendData(stats, state.child_appends[0], vdata, count);
}

void StandardColumnData::RevertAppend(row_t start_row) {
	ColumnData::RevertAppend(start_row);

	validity.RevertAppend(start_row);
}

idx_t StandardColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	// fetch validity mask
	if (state.child_states.empty()) {
		ColumnScanState child_state;
		child_state.scan_options = state.scan_options;
		state.child_states.push_back(std::move(child_state));
	}
	auto scan_count = ColumnData::Fetch(state, row_id, result);
	validity.Fetch(state.child_states[0], row_id, result);
	return scan_count;
}

void StandardColumnData::Update(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                                idx_t update_count) {
	ColumnData::Update(transaction, column_index, update_vector, row_ids, update_count);
	validity.Update(transaction, column_index, update_vector, row_ids, update_count);
}

void StandardColumnData::UpdateColumn(TransactionData transaction, const vector<column_t> &column_path,
                                      Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth) {
	if (depth >= column_path.size()) {
		// update this column
		ColumnData::Update(transaction, column_path[0], update_vector, row_ids, update_count);
	} else {
		// update the child column (i.e. the validity column)
		validity.UpdateColumn(transaction, column_path, update_vector, row_ids, update_count, depth + 1);
	}
}

unique_ptr<BaseStatistics> StandardColumnData::GetUpdateStatistics() {
	auto stats = updates ? updates->GetStatistics() : nullptr;
	auto validity_stats = validity.GetUpdateStatistics();
	if (!stats && !validity_stats) {
		return nullptr;
	}
	if (!stats) {
		stats = BaseStatistics::CreateEmpty(type).ToUnique();
	}
	if (validity_stats) {
		stats->Merge(*validity_stats);
	}
	return stats;
}

void StandardColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                                  idx_t result_idx) {
	// find the segment the row belongs to
	if (state.child_states.empty()) {
		auto child_state = make_uniq<ColumnFetchState>();
		state.child_states.push_back(std::move(child_state));
	}
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);
	ColumnData::FetchRow(transaction, state, row_id, result, result_idx);
}

void StandardColumnData::CommitDropColumn() {
	ColumnData::CommitDropColumn();
	validity.CommitDropColumn();
}

unique_ptr<BaseStatistics> StandardColumnCheckpointState::GetStatistics() {
	D_ASSERT(global_stats);
	return std::move(global_stats);
}

PersistentColumnData StandardColumnCheckpointState::ToPersistentData() {
	auto data = ColumnCheckpointState::ToPersistentData();
	data.child_columns.push_back(validity_state->ToPersistentData());
	return data;
}

unique_ptr<ColumnCheckpointState> StandardColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                            PartialBlockManager &partial_block_manager,
                                                                            SegmentLock &&lock) {
	return make_uniq<StandardColumnCheckpointState>(row_group, *this, partial_block_manager, std::move(lock));
}

bool StandardColumnData::HasUpdates(idx_t start_row_idx, idx_t end_row_idx) {
	bool base_has_changes = ColumnData::HasUpdates(start_row_idx, end_row_idx);
	if (base_has_changes) {
		return true;
	}

	if (compression && compression->validity == CompressionValidity::NO_VALIDITY_REQUIRED) {
		// We are also responsible for encoding the validity, check if there are changes there
		return validity.HasUpdates(start_row_idx, end_row_idx);
	}
	return false;
}

unique_ptr<ColumnCheckpointState> StandardColumnData::Checkpoint(RowGroup &row_group,
                                                                 ColumnCheckpointInfo &checkpoint_info) {

	// Grab both of the locks
	auto base_lock = data.Lock();
	auto validity_lock = validity.data.Lock();

	// scan the segments of the column data
	// set up the checkpoint state
	auto base_state = CreateCheckpointState(row_group, checkpoint_info.info.manager, std::move(base_lock));
	base_state->global_stats = BaseStatistics::CreateEmpty(type).ToUnique();
	auto validity_checkpoint_state =
	    validity.CreateCheckpointState(row_group, checkpoint_info.info.manager, std::move(validity_lock));
	validity_checkpoint_state->global_stats = BaseStatistics::CreateEmpty(validity.type).ToUnique();

	// Move the validity state into the base state
	auto &validity_state = validity_checkpoint_state->Cast<ValidityColumnCheckpointState>();
	auto &checkpoint_state = base_state->Cast<StandardColumnCheckpointState>();
	checkpoint_state.validity_state = std::move(validity_checkpoint_state);
	//! Set the parent state so this can be referenced during checkpointing
	validity_state.parent_state = base_state.get();

	// Reference the segments to compress
	auto &base_nodes = data.ReferenceSegments(base_lock);
	auto &validity_nodes = validity.data.ReferenceSegments(validity_lock);
	if (base_nodes.empty()) {
		D_ASSERT(validity_nodes.empty());
		// empty table: flush the empty list
		return base_state;
	}
	D_ASSERT(!validity_nodes.empty());

	ColumnDataCheckpointer base_checkpointer(*this, row_group, checkpoint_state, checkpoint_info);
	ColumnDataCheckpointer validity_checkpointer(validity, row_group, validity_state, checkpoint_info);

	base_checkpointer.Checkpoint(base_nodes);
	validity_checkpointer.Checkpoint(validity_nodes);

	base_checkpointer.FinalizeCheckpoint(data.MoveSegments(base_lock));
	validity_checkpointer.FinalizeCheckpoint(validity.data.MoveSegments(validity_lock));

	// reset the compression function
	compression = nullptr;
	validity.compression = nullptr;

	// replace the old tree of the base with the new one
	auto new_base_segments = checkpoint_state.new_tree.MoveSegments();
	for (auto &new_base_segment : new_base_segments) {
		AppendSegment(base_lock, std::move(new_base_segment.node));
	}

	// replace the old tree of the validity with the new one
	auto new_validity_segments = validity_state.new_tree.MoveSegments();
	for (auto &new_validity_segment : new_validity_segments) {
		validity.AppendSegment(validity_lock, std::move(new_validity_segment.node));
	}

	ClearUpdates();
	validity.ClearUpdates();

	return base_state;
}

void StandardColumnData::CheckpointScan(ColumnSegment &segment, ColumnCheckpointState &checkpoint_state_p,
                                        ColumnScanState &state, idx_t row_group_start, idx_t count,
                                        Vector &scan_vector) {
	ColumnData::CheckpointScan(segment, checkpoint_state_p, state, row_group_start, count, scan_vector);

	idx_t offset_in_row_group = state.row_index - row_group_start;
	auto &checkpoint_state = checkpoint_state_p.Cast<StandardColumnCheckpointState>();
	auto &validity_state = *checkpoint_state.validity_state;
	validity.ScanCommittedRange(row_group_start, offset_in_row_group, count, scan_vector, validity_state.lock);
}

bool StandardColumnData::IsPersistent() {
	return ColumnData::IsPersistent() && validity.IsPersistent();
}

PersistentColumnData StandardColumnData::Serialize() {
	auto persistent_data = ColumnData::Serialize();
	persistent_data.child_columns.push_back(validity.Serialize());
	return persistent_data;
}

void StandardColumnData::InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) {
	ColumnData::InitializeColumn(column_data, target_stats);
	validity.InitializeColumn(column_data.child_columns[0], target_stats);
}

void StandardColumnData::GetColumnSegmentInfo(duckdb::idx_t row_group_index, vector<duckdb::idx_t> col_path,
                                              vector<duckdb::ColumnSegmentInfo> &result) {
	ColumnData::GetColumnSegmentInfo(row_group_index, col_path, result);
	col_path.push_back(0);
	validity.GetColumnSegmentInfo(row_group_index, std::move(col_path), result);
}

void StandardColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	ColumnData::Verify(parent);
	validity.Verify(parent);
#endif
}

} // namespace duckdb
