#pragma once

#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/task.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/main/chunk_scan_state/batched_data_collection.hpp"
#include "duckdb_python/arrow/arrow_array_stream.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

namespace {

struct ArrowRecordBatchListEntry {
public:
	ArrowRecordBatchListEntry(py::list &batches, idx_t index) : batches(batches), index(index) {
	}

public:
	//! The pre-allocated list of batches
	py::list &batches;
	//! The index where our batch needs to go
	idx_t index;
};

} // namespace

// Task to create one RecordBatch by (partially) scanning a BatchedDataCollection
class ArrowBatchTask : public ExecutorTask {
public:
	ArrowBatchTask(ArrowRecordBatchListEntry entry, Executor &executor, shared_ptr<Event> event_p,
	               BatchCollectionChunkScanState scan_state, vector<string> names, idx_t batch_size)
	    : ExecutorTask(executor), entry(std::move(entry)), event(std::move(event_p)), batch_size(batch_size),
	      names(std::move(names)), scan_state(std::move(scan_state)) {
	}

	void ProduceRecordBatch() {
		ArrowArray data;
		idx_t count;
		auto arrow_options = executor.context.GetArrowOptions();
		count = ArrowUtil::FetchChunk(scan_state, arrow_options, batch_size, &data);
		D_ASSERT(count != 0);
		ArrowSchema arrow_schema;
		ArrowConverter::ToArrowSchema(&arrow_schema, scan_state.Types(), names, arrow_options);

		// Insert the finished record batch in the index where it's supposed to be
		auto &list = entry.batches;
		auto batch_index = entry.index;
		{
			py::gil_scoped_acquire gil;
			list[batch_index] = CreatePyArrowRecordBatch(arrow_schema, data);
		}
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		ProduceRecordBatch();
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	ArrowRecordBatchListEntry entry;
	shared_ptr<Event> event;
	idx_t batch_size;
	vector<string> names;
	BatchCollectionChunkScanState scan_state;
};

static

    class ArrowMergeEvent : public BasePipelineEvent {
public:
	ArrowMergeEvent(ArrowQueryResult &result, BatchedDataCollection &batches, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), result(result), batches(batches) {
		record_batch_size = result.BatchSize();
	}

	ArrowQueryResult &result;
	BatchedDataCollection &batches;

public:
	idx_t RemainingInCurrentChunk() {
		// This assumes that every produced chunk from FetchChunk has STANDARD_VECTOR_SIZE tuples,
		// except for the last one
		idx_t start_of_chunk = batch_offset - (batch_offset % STANDARD_VECTOR_SIZE);
		idx_t remaining = batch_tuples - start_of_chunk;
		return MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
	}

	bool NextBatch() {
		index++;
		if (index >= batches.BatchCount()) {
			return false;
		}
		auto batch_index = batches.IndexToBatchIndex(index);
		batch_tuples = batches.BatchSize(batch_index);
		batch_offset = 0;
		return true;
	}

	idx_t RequiredChunksForBatch() {
		idx_t size = 0;
		idx_t chunks = 0;
		while (size < record_batch_size) {
			auto remaining_in_chunk = RemainingInCurrentChunk();
			auto remaining_to_insert = record_batch_size - size;
			auto append = MinValue(remaining_to_insert, remaining_in_chunk);
			size += append;
			if (size == record_batch_size) {
				// We have completed this batch
				break;
			}
			batch_offset += append;
			if (batch_offset == batch_tuples) {
				if (!NextBatch()) {
					break;
				}
				chunks++;
			}
		}
		return chunks;
	}

	void Schedule() override {
		vector<shared_ptr<Task>> tasks;
		idx_t chunk_offset = 0;
		idx_t record_batch_index = 0;
		auto &record_batches = result.GetRecordBatches();

		while (index < batches.BatchCount()) {
			auto starting_index = index;
			auto initial_offset = batch_offset;
			auto chunks_required = RequiredChunksForBatch();

			// We can safely set the end to max, as we will only fetch one record batch with every task
			// only the start index and the scan state offset mathers.
			auto batch_range = batches.BatchRange(starting_index, DConstants::INVALID_INDEX);

			// Prepare the initial state for this scan state
			BatchCollectionChunkScanState scan_state(batches, batch_range, pipeline->executor.context);
			idx_t chunks_to_skip = initial_offset / STANDARD_VECTOR_SIZE;
			idx_t offset_in_chunk = initial_offset % STANDARD_VECTOR_SIZE;
			for (idx_t i = 0; i < chunks_to_skip; i++) {
				scan_state.SkipChunk();
			}
			scan_state.IncreaseOffset(offset_in_chunk, true);

			// Create a task to populate the arrow result with this batch at this offset
			ArrowRecordBatchListEntry batch_list_entry(record_batches, record_batch_index);
			tasks.push_back(make_uniq<ArrowBatchTask>(std::move(batch_list_entry), pipeline->executor,
			                                          shared_from_this(), std::move(scan_state), result.names,
			                                          record_batch_size));

			record_batch_index++;
		}
		D_ASSERT(!tasks.empty());
		SetTasks(std::move(tasks));
	}

private:
	//! The current batch index of the batched data collection
	idx_t index = 0;
	//! The total amount of tuples in the current batch
	idx_t batch_tuples = 0;
	//! The offset inside the current batch
	idx_t batch_offset = 0;
	//! The max size of a record batch to output
	idx_t record_batch_size;
};

} // namespace duckdb
