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

static idx_t RequiredChunksForBatch(idx_t batch_size, idx_t &offset) {
	idx_t size = 0;
	idx_t chunks = 0;
	while (size < batch_size) {
		auto remaining_in_chunk = STANDARD_VECTOR_SIZE - offset;
		auto remaining_to_insert = batch_size - size;
		auto append = MinValue(remaining_to_insert, remaining_in_chunk);
		size += append;
		offset += append;
		if (offset == STANDARD_VECTOR_SIZE) {
			offset = 0;
			chunks++;
		}
	}
	return chunks;
}

class ArrowMergeEvent : public BasePipelineEvent {
public:
	ArrowMergeEvent(ArrowQueryResult &result, BatchedDataCollection &batches, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), result(result), batches(batches) {
	}

	ArrowQueryResult &result;
	BatchedDataCollection &batches;

public:
	void Schedule() override {
		vector<shared_ptr<Task>> tasks;
		idx_t chunk_offset = 0;
		idx_t record_batch_index = 0;
		auto &record_batches = result.GetRecordBatches();
		auto batch_size = result.BatchSize();

		idx_t index = 0;
		while (index < batches.BatchCount()) {
			// FIXME: this assumes that every batch contains STANDARD_VECTOR_SIZE tuples
			auto chunks_required = 1 + RequiredChunksForBatch(batch_size, chunk_offset);
			if (index + chunks_required > batches.BatchCount()) {
				// Cap to the maximum batches we have, this is likely the last batch (?)
				chunks_required = batches.BatchCount() - index;
			}

			idx_t range_end = chunks_required;
			if (range_end == 0) {
				D_ASSERT(batch_size < STANDARD_VECTOR_SIZE);
				// If the batch size is smaller than a vector, we can potentially collect multiple batches from the same
				// chunk
				range_end++;
			}
			range_end += index;

			auto batch_range = batches.BatchRange(index, range_end);
			BatchCollectionChunkScanState scan_state(batches, batch_range, pipeline->executor.context);
			scan_state.IncreaseOffset(chunk_offset);

			// Create a task to populate the arrow result with this batch at this offset
			ArrowRecordBatchListEntry batch_list_entry(record_batches, record_batch_index);
			tasks.push_back(make_uniq<ArrowBatchTask>(std::move(batch_list_entry), pipeline->executor,
			                                          shared_from_this(), std::move(scan_state), result.names,
			                                          batch_size));

			index += chunks_required;
			record_batch_index++;
		}
		D_ASSERT(!tasks.empty());
		SetTasks(std::move(tasks));
	}
};

} // namespace duckdb
