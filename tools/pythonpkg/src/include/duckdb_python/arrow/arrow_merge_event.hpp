#pragma once

#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/task.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

namespace {

struct ArrowRecordBatchListEntry {
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
	               BatchedChunkScanState scan_state, idx_t offset, BatchedDataCollection &source)
	    : ExecutorTask(executor), event(std::move(event_p)), offset(offset), scan_state(std::move(scan_state)),
	      source(source) {
	}

	void ProduceRecordBatch() {
		D_ASSERT(offset < result.Capacity());

		auto &allocator = BufferManager::GetBufferManager(executor.context).GetBufferAllocator();

		DataChunk intermediate;
		intermediate.Initialize(allocator, result.Types());
		while (scan_state.range.begin != scan_state.range.end) {
			source.Scan(scan_state, intermediate);
			if (intermediate.size() == 0) {
				break;
			}
			result.Append(intermediate, offset);
			offset += intermediate.size();
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
	idx_t offset;
	BatchedChunkScanState scan_state;
	BatchedDataCollection &source;
};

// Spawn tasks to populate a pre-allocated NumpyConversionResult
class NumpyMergeEvent : public BasePipelineEvent {
public:
	NumpyMergeEvent(ArrowQueryResult &result, BatchedDataCollection &batches, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), result(result), batches(batches) {
	}

	ArrowQueryResult &result;
	BatchedDataCollection &batches;

public:
	void Schedule() override {
		vector<shared_ptr<Task>> tasks;
		idx_t offset = 0;
		for (idx_t i = 0; i < batches.BatchCount(); i++) {
			// Create a range of 1 batch per task
			auto batch_range = batches.BatchRange(i, i + 1);
			BatchedChunkScanState batch_scan_state;
			batches.InitializeScan(batch_scan_state, batch_range);

			// Create a task to populate the numpy result with this batch at this offset
			tasks.push_back(make_uniq<ArrowBatchTask>(pipeline->executor, shared_from_this(),
			                                          std::move(batch_scan_state), result, offset, batches));

			// Forward the offset
			auto batch_index = batches.IndexToBatchIndex(i);
			auto row_count = batches.BatchSize(batch_index);
			offset += row_count;
		}
		D_ASSERT(!tasks.empty());
		SetTasks(std::move(tasks));
	}
};

} // namespace duckdb
