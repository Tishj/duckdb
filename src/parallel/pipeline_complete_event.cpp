#include "duckdb/parallel/pipeline_complete_event.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {

PipelineCompleteEvent::PipelineCompleteEvent(shared_ptr<Pipeline> pipeline_p, bool complete_pipeline_p)
    : Event(pipeline_p->executor), complete_pipeline(complete_pipeline_p), pipeline(std::move(pipeline_p)) {
}

void PipelineCompleteEvent::Schedule() {
}

void PipelineCompleteEvent::FinalizeFinish() {
	if (complete_pipeline) {
		executor.CompletePipeline();
	}
}

} // namespace duckdb
