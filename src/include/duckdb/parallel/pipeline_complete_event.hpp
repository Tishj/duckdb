//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline_complete_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/event.hpp"

namespace duckdb {
class Executor;

class PipelineCompleteEvent : public Event {
public:
	PipelineCompleteEvent(shared_ptr<Pipeline> pipeline, bool complete_pipeline_p);

	bool complete_pipeline;

public:
	void Schedule() override;
	void FinalizeFinish() override;

public:
	//! The pipeline that this event belongs to
	shared_ptr<Pipeline> pipeline;
};

} // namespace duckdb
