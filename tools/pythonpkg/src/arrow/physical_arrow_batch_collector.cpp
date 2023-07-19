#include "duckdb_python/arrow/physical_arrow_batch_collector.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb_python/arrow/arrow_query_result.hpp"
#include "duckdb_python/arrow/arrow_merge_event.hpp"

namespace duckdb {

unique_ptr<GlobalSinkState> PhysicalArrowBatchCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<ArrowBatchGlobalState>(context, *this);
}

SinkFinalizeType PhysicalArrowBatchCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                       GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<ArrowBatchGlobalState>();

	// Pre-allocate the conversion result
	py::list record_batches;
	auto total_tuple_count = gstate.data.Count();
	auto &types = gstate.data.Types();
	{
		py::gil_scoped_acquire gil;
		result = make_uniq<ArrowResultConversion>(types, total_tuple_count);
		result->SetCardinality(total_tuple_count);
	}
	if (total_tuple_count == 0) {
		// Create the result containing a single empty result conversion
		gstate.result = make_uniq<ArrowQueryResult>(statement_type, properties, names, std::move(result),
		                                            context.GetClientProperties());
		return SinkFinalizeType::READY;
	}

	// Spawn an event that will populate the conversion result
	auto new_event = make_shared<ArrowMergeEvent>(*result, gstate.data, pipeline);
	event.InsertEvent(std::move(new_event));

	// Already create the final query result
	gstate.result = make_uniq<ArrowQueryResult>(statement_type, properties, names, std::move(result),
	                                            context.GetClientProperties());

	return SinkFinalizeType::READY;
}

} // namespace duckdb
