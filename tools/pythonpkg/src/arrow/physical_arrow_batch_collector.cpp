#include "duckdb_python/arrow/physical_arrow_batch_collector.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb_python/arrow/arrow_query_result.hpp"
#include "duckdb_python/arrow/arrow_merge_event.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

unique_ptr<GlobalSinkState> PhysicalArrowBatchCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<ArrowBatchGlobalState>(context, *this);
}

SinkFinalizeType PhysicalArrowBatchCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                       GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<ArrowBatchGlobalState>();

	auto total_tuple_count = gstate.data.Count();
	if (total_tuple_count == 0) {
		// Create the result containing a single empty result conversion
		gstate.result =
		    make_uniq<ArrowQueryResult>(statement_type, properties, names, types, context.GetClientProperties(),
		                                total_tuple_count, record_batch_size);
		return SinkFinalizeType::READY;
	}

	// Spawn an event that will populate the conversion result
	auto &arrow_result = (ArrowQueryResult &)*gstate.result;
	auto new_event = make_shared<ArrowMergeEvent>(arrow_result, gstate.data, pipeline);
	event.InsertEvent(std::move(new_event));

	// Already create the final query result
	gstate.result = make_uniq<ArrowQueryResult>(statement_type, properties, names, types, context.GetClientProperties(),
	                                            total_tuple_count, record_batch_size);

	return SinkFinalizeType::READY;
}

} // namespace duckdb
