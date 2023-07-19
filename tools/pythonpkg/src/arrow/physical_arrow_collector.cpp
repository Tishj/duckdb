#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb_python/arrow/physical_arrow_collector.hpp"
#include "duckdb_python/arrow/arrow_query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb_python/arrow/physical_arrow_batch_collector.hpp"
#include "duckdb_python/arrow/arrow_merge_event.hpp"

namespace duckdb {

unique_ptr<PhysicalResultCollector> PhysicalArrowCollector::Create(ClientContext &context, PreparedStatementData &data,
                                                                   idx_t batch_size) {
	(void)context;

	if (!PhysicalPlanGenerator::PreserveInsertionOrder(context, *data.plan)) {
		// the plan is not order preserving, so we just use the parallel materialized collector
		return make_uniq_base<PhysicalResultCollector, PhysicalArrowCollector>(data, true, batch_size);
	} else if (!PhysicalPlanGenerator::UseBatchIndex(context, *data.plan)) {
		// the plan is order preserving, but we cannot use the batch index: use a single-threaded result collector
		return make_uniq_base<PhysicalResultCollector, PhysicalArrowCollector>(data, false, batch_size);
	} else {
		// we care about maintaining insertion order and the sources all support batch indexes
		// use a batch collector
		return make_uniq_base<PhysicalResultCollector, PhysicalArrowBatchCollector>(data, batch_size);
	}
}

void PhysicalArrowCollector::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                     LocalSinkState &lstate_p) const {
	auto &gstate = gstate_p.Cast<ArrowCollectorGlobalState>();
	auto &lstate = lstate_p.Cast<MaterializedCollectorLocalState>();
	if (lstate.collection->Count() == 0) {
		py::gil_scoped_acquire gil;
		lstate.collection.reset();
		return;
	}

	// Collect all the collections
	lock_guard<mutex> l(gstate.glock);
	gstate.batches[gstate.batch_index++] = std::move(lstate.collection);
}

unique_ptr<QueryResult> PhysicalArrowCollector::GetResult(GlobalSinkState &state_p) {
	auto &gstate = state_p.Cast<ArrowCollectorGlobalState>();
	return std::move(gstate.result);
}

unique_ptr<GlobalSinkState> PhysicalArrowCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<ArrowCollectorGlobalState>();
}

idx_t PhysicalArrowCollector::CalculateAmountOfBatches(idx_t total_row_count, idx_t batch_size) {
	return (total_row_count / batch_size) + (total_row_count % batch_size) != 0;
}

SinkFinalizeType PhysicalArrowCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<ArrowCollectorGlobalState>();
	D_ASSERT(gstate.collection == nullptr);

	gstate.collection = make_uniq<BatchedDataCollection>(context, types, std::move(gstate.batches), true);

	// Pre-allocate the conversion result
	py::list record_batches;
	auto total_tuple_count = gstate.collection->Count();
	auto total_batch_count = CalculateAmountOfBatches(total_tuple_count, record_batch_size);
	// TODO: does this need the GIL?
	record_batches.resize(total_batch_count);
	auto &types = gstate.collection->Types();
	if (total_tuple_count == 0) {
		// Create the result containing a single empty arrow result
		gstate.result = make_uniq<ArrowQueryResult>(statement_type, properties, names, types, std::move(record_batches),
		                                            context.GetClientProperties(), total_tuple_count);
		return SinkFinalizeType::READY;
	}

	// Spawn an event that will populate the batches in the arrow result
	auto new_event = make_shared<ArrowMergeEvent>(*gstate.result, *gstate.collection, pipeline);
	event.InsertEvent(std::move(new_event));

	// Already create the final query result
	gstate.result = make_uniq<ArrowQueryResult>(statement_type, properties, names, std::move(result),
	                                            context.GetClientProperties(), total_tuple_count);

	return SinkFinalizeType::READY;
}

} // namespace duckdb
