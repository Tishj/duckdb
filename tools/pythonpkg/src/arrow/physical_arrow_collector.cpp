#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb_python/arrow/physical_arrow_collector.hpp"
#include "duckdb_python/arrow/arrow_query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb_python/arrow/physical_arrow_batch_collector.hpp"
#include "duckdb_python/arrow/arrow_merge_event.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

unique_ptr<PhysicalResultCollector> PhysicalArrowCollector::Create(ClientContext &context, PreparedStatementData &data,
                                                                   idx_t batch_size) {
	(void)context;

	// The creation of the record batches requires this module, and when this is imported for the first time from a
	// thread that is not the main execution thread this might cause a crash. So we import it here while we're still in
	// the main thread.
	{
		py::gil_scoped_acquire gil;
		auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");
	}

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

SinkCombineResultType PhysicalArrowCollector::Combine(ExecutionContext &context,
                                                      OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<ArrowCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<MaterializedCollectorLocalState>();
	if (lstate.collection->Count() == 0) {
		py::gil_scoped_acquire gil;
		lstate.collection.reset();
		return SinkCombineResultType::FINISHED;
	}

	// Collect all the collections
	lock_guard<mutex> l(gstate.glock);
	gstate.batches[gstate.batch_index++] = std::move(lstate.collection);
	return SinkCombineResultType::FINISHED;
}

unique_ptr<QueryResult> PhysicalArrowCollector::GetResult(GlobalSinkState &state_p) {
	auto &gstate = state_p.Cast<ArrowCollectorGlobalState>();
	return std::move(gstate.result);
}

unique_ptr<GlobalSinkState> PhysicalArrowCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<ArrowCollectorGlobalState>();
}

SinkFinalizeType PhysicalArrowCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<ArrowCollectorGlobalState>();
	D_ASSERT(gstate.collection == nullptr);

	gstate.collection = make_uniq<BatchedDataCollection>(context, types, std::move(gstate.batches), true);

	auto total_tuple_count = gstate.collection->Count();
	auto &types = gstate.collection->Types();
	if (total_tuple_count == 0) {
		gstate.result = make_uniq<ArrowQueryResult>(statement_type, properties, names, types,
		                                            context.GetClientProperties(), 0, record_batch_size);
		{
			py::gil_scoped_acquire gil;
			// This result is empty, add an empty list of record batches
			auto &arrow_result = (ArrowQueryResult &)*gstate.result;
			arrow_result.SetRecordBatches(make_uniq<py::list>(0));
		}
		return SinkFinalizeType::READY;
	}

	// Already create the final query result
	gstate.result = make_uniq<ArrowQueryResult>(statement_type, properties, names, types, context.GetClientProperties(),
	                                            total_tuple_count, record_batch_size);

	auto &arrow_result = (ArrowQueryResult &)*gstate.result;
	// Spawn an event that will populate the batches in the arrow result
	auto new_event = make_shared<ArrowMergeEvent>(arrow_result, *gstate.collection, pipeline);
	event.InsertEvent(std::move(new_event));

	return SinkFinalizeType::READY;
}

} // namespace duckdb
