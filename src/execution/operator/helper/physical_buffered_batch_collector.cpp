#include "duckdb/execution/operator/helper/physical_buffered_batch_collector.hpp"

#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"

namespace duckdb {

PhysicalBufferedBatchCollector::PhysicalBufferedBatchCollector(PreparedStatementData &data)
    : PhysicalResultCollector(data) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BufferedBatchCollectorGlobalState : public GlobalSinkState {
public:
	BufferedBatchCollectorGlobalState(ClientContext &context, const PhysicalBufferedBatchCollector &op)
	    : data(context, op.types) {
	}

	mutex glock;
	BatchedDataCollection data;
	unique_ptr<MaterializedQueryResult> result;
};

class BufferedBatchCollectorLocalState : public LocalSinkState {
public:
	BufferedBatchCollectorLocalState(ClientContext &context, const PhysicalBufferedBatchCollector &op)
	    : data(context, op.types) {
	}

	BatchedDataCollection data;
};

SinkResultType PhysicalBufferedBatchCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSinkInput &input) const {
	auto &state = input.local_state.Cast<BufferedBatchCollectorLocalState>();
	state.data.Append(chunk, state.partition_info.batch_index.GetIndex());
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalBufferedBatchCollector::Combine(ExecutionContext &context,
                                                              OperatorSinkCombineInput &input) const {
	return SinkCombineResultType::FINISHED;
}

unique_ptr<LocalSinkState> PhysicalBufferedBatchCollector::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<BufferedBatchCollectorLocalState>(context.client, *this);
}

unique_ptr<GlobalSinkState> PhysicalBufferedBatchCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<BufferedBatchCollectorGlobalState>(context, *this);
}

unique_ptr<QueryResult> PhysicalBufferedBatchCollector::GetResult(GlobalSinkState &state) {
	auto &gstate = state.Cast<BufferedBatchCollectorGlobalState>();
	D_ASSERT(gstate.result);
	return std::move(gstate.result);
}

} // namespace duckdb
