#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/map.hpp"

namespace duckdb {

class ArrowCollectorGlobalState : public MaterializedCollectorGlobalState {
public:
	~ArrowCollectorGlobalState() override {
		py::gil_scoped_acquire gil;
		result.reset();
		batches.clear();
	}

public:
	//! The list of batches produced
	vector<py::object> record_batches;
	//! The result returned by GetResult
	unique_ptr<QueryResult> result;
	//! The unordered batches
	map<idx_t, unique_ptr<ColumnDataCollection>> batches;
	//! The number assigned to a batch in Combine
	idx_t batch_index = 0;
	//! The collection we will create from the batches
	unique_ptr<BatchedDataCollection> collection;
};

class PhysicalArrowCollector : public PhysicalMaterializedCollector {
public:
	PhysicalArrowCollector(PreparedStatementData &data, bool parallel, idx_t batch_size)
	    : PhysicalMaterializedCollector(data, parallel), record_batch_size(batch_size) {
	}

public:
	static unique_ptr<PhysicalResultCollector> Create(ClientContext &context, PreparedStatementData &data,
	                                                  idx_t batch_size);
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	unique_ptr<QueryResult> GetResult(GlobalSinkState &state) override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

public:
	//! User provided batch size
	idx_t record_batch_size;
};

} // namespace duckdb
