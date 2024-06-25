#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/helper/physical_explain_analyze.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalExplain &op) {
	D_ASSERT(op.children.size() == 1);

	auto &renderer = op.GetRenderer();
	auto logical_plan_opt = renderer.RenderLogicalPlan(*op.children[0]);
	auto plan = CreatePlan(*op.children[0]);
	if (op.explain_type == ExplainType::EXPLAIN_ANALYZE) {
		auto result = make_uniq<PhysicalExplainAnalyze>(op.types);
		result->children.push_back(std::move(plan));
		return std::move(result);
	}
	vector<string> keys, values;
	auto explain_output_type = ClientConfig::GetConfig(context).explain_output_type;
	if (explain_output_type == ExplainOutputType::ALL) {
		// Unoptimized logical plan
		keys.push_back("logical_plan");
		values.push_back(op.logical_plan_unopt);
	}
	if (explain_output_type == ExplainOutputType::ALL || explain_output_type == ExplainOutputType::OPTIMIZED_ONLY) {
		// Optimized logical plan
		keys.push_back("logical_opt");
		values.push_back(logical_plan_opt);
	}
	if (explain_output_type == ExplainOutputType::ALL || explain_output_type == ExplainOutputType::PHYSICAL_ONLY) {
		// Physical plan
		keys.push_back("physical_plan");
		values.push_back(renderer.RenderPhysicalPlan(*plan));
	}

	// create a ColumnDataCollection from the output
	auto &allocator = Allocator::Get(context);
	vector<LogicalType> plan_types {LogicalType::VARCHAR, LogicalType::VARCHAR};
	auto collection =
	    make_uniq<ColumnDataCollection>(context, plan_types, ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR);

	DataChunk chunk;
	chunk.Initialize(allocator, op.types);
	for (idx_t i = 0; i < keys.size(); i++) {
		chunk.SetValue(0, chunk.size(), Value(keys[i]));
		chunk.SetValue(1, chunk.size(), Value(values[i]));
		chunk.SetCardinality(chunk.size() + 1);
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			collection->Append(chunk);
			chunk.Reset();
		}
	}
	collection->Append(chunk);

	// create a chunk scan to output the result
	auto chunk_scan = make_uniq<PhysicalColumnDataScan>(op.types, PhysicalOperatorType::COLUMN_DATA_SCAN,
	                                                    op.estimated_cardinality, std::move(collection));
	return std::move(chunk_scan);
}

} // namespace duckdb
