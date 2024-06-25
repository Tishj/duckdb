#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/common/json/serializer/json_serializer.hpp"
#include "yyjson.hpp"
#include "duckdb/common/enum_util.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

string TextExplainRenderer::RenderUnoptimizedPlan(const LogicalOperator &plan) {
	return plan.ToString();
}

string TextExplainRenderer::RenderLogicalPlan(const LogicalOperator &plan) {
	return plan.ToString();
}

string TextExplainRenderer::RenderPhysicalPlan(const PhysicalOperator &plan) {
	return plan.ToString();
}

unique_ptr<ExplainRenderer> TextExplainRenderer::Copy() const {
	return make_uniq<TextExplainRenderer>();
}

static string LogicalPlanToJSON(const LogicalOperator &plan) {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto result_obj = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, result_obj);

	yyjson_mut_obj_add_false(doc, result_obj, "error");
	auto plans_arr = yyjson_mut_arr(doc);

	auto plan_json = JsonSerializer::Serialize(plan, doc, false, false, false);
	yyjson_mut_arr_append(plans_arr, plan_json);
	yyjson_mut_obj_add_val(doc, result_obj, "plans", plans_arr);
	size_t len;
	auto data = yyjson_mut_val_write_opts(result_obj, YYJSON_WRITE_ALLOW_INF_AND_NAN | YYJSON_WRITE_PRETTY, nullptr,
	                                      &len, nullptr);
	if (!data) {
		throw SerializationException("Could not render the plan as JSON");
	}
	return string(data);
}

string JSONExplainRenderer::RenderUnoptimizedPlan(const LogicalOperator &plan) {
	return LogicalPlanToJSON(plan);
}

string JSONExplainRenderer::RenderLogicalPlan(const LogicalOperator &plan) {
	return LogicalPlanToJSON(plan);
}

string JSONExplainRenderer::RenderPhysicalPlan(const PhysicalOperator &plan) {
	throw NotImplementedException("PhysicalOperator can not be serialized to JSON currently");
}

unique_ptr<ExplainRenderer> JSONExplainRenderer::Copy() const {
	return make_uniq<JSONExplainRenderer>();
}

ExplainStatement::ExplainStatement(unique_ptr<SQLStatement> stmt, unique_ptr<ExplainRenderer> renderer,
                                   ExplainType explain_type)
    : SQLStatement(StatementType::EXPLAIN_STATEMENT), stmt(std::move(stmt)), renderer(std::move(renderer)),
      explain_type(explain_type) {
}

ExplainStatement::ExplainStatement(const ExplainStatement &other)
    : SQLStatement(other), stmt(other.stmt->Copy()), explain_type(other.explain_type) {
}

unique_ptr<SQLStatement> ExplainStatement::Copy() const {
	return unique_ptr<ExplainStatement>(new ExplainStatement(*this));
}

static string ExplainTypeToString(ExplainType type) {
	switch (type) {
	case ExplainType::EXPLAIN_STANDARD:
		return "EXPLAIN";
	case ExplainType::EXPLAIN_ANALYZE:
		return "EXPLAIN ANALYZE";
	default:
		throw InternalException("ToString for ExplainType with type: %s not implemented", EnumUtil::ToString(type));
	}
}

string ExplainStatement::ToString() const {
	string result = "";
	result += ExplainTypeToString(explain_type);
	result += " " + stmt->ToString();
	return result;
}

ExplainRenderer &ExplainStatement::GetRenderer() {
	return *renderer;
}

} // namespace duckdb
