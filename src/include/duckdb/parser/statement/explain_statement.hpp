//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/explain_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

enum class ExplainType : uint8_t { EXPLAIN_STANDARD, EXPLAIN_ANALYZE };
enum class ExplainFormat : uint8_t { TEXT, JSON };

class ExplainRenderer {
public:
	ExplainRenderer() {
	}
	virtual ~ExplainRenderer() {
	}

public:
	virtual string RenderUnoptimizedPlan(const LogicalOperator &plan) = 0;
	virtual string RenderLogicalPlan(const LogicalOperator &plan) = 0;
	virtual string RenderPhysicalPlan(const PhysicalOperator &plan) = 0;
	virtual unique_ptr<ExplainRenderer> Copy() const = 0;
};

class TextExplainRenderer : public ExplainRenderer {
public:
	TextExplainRenderer() {
	}
	~TextExplainRenderer() override {
	}

public:
	string RenderUnoptimizedPlan(const LogicalOperator &plan) override;
	string RenderLogicalPlan(const LogicalOperator &plan) override;
	string RenderPhysicalPlan(const PhysicalOperator &plan) override;
	unique_ptr<ExplainRenderer> Copy() const override;
};

class JSONExplainRenderer : public ExplainRenderer {
public:
	JSONExplainRenderer() {
	}
	~JSONExplainRenderer() override {
	}

public:
	string RenderUnoptimizedPlan(const LogicalOperator &plan) override;
	string RenderLogicalPlan(const LogicalOperator &plan) override;
	string RenderPhysicalPlan(const PhysicalOperator &plan) override;
	unique_ptr<ExplainRenderer> Copy() const override;
};

class ExplainStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::EXPLAIN_STATEMENT;

public:
	explicit ExplainStatement(unique_ptr<SQLStatement> stmt, unique_ptr<ExplainRenderer> renderer,
	                          ExplainType explain_type = ExplainType::EXPLAIN_STANDARD);

	unique_ptr<SQLStatement> stmt;
	unique_ptr<ExplainRenderer> renderer;
	ExplainType explain_type;

protected:
	ExplainStatement(const ExplainStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
	ExplainRenderer &GetRenderer();
};

} // namespace duckdb
