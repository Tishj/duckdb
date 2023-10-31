#include <iostream>
#include <map>
#include <set>
#include "duckdb/main/acero/dataset/scan_options.hpp"
#include "duckdb/main/acero/dataset/scan_node_options.hpp"
#include "duckdb/main/acero/hash_join_node_options.hpp"
#include "duckdb/main/acero/dataset/dataset.hpp"
#include "duckdb/main/acero/declaration.hpp"
#include "duckdb/main/acero/project_node_options.hpp"
#include "duckdb/main/acero/compute/expression.hpp"
#include "duckdb/main/acero/compute/compute.hpp"
#include "duckdb/main/acero/source_node_options.hpp"
#include "duckdb/main/acero/util/array_vector_stream.hpp"
#include "duckdb/main/acero/tpch_nodes.hpp"

#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/table_relation.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"

namespace duckdb {
namespace ac {

// clang-format off

vector<ColumnDefinition> LineItemColumns() {
	vector<ColumnDefinition> columns;
	columns.emplace_back("l_orderkey", LogicalType::INTEGER);
	columns.emplace_back("l_partkey", LogicalType::INTEGER);
	columns.emplace_back("l_suppkey", LogicalType::INTEGER);
	columns.emplace_back("l_linenumber", LogicalType::INTEGER);
	columns.emplace_back("l_quantity", LogicalType::DECIMAL(15, 2));
	columns.emplace_back("l_extendedprice", LogicalType::DECIMAL(15, 2));
	columns.emplace_back("l_discount", LogicalType::DECIMAL(15, 2));
	columns.emplace_back("l_tax", LogicalType::DECIMAL(15, 2));
	columns.emplace_back("l_returnflag", LogicalType::VARCHAR);
	columns.emplace_back("l_linestatus", LogicalType::VARCHAR);
	columns.emplace_back("l_shipdate", LogicalType::DATE);
	columns.emplace_back("l_commitdate", LogicalType::DATE);
	columns.emplace_back("l_receiptdate", LogicalType::DATE);
	columns.emplace_back("l_shipinstruct", LogicalType::VARCHAR);
	columns.emplace_back("l_shipmode", LogicalType::VARCHAR);
	columns.emplace_back("l_comment", LogicalType::VARCHAR);
	return columns;
}

shared_ptr<Relation> AceroTPCHNodes::DuckDBTpchQuery6(shared_ptr<ClientContext> &context) {
	shared_ptr<Relation> plan;

	context->Query("call dbgen(sf=0.1)", false);

	// root source: LogicalGet
	auto table_description = make_uniq<TableDescription>(
		"main",
		"lineitem",
		LineItemColumns()
	);
	plan = make_shared<TableRelation>(context, std::move(table_description));

	// Logical Filter
	vector<unique_ptr<ParsedExpression>> filter_expressions;
	filter_expressions.push_back(
		make_uniq<ComparisonExpression>(
			ExpressionType::COMPARE_GREATERTHANOREQUALTO,
			make_uniq<ColumnRefExpression>(
				"l_shipdate", "lineitem"
			),
			make_uniq<CastExpression>(
				LogicalType::DATE,
				make_uniq<ConstantExpression>(
					Value("1994-01-01")
				)
			)
		)
	);
	filter_expressions.push_back(
		make_uniq<ComparisonExpression>(
			ExpressionType::COMPARE_LESSTHAN,
			make_uniq<ColumnRefExpression>(
				"l_shipdate", "lineitem"
			),
			make_uniq<CastExpression>(
				LogicalType::DATE,
				make_uniq<ConstantExpression>(
					Value("1995-01-01")
				)
			)
		)
	);
	filter_expressions.push_back(
		make_uniq<ComparisonExpression>(
			ExpressionType::COMPARE_GREATERTHANOREQUALTO,
			make_uniq<ColumnRefExpression>(
				"l_discount", "lineitem"
			),
			make_uniq<CastExpression>(
				LogicalType::DECIMAL(15, 2),
				make_uniq<ConstantExpression>(
					// FIXME: should this really be 5?
					Value::DECIMAL(5, 3, 2)
				)
			)
		)
	);
	filter_expressions.push_back(
		make_uniq<ComparisonExpression>(
			ExpressionType::COMPARE_LESSTHAN,
			make_uniq<ColumnRefExpression>(
				"l_quantity", "lineitem"
			),
			make_uniq<CastExpression>(
				LogicalType::DECIMAL(15, 2),
				make_uniq<ConstantExpression>(
					Value::INTEGER(24)
				)
			)
		)
	);
	filter_expressions.push_back(
		make_uniq<ComparisonExpression>(
			ExpressionType::COMPARE_LESSTHANOREQUALTO,
			make_uniq<ColumnRefExpression>(
				"l_discount", "lineitem"
			),
			make_uniq<CastExpression>(
				LogicalType::DECIMAL(15, 2),
				make_uniq<ConstantExpression>(
					Value::DECIMAL(7, 3, 2)
				)
			)
		)
	);
	auto filter_expr = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(filter_expressions));
	plan = make_shared<FilterRelation>(std::move(plan), std::move(filter_expr));

	vector<unique_ptr<ParsedExpression>> multiply_input;
	multiply_input.push_back(make_uniq_base<ParsedExpression, ColumnRefExpression>("l_extendedprice", "lineitem"));
	multiply_input.push_back(make_uniq_base<ParsedExpression, ColumnRefExpression>("l_discount", "lineitem"));
	vector<unique_ptr<ParsedExpression>> sum_input;
	sum_input.push_back(
		make_uniq_base<ParsedExpression, FunctionExpression>(
			"*",
			std::move(multiply_input)
		)
	);
	// Logical Aggregate
	auto sum = make_uniq_base<ParsedExpression, FunctionExpression>(
		"sum",
		std::move(sum_input)
	);
	sum->alias = "revenue";
	vector<unique_ptr<ParsedExpression>> aggregate_input;
	aggregate_input.push_back(std::move(sum));
	plan = make_shared<AggregateRelation>(
		std::move(plan),
		std::move(aggregate_input)
	);

	// Logical projection
	vector<unique_ptr<ParsedExpression>> projection_input;
	projection_input.push_back(
		make_uniq_base<ParsedExpression, ColumnRefExpression>(
			"revenue"
		)
	);
	plan = make_shared<ProjectionRelation>(
		std::move(plan),
		std::move(projection_input),
		vector<string>{
			"revenue"
		}
	);
	return plan;
}
shared_ptr<Relation> AceroTPCHNodes::DuckDBTpchQuery1(shared_ptr<ClientContext> &context) {
	return nullptr;
}

// Acero

shared_ptr<Relation> AceroTPCHNodes::AceroTpchQuery6(shared_ptr<ClientContext> &context) {
	return nullptr;
}

shared_ptr<Relation> AceroTPCHNodes::AceroTpchQuery1(shared_ptr<ClientContext> &context) {
	// generate tpch and convert it to arrow so we can feed it to a SourceNode
	ac::Declaration lineitem {"source", ac::SourceNodeOptions {input_l->schema, input_l}};
	
}

} // namespace ac
} // namespace duckdb

// clang-format on
