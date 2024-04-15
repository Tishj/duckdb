//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/common_table_expression_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/common/enums/cte_materialize.hpp"

namespace duckdb {

class SelectStatement;

struct CommonTableExpressionInfo {
	CommonTableExpressionInfo();
	~CommonTableExpressionInfo();
	CommonTableExpressionInfo(const CommonTableExpressionInfo &) = delete;
	CommonTableExpressionInfo(CommonTableExpressionInfo &&) noexcept;

public:
	vector<string> aliases;
	unique_ptr<SelectStatement> query;
	CTEMaterialize materialized = CTEMaterialize::CTE_MATERIALIZE_DEFAULT;

public:
	bool Equals(const CommonTableExpressionInfo &other) const;
	void Serialize(Serializer &serializer) const;
	static unique_ptr<CommonTableExpressionInfo> Deserialize(Deserializer &deserializer);
	unique_ptr<CommonTableExpressionInfo> Copy();
};

} // namespace duckdb
