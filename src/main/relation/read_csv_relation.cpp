#include "duckdb/main/relation/read_csv_relation.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"

namespace duckdb {

// ReadCSVRelation::ReadCSVRelation(const std::shared_ptr<ClientContext> &context, string csv_file_p,
//                                  vector<ColumnDefinition> columns_p, string alias_p)
//     : TableFunctionRelation(context, "read_csv", {Value(csv_file_p)}), alias(std::move(alias_p)),
//       auto_detect(false), csv_file(std::move(csv_file_p)) {

//	if (alias.empty()) {
//		alias = StringUtil::Split(csv_file, ".")[0];
//	}
//}

ReadCSVRelation::ReadCSVRelation(const std::shared_ptr<ClientContext> &context, string csv_file_p,
                                 named_parameter_map_t options, string alias_p)
    : TableFunctionRelation(context, options.count("columns") ? "read_csv" : "read_csv_auto", {Value(csv_file_p)},
                            std::move(options)),
      alias(std::move(alias_p)), auto_detect(true), csv_file(std::move(csv_file_p)) {

	if (alias.empty()) {
		alias = StringUtil::Split(csv_file, ".")[0];
	}
}

string ReadCSVRelation::GetAlias() {
	return alias;
}

} // namespace duckdb
