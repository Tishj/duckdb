#include "duckdb/common/exception.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformListOfFiles(duckdb_libpgquery::PGFuncCall &list_value) {
	auto expr = TransformFuncCall(list_value);
	D_ASSERT(expr->type == ExpressionType::FUNCTION);
	auto &function = expr->Cast<FunctionExpression>();
	D_ASSERT(function.function_name == "list_value");

	if (function.children.empty()) {
		throw ParserException("List of files can not be empty");
	}

	// Figure out what the file extension is
	string file_extension;
	vector<Value> values;
	for (auto &child_p : function.children) {
		if (child_p->type != ExpressionType::VALUE_CONSTANT) {
			throw ParserException("Provided file paths should be a constant value");
		}
		auto &child = child_p->Cast<ConstantExpression>();
		auto &value = child.value;
		if (value.type().id() != LogicalTypeId::VARCHAR) {
			throw ParserException("The provided file path should be a string");
		}
		auto &path = StringValue::Get(value);
		auto dot_location = path.rfind('.');
		if (dot_location == std::string::npos) {
			throw ParserException("The provided file path does not have an extension (name.[json|parquet|csv etc])");
		}
		auto child_file_extension = path.substr(dot_location + 1, std::string::npos);
		if (!StringUtil::CIEquals(child_file_extension, "csv")) {
			throw NotImplementedException("Only CSV files are supported currently");
		}
		if (FileSystem::HasGlob(path)) {
			throw ParserException("No globs are allowed here");
		}
		values.push_back(path);
		file_extension = child_file_extension;
	}

	// Create the replacement scan based on the extension
	if (StringUtil::CIEquals(file_extension, "csv")) {
		auto table_function = make_uniq<TableFunctionRef>();

		auto list_of_paths = Value::LIST(std::move(values));
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(make_uniq<ConstantExpression>(std::move(list_of_paths)));
		table_function->function = make_uniq<FunctionExpression>("read_csv_auto", std::move(children));
		return table_function;
	} else {
		throw InternalException("This shouldn't happen, replacement scan not implemented for %s", file_extension);
	}
}

unique_ptr<TableRef> Transformer::TransformTableRefNode(duckdb_libpgquery::PGNode &n) {
	auto stack_checker = StackCheck();

	switch (n.type) {
	case duckdb_libpgquery::T_PGRangeVar:
		return TransformRangeVar(PGCast<duckdb_libpgquery::PGRangeVar>(n));
	case duckdb_libpgquery::T_PGJoinExpr:
		return TransformJoin(PGCast<duckdb_libpgquery::PGJoinExpr>(n));
	case duckdb_libpgquery::T_PGRangeSubselect:
		return TransformRangeSubselect(PGCast<duckdb_libpgquery::PGRangeSubselect>(n));
	case duckdb_libpgquery::T_PGRangeFunction:
		return TransformRangeFunction(PGCast<duckdb_libpgquery::PGRangeFunction>(n));
	case duckdb_libpgquery::T_PGPivotExpr:
		return TransformPivot(PGCast<duckdb_libpgquery::PGPivotExpr>(n));
	case duckdb_libpgquery::T_PGFuncCall:
		return TransformListOfFiles(PGCast<duckdb_libpgquery::PGFuncCall>(n));
	default:
		throw NotImplementedException("From Type %d not supported", n.type);
	}
}

} // namespace duckdb
