#include "duckdb/main/acero/declaration.hpp"
#include "duckdb/main/acero/dataset/table.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {
namespace ac {

shared_ptr<arrow::Table> ac::DeclarationToTable(shared_ptr<Declaration> plan) {
	DuckDB db(nullptr);
	Connection con(db);

	// Construct an arrow scan as Source
	// then construct a PhysicalPlan from the LogicalPlan inside the Declaration
	// Assign an arrow result collector

	// Create an executor
	// execute the pipelines

	// create an arrow table from the arrow query result
}

} // namespace ac
} // namespace duckdb
