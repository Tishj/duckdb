#include "duckdb/main/acero/dataset/dataset.hpp"

namespace duckdb {
namespace arrow {
namespace dataset {

Dataset::Dataset(unique_ptr<DuckDB> db_p, unique_ptr<Connection> conn_p, const string &query)
    : db(std::move(db_p)), conn(std::move(conn_p)) {

	auto initial_result = conn->Query(query);
	auto client_properties = conn->context->GetClientProperties();
	auto types = initial_result->types;
	auto names = initial_result->names;
	this->factory = make_uniq<ac::ArrowTestFactory>(std::move(types), std::move(names), std::move(initial_result),
	                                                false, client_properties);
}

uintptr_t Dataset::ArrowObject() {
	return (uintptr_t)this->factory.get();
}

} // namespace dataset
} // namespace arrow
} // namespace duckdb
