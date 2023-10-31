#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/main/relation.hpp"

namespace duckdb {
namespace ac {

class AceroTPCHNodes {
public:
public:
	static shared_ptr<Relation> DuckDBTpchQuery6(shared_ptr<ClientContext> &context);
	static shared_ptr<Relation> DuckDBTpchQuery1(shared_ptr<ClientContext> &context);
	static shared_ptr<Relation> AceroTpchQuery1(shared_ptr<ClientContext> &context);
	static shared_ptr<Relation> AceroTpchQuery6(shared_ptr<ClientContext> &context);
};

} // namespace ac
} // namespace duckdb
