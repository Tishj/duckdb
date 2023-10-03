#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {
namespace ac {

class ArrowStreamTestFactory {
public:
	static unique_ptr<ArrowArrayStreamWrapper> CreateStream(uintptr_t this_ptr, ArrowStreamParameters &parameters);

	static void GetSchema(uintptr_t factory_ptr, ArrowSchemaWrapper &schema);
};

} // namespace ac
} // namespace duckdb
