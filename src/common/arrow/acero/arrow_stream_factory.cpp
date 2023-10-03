#include "duckdb/main/acero/util/arrow_stream_factory.hpp"

namespace duckdb {
namespace ac {

unique_ptr<ArrowArrayStreamWrapper> ArrowStreamTestFactory::CreateStream(uintptr_t this_ptr,
                                                                         ArrowStreamParameters &parameters) {
	auto stream_wrapper = make_uniq<ArrowArrayStreamWrapper>();
	stream_wrapper->number_of_rows = -1;
	stream_wrapper->arrow_array_stream = *(ArrowArrayStream *)this_ptr;

	return stream_wrapper;
}

void ArrowStreamTestFactory::GetSchema(uintptr_t factory_ptr, duckdb::ArrowSchemaWrapper &schema) {
	auto &factory = *reinterpret_cast<ArrowArrayStreamWrapper *>(factory_ptr); //! NOLINT
	factory.arrow_array_stream.get_schema(&factory.arrow_array_stream, &schema.arrow_schema);
}

} // namespace ac
} // namespace duckdb
