#include "duckdb/main/acero/util/arrow_test_factory.hpp"

namespace duckdb {
namespace ac {

int ArrowTestFactory::ArrowArrayStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	if (!stream->private_data) {
		throw InternalException("No private data!?");
	}
	auto &data = *((ArrowArrayStreamData *)stream->private_data);
	data.factory.ToArrowSchema(out);
	return 0;
}

int ArrowTestFactory::ArrowArrayStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	if (!stream->private_data) {
		throw InternalException("No private data!?");
	}
	auto &data = *((ArrowArrayStreamData *)stream->private_data);
	if (!data.factory.big_result) {
		auto chunk = data.factory.result->Fetch();
		if (!chunk || chunk->size() == 0) {
			return 0;
		}
		ArrowConverter::ToArrowArray(*chunk, out, data.options);
	} else {
		ArrowAppender appender(data.factory.result->types, STANDARD_VECTOR_SIZE, data.options);
		idx_t count = 0;
		while (true) {
			auto chunk = data.factory.result->Fetch();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			count += chunk->size();
			appender.Append(*chunk, 0, chunk->size(), chunk->size());
		}
		if (count > 0) {
			*out = appender.Finalize();
		}
	}
	return 0;
}

const char *ArrowTestFactory::ArrowArrayStreamGetLastError(struct ArrowArrayStream *stream) {
	throw InternalException("Error!?!!");
}

void ArrowTestFactory::ArrowArrayStreamRelease(struct ArrowArrayStream *stream) {
	if (!stream->private_data) {
		return;
	}
	auto data = (ArrowArrayStreamData *)stream->private_data;
	delete data;
	stream->private_data = nullptr;
}

duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> ArrowTestFactory::CreateStream(uintptr_t this_ptr,
                                                                                   ArrowStreamParameters &parameters) {
	//! Create a new batch reader
	auto &factory = *reinterpret_cast<ArrowTestFactory *>(this_ptr); //! NOLINT
	if (!factory.result) {
		throw InternalException("Stream already consumed!");
	}

	auto stream_wrapper = make_uniq<ArrowArrayStreamWrapper>();
	stream_wrapper->number_of_rows = -1;
	auto private_data = make_uniq<ArrowArrayStreamData>(factory, factory.options);
	stream_wrapper->arrow_array_stream.get_schema = ArrowArrayStreamGetSchema;
	stream_wrapper->arrow_array_stream.get_next = ArrowArrayStreamGetNext;
	stream_wrapper->arrow_array_stream.get_last_error = ArrowArrayStreamGetLastError;
	stream_wrapper->arrow_array_stream.release = ArrowArrayStreamRelease;
	stream_wrapper->arrow_array_stream.private_data = private_data.release();

	return stream_wrapper;
}

void ArrowTestFactory::GetSchema(uintptr_t factory_ptr, duckdb::ArrowSchemaWrapper &schema) {
	//! Create a new batch reader
	auto &factory = *reinterpret_cast<ArrowTestFactory *>(factory_ptr); //! NOLINT
	factory.ToArrowSchema(&schema.arrow_schema);
}

void ArrowTestFactory::ToArrowSchema(struct ArrowSchema *out) {
	ArrowConverter::ToArrowSchema(out, types, names, options);
}

} // namespace ac
} // namespace duckdb
