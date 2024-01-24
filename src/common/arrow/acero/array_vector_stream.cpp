#include "duckdb/main/acero/util/array_vector_stream.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"

namespace duckdb {
namespace arrow {

int ArrayVectorStream::GetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	if (!stream->release) {
		return -1;
	}
	auto data = reinterpret_cast<ArrayVectorStream *>(stream->private_data);
	auto &types = data->table.types;
	auto &names = data->table.names;
	auto &properties = data->table.properties;
	ArrowConverter::ToArrowSchema(out, types, names, properties);
	return 0;
}

namespace {

struct ArtificialStruct {
	vector<ArrowArray> children;
	vector<ArrowArray *> child_pointers;
};

} // namespace

static void ArrayRelease(ArrowArray *array) {
	if (!array->release) {
		return;
	}
	for (int64_t i = 0; i < array->n_children; i++) {
		auto child = array->children[i];
		if (child->release) {
			child->release(child);
		}
	}
	delete reinterpret_cast<ArtificialStruct *>(array->private_data);
	array->release = nullptr;
}

int ArrayVectorStream::GetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	if (!stream->release) {
		return -1;
	}
	if (!stream->private_data) {
		return -1;
	}
	auto data = reinterpret_cast<ArrayVectorStream *>(stream->private_data);
	auto &chunk_index = data->chunk_index;
	auto &table = data->table;
	if (table.arrays.empty()) {
		return -1;
	}
	auto chunk_count = table.arrays[0]->Count();
	if (chunk_index >= chunk_count) {
		out->release = nullptr;
		return 0;
	}

	auto artificial_struct = make_uniq<ArtificialStruct>();
	artificial_struct->child_pointers.resize(table.arrays.size());
	artificial_struct->children.resize(table.arrays.size());

	out->n_children = artificial_struct->child_pointers.size();
	out->children = (ArrowArray **)artificial_struct->child_pointers.data();
	for (idx_t i = 0; i < table.arrays.size(); i++) {
		auto &col = table.arrays[i];
		auto array = col->TakeChunk(chunk_index);
		out->length = array.length;
		artificial_struct->children[i] = array;
		artificial_struct->child_pointers[i] = &artificial_struct->children[i];
	}
	chunk_index++;
	out->private_data = artificial_struct.release();
	out->release = ArrayRelease;

	return 0;
}

void ArrayVectorStream::Release(struct ArrowArrayStream *stream) {
	if (!stream || !stream->release) {
		return;
	}
	stream->release = nullptr;
	delete reinterpret_cast<ArrayVectorStream *>(stream->private_data);
}

const char *ArrayVectorStream::GetLastError(struct ArrowArrayStream *stream) {
	return nullptr;
}

ArrayVectorStream::ArrayVectorStream(Table &&table) : table(std::move(table)) {
	//! We first initialize the private data of the stream
	auto &stream = GetStream();

	stream.private_data = this;
	//! We initialize the stream functions
	stream.get_schema = ArrayVectorStream::GetSchema;
	stream.get_next = ArrayVectorStream::GetNext;
	stream.release = ArrayVectorStream::Release;
	stream.get_last_error = ArrayVectorStream::GetLastError;
}

} // namespace arrow
} // namespace duckdb
