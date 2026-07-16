//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/compression_segment_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/fast_mem.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/table/column_segment.hpp"

#include <type_traits>

namespace duckdb {

//! A non-owning reader for persisted compression data.
//! The BufferHandle backing a segment reader must outlive the reader.
class CompressionSegmentReader {
public:
	CompressionSegmentReader(data_ptr_t data_p, idx_t size_p, const char *context_p)
	    : data(data_p), size(size_p), position(0), context(context_p) {
	}

	CompressionSegmentReader(BufferHandle &handle, const ColumnSegment &segment, const char *context_p)
	    : data(nullptr), size(0), position(0), context(context_p) {
		auto block_size = segment.GetBlockSize();
		auto block_offset = segment.GetBlockOffset();
		if (block_offset > block_size) {
			ThrowOutOfRange(block_offset, 0, block_size);
		}
		data = handle.GetDataMutable() + block_offset;
		size = block_size - block_offset;
	}

public:
	idx_t Size() const {
		return size;
	}

	idx_t Position() const {
		return position;
	}

	idx_t Remaining() const {
		return size - position;
	}

	bool Finished() const {
		return position == size;
	}

	void SetPosition(idx_t new_position) {
		CheckRange(new_position, 0);
		position = new_position;
	}

	void Skip(idx_t length) {
		CheckRange(position, length);
		position += length;
	}

	void Rewind(idx_t length) {
		if (length > position) {
			ThrowOutOfRange(position, length, size);
		}
		position -= length;
	}

	void Align(idx_t alignment) {
		if (alignment == 0) {
			throw IOException("Corrupted %s: cannot align a compression reader to zero bytes", Context());
		}
		auto remainder = position % alignment;
		if (remainder != 0) {
			Skip(alignment - remainder);
		}
	}

	template <class T>
	T Read() {
		static_assert(std::is_trivially_copyable<T>::value, "CompressionSegmentReader can only read trivial types");
		auto ptr = ReadSpan(sizeof(T));
		return Load<T>(ptr);
	}

	template <class T>
	T ReadAt(idx_t read_offset) const {
		static_assert(std::is_trivially_copyable<T>::value, "CompressionSegmentReader can only read trivial types");
		return Load<T>(GetSpan(read_offset, sizeof(T)));
	}

	template <class T>
	T ReadBackward() {
		static_assert(std::is_trivially_copyable<T>::value, "CompressionSegmentReader can only read trivial types");
		if (sizeof(T) > position) {
			ThrowOutOfRange(position, sizeof(T), size);
		}
		position -= sizeof(T);
		return Load<T>(data + position);
	}

	data_ptr_t GetSpan(idx_t read_offset, idx_t length) const {
		CheckRange(read_offset, length);
		return data + read_offset;
	}

	data_ptr_t ReadSpan(idx_t length) {
		auto result = GetSpan(position, length);
		position += length;
		return result;
	}

	data_ptr_t ReadBackwardSpan(idx_t length) {
		if (length > position) {
			ThrowOutOfRange(position, length, size);
		}
		position -= length;
		return data + position;
	}

	template <class T>
	T *GetArray(idx_t read_offset, idx_t count) const {
		static_assert(std::is_trivially_copyable<T>::value,
		              "CompressionSegmentReader can only expose arrays of trivial types");
		auto byte_count = CheckedByteCount<T>(read_offset, count);
		auto ptr = GetSpan(read_offset, byte_count);
		if (reinterpret_cast<uintptr_t>(ptr) % alignof(T) != 0) {
			throw IOException("Corrupted %s: typed array at offset %llu is not aligned to %llu bytes", Context(),
			                  read_offset, alignof(T));
		}
		return reinterpret_cast<T *>(ptr);
	}

	template <class T>
	T *ReadArray(idx_t count) {
		auto result = GetArray<T>(position, count);
		position += count * sizeof(T);
		return result;
	}

	template <class T>
	T *GetArrayAt(idx_t element_offset, idx_t count) const {
		if (element_offset > size / sizeof(T) || count > size / sizeof(T) - element_offset) {
			ThrowOutOfRange(element_offset, count, size / sizeof(T));
		}
		return GetArray<T>(element_offset * sizeof(T), count);
	}

	void CopyTo(data_ptr_t target, idx_t length) {
		auto source = ReadSpan(length);
		memcpy(target, source, length);
	}

	CompressionSegmentReader SubReader(idx_t read_offset, idx_t length, const char *sub_context) const {
		return CompressionSegmentReader(GetSpan(read_offset, length), length, sub_context);
	}

	CompressionSegmentReader ReadSubReader(idx_t length, const char *sub_context) {
		return CompressionSegmentReader(ReadSpan(length), length, sub_context);
	}

private:
	const char *Context() const {
		return context ? context : "compressed segment";
	}

	void CheckRange(idx_t read_offset, idx_t length) const {
		if (read_offset > size || length > size - read_offset) {
			ThrowOutOfRange(read_offset, length, size);
		}
	}

	template <class T>
	idx_t CheckedByteCount(idx_t read_offset, idx_t count) const {
		if (read_offset > size || count > (size - read_offset) / sizeof(T)) {
			ThrowOutOfRange(read_offset, count, size);
		}
		return count * sizeof(T);
	}

	[[noreturn]] void ThrowOutOfRange(idx_t read_offset, idx_t length, idx_t capacity) const {
		throw IOException("Corrupted %s: read at offset %llu with length %llu exceeds capacity %llu", Context(),
		                  read_offset, length, capacity);
	}

private:
	data_ptr_t data;
	idx_t size;
	idx_t position;
	const char *context;
};

} // namespace duckdb
