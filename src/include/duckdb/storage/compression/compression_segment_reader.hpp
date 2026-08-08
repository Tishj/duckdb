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

//! A position/offset into the buffer, measured in bytes
using byte_offset_t = idx_t;
//! A span or count of bytes (e.g. how many bytes to read, or the total buffer size)
using byte_length_t = idx_t;

//! A minimal, C++17-compatible stand-in for std::span<T>: a non-owning
//! (pointer, count) pair that keeps array length attached to the pointer.
template <class T>
class ArrayPtr {
public:
	ArrayPtr() : ptr(nullptr), count(0) {
	}
	ArrayPtr(T *ptr_p, idx_t count_p) : ptr(ptr_p), count(count_p) {
	}

	T *data() const {
		return ptr;
	}
	idx_t size() const {
		return count;
	}
	bool empty() const {
		return count == 0;
	}

	T &operator[](idx_t i) const {
		return ptr[i];
	}

	T *begin() const {
		return ptr;
	}
	T *end() const {
		return ptr + count;
	}

private:
	T *ptr;
	idx_t count;
};

//! A non-owning reader for persisted compression data.
//! The BufferHandle backing a segment reader must outlive the reader.
class CompressionSegmentReader {
public:
	//! Construct a reader over the (validated) block-offset region of a persisted segment.
	static CompressionSegmentReader Create(BufferHandle &handle, const ColumnSegment &segment, const char *context_p) {
		auto block_size = segment.GetBlockSize();
		auto block_offset = segment.GetBlockOffset();
		if (block_offset > block_size) {
			ThrowOutOfRange(block_offset, 0, block_size, context_p);
		}
		return CompressionSegmentReader(handle.GetDataMutable() + block_offset, block_size - block_offset, context_p);
	}

	byte_length_t Size() const {
		return size;
	}

	byte_offset_t Position() const {
		return position;
	}

	byte_length_t Remaining() const {
		return size - position;
	}

	bool Finished() const {
		return position == size;
	}

	//! Methods that don't modify state
public:
	//! Read of `length` bytes at byte offset `read_offset`
	data_ptr_t GetSpan(byte_offset_t read_offset, byte_length_t length) const {
		CheckRange(read_offset, length);
		return data + read_offset;
	}

	//! Read a value of type T at byte offset `read_offset`
	template <class T>
	T ReadAt(byte_offset_t read_offset) const {
		static_assert(std::is_trivially_copyable<T>::value, "CompressionSegmentReader can only read trivial types");
		return Load<T>(GetSpan(read_offset, sizeof(T)));
	}

	//! Returns a non-owning view of `count` elements of T starting at `read_offset` bytes into the buffer.
	template <class T>
	ArrayPtr<T> GetArray(byte_offset_t read_offset, idx_t count) const {
		static_assert(std::is_trivially_copyable<T>::value,
		              "CompressionSegmentReader can only expose arrays of trivial types");
		auto byte_count = CheckedByteCount<T>(read_offset, count);
		auto ptr = GetSpan(read_offset, byte_count);
		if (reinterpret_cast<uintptr_t>(ptr) % alignof(T) != 0) {
			throw IOException("Corrupted %s: typed array at offset %llu is not aligned to %llu bytes", Context(),
			                  read_offset, alignof(T));
		}
		return ArrayPtr<T>(reinterpret_cast<T *>(ptr), count);
	}

	//! Like GetArray, but `element_offset` is expressed in units of T rather than bytes.
	template <class T>
	ArrayPtr<T> GetArrayAt(idx_t element_offset, idx_t count) const {
		if (element_offset > size / sizeof(T) || count > size / sizeof(T) - element_offset) {
			ThrowOutOfRange(element_offset, count, size / sizeof(T), Context());
		}
		return GetArray<T>(element_offset * sizeof(T), count);
	}

	//! Similar to `GetSpan(read_offset, length)` only the result is a sub-reader, rather than raw bytes
	CompressionSegmentReader SubReader(byte_offset_t read_offset, byte_length_t length, const char *sub_context) const {
		return CompressionSegmentReader(GetSpan(read_offset, length), length, sub_context);
	}

	//! Methods that modify state
public:
	void SetPosition(byte_offset_t new_position) {
		CheckRange(new_position, 0);
		position = new_position;
	}

	void Skip(byte_length_t length) {
		CheckRange(position, length);
		position += length;
	}

	void Rewind(byte_length_t length) {
		if (length > position) {
			ThrowOutOfRange(position, length, size, Context());
		}
		position -= length;
	}

	void Align(byte_length_t alignment) {
		if (alignment == 0) {
			throw IOException("Corrupted %s: cannot align a compression reader to zero bytes", Context());
		}
		auto remainder = position % alignment;
		if (remainder != 0) {
			Skip(alignment - remainder);
		}
	}

	//! WITH `sizeof(T)` as `length`:
	//! Read `length` bytes at the current position, advancing position by `length` in the process
	template <class T>
	T Read() {
		static_assert(std::is_trivially_copyable<T>::value, "CompressionSegmentReader can only read trivial types");
		auto ptr = ReadSpan(sizeof(T));
		return Load<T>(ptr);
	}

	//! WITH `sizeof(T)` as `length`:
	//! Read `length` bytes ranging from position-`length` to position, decrementing position by `length` in the process
	template <class T>
	T ReadBackward() {
		static_assert(std::is_trivially_copyable<T>::value, "CompressionSegmentReader can only read trivial types");
		if (sizeof(T) > position) {
			ThrowOutOfRange(position, sizeof(T), size, Context());
		}
		position -= sizeof(T);
		return Load<T>(data + position);
	}

	//! Read of `length` bytes at the current position, advancing position by `length` in the process
	data_ptr_t ReadSpan(byte_length_t length) {
		auto result = GetSpan(position, length);
		position += length;
		return result;
	}

	//! Read `length` bytes ranging from position-`length` to position, decrementing position by `length` in the process
	data_ptr_t ReadBackwardSpan(byte_length_t length) {
		if (length > position) {
			ThrowOutOfRange(position, length, size, Context());
		}
		position -= length;
		return data + position;
	}

	//! Read `count` elements of T at the current position, advancing position by `count * sizeof(T)` in the process
	template <class T>
	ArrayPtr<T> ReadArray(idx_t count) {
		auto result = GetArray<T>(position, count);
		position += count * sizeof(T);
		return result;
	}

	//! Read `length` bytes at the current position, advancing position by `length` in the process
	void CopyTo(data_ptr_t target, byte_length_t length) {
		auto source = ReadSpan(length);
		memcpy(target, source, length);
	}

	//! Create a sub-reader and increment this reader's position by `length`
	CompressionSegmentReader ReadSubReader(byte_length_t length, const char *sub_context) {
		return CompressionSegmentReader(ReadSpan(length), length, sub_context);
	}

private:
	CompressionSegmentReader(data_ptr_t data_p, byte_length_t size_p, const char *context_p)
	    : data(data_p), size(size_p), position(0), context(context_p) {
	}

	const char *Context() const {
		return context ? context : "compressed segment";
	}

	void CheckRange(byte_offset_t read_offset, byte_length_t length) const {
		if (read_offset > size || length > size - read_offset) {
			ThrowOutOfRange(read_offset, length, size, Context());
		}
	}

	template <class T>
	byte_length_t CheckedByteCount(byte_offset_t read_offset, idx_t count) const {
		if (read_offset > size || count > (size - read_offset) / sizeof(T)) {
			ThrowOutOfRange(read_offset, count, size, Context());
		}
		return count * sizeof(T);
	}

	//! Static so it's usable from Create() before an instance exists.
	[[noreturn]] static void ThrowOutOfRange(idx_t read_offset, idx_t length, idx_t capacity, const char *context) {
		throw IOException("Corrupted %s: read at offset %llu with length %llu exceeds capacity %llu",
		                  context ? context : "compressed segment", read_offset, length, capacity);
	}

private:
	const data_ptr_t data;
	const byte_length_t size;
	byte_offset_t position;
	const char *context;
};

} // namespace duckdb
