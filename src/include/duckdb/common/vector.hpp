//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/memory_safety.hpp"
#include <vector>

namespace duckdb {

template <class _Tp, bool SAFE = true>
class vector : public std::vector<_Tp, std::allocator<_Tp>> {
public:
	using original = std::vector<_Tp, std::allocator<_Tp>>;
	using size_type = typename original::size_type;
	using const_reference = typename original::const_reference;
	using reference = typename original::reference;
	using value_type = typename original::value_type;

private:
	static inline void AssertIndexInBounds(idx_t index, idx_t size) {
#if defined(DUCKDB_DEBUG_NO_SAFETY) || defined(DUCKDB_CLANG_TIDY)
		return;
#else
		if (DUCKDB_UNLIKELY(index >= size)) {
			throw InternalException("Attempted to access index %ld within vector of size %ld", index, size);
		}
#endif
	}

public:
#ifdef DUCKDB_CLANG_TIDY
	// This is necessary to tell clang-tidy that it reinitializes the variable after a move
	[[clang::reinitializes]]
#endif
	inline void
	clear() noexcept {
		original::clear();
	}

	// Move constructor from base class
	vector(original &&other) : original(std::move(other)) {
	}

	// Construct a new vector of this type from another vector with a different SAFE level
	template <bool _SAFE>
	vector(vector<_Tp, _SAFE> &&other) : original(std::move(other)) {
	}

	// Default constructor
	vector() : original() {
	}

	// Constructor with allocator
	explicit vector(const std::allocator<_Tp> &__a) : original(__a) {
	}

	// Constructor with size
	explicit vector(size_type __n) : original(__n) {
	}

	// Constructor with size and value
	vector(size_type __n, const value_type &__x) : original(__n, __x) {
	}

	// Constructor with size, value, and allocator
	vector(size_type __n, const value_type &__x, const std::allocator<_Tp> &__a) : original(__n, __x, __a) {
	}

	// Range constructor with iterators
	template <class _InputIterator>
	vector(_InputIterator __first, _InputIterator __last) : original(__first, __last) {
	}

	// Range constructor with iterators and allocator
	template <class _InputIterator>
	vector(_InputIterator __first, _InputIterator __last, const std::allocator<_Tp> &__a)
	    : original(__first, __last, __a) {
	}

	// Copy constructor
	vector(const vector &__x) : original(__x) {
	}

	// Copy constructor with allocator
	vector(const vector &__x, const std::allocator<_Tp> &__a) : original(__x, __a) {
	}

	// Move constructor
	vector(vector &&__x) noexcept : original(std::move(__x)) {
	}

	// Move constructor with allocator
	vector(vector &&__x, const std::allocator<_Tp> &__a) : original(std::move(__x), __a) {
	}

	// Initializer list constructor
	vector(std::initializer_list<_Tp> __il) : original(__il) {
	}

	// Initializer list constructor with allocator
	vector(std::initializer_list<_Tp> __il, const std::allocator<_Tp> &__a) : original(__il, __a) {
	}

	// Copy assignment operator
	vector &operator=(const vector &__x) {
		original::operator=(__x);
		return *this;
	}

	// Move assignment operator
	vector &operator=(vector &&__x) noexcept {
		original::operator=(std::move(__x));
		return *this;
	}

	// Initializer list assignment operator
	vector &operator=(std::initializer_list<_Tp> __il) {
		original::operator=(__il);
		return *this;
	}

	template <bool _SAFE = false>
	inline typename original::reference get(typename original::size_type __n) {
		if (MemorySafety<_SAFE>::enabled) {
			AssertIndexInBounds(__n, original::size());
		}
		return original::operator[](__n);
	}

	template <bool _SAFE = false>
	inline typename original::const_reference get(typename original::size_type __n) const {
		if (MemorySafety<_SAFE>::enabled) {
			AssertIndexInBounds(__n, original::size());
		}
		return original::operator[](__n);
	}

	typename original::reference operator[](typename original::size_type __n) {
		return get<SAFE>(__n);
	}
	typename original::const_reference operator[](typename original::size_type __n) const {
		return get<SAFE>(__n);
	}

	typename original::reference front() {
		return get<SAFE>(0);
	}

	typename original::const_reference front() const {
		return get<SAFE>(0);
	}

	typename original::reference back() {
		if (original::empty()) {
			throw InternalException("'back' called on an empty vector!");
		}
		return get<SAFE>(original::size() - 1);
	}

	typename original::const_reference back() const {
		if (original::empty()) {
			throw InternalException("'back' called on an empty vector!");
		}
		return get<SAFE>(original::size() - 1);
	}
};

template <typename T>
using unsafe_vector = vector<T, false>;

} // namespace duckdb
