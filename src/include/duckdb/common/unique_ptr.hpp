#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/memory_safety.hpp"

#include <memory>
#include <type_traits>

namespace duckdb {

template <bool SAFE>
struct DefaultHandler {};

template <>
struct DefaultHandler<true> {
	// Safety is enabled, do a nullptr check
	static void OP(const bool null) {
#if defined(DUCKDB_DEBUG_NO_SAFETY) || defined(DUCKDB_CLANG_TIDY)
		return;
#else
		if (DUCKDB_UNLIKELY(null)) {
			throw duckdb::InternalException("Attempted to dereference unique_ptr that is NULL!");
		}
#endif
	}
};

template <>
struct DefaultHandler<false> {
	static void OP(const bool null) {
	}
};

template <class DATA_TYPE, class DELETER = std::default_delete<DATA_TYPE>, bool SAFE = true,
          class AccessFailureHandler = DefaultHandler<MemorySafety<SAFE>::ENABLED>>
class unique_ptr : public std::unique_ptr<DATA_TYPE, DELETER> { // NOLINT: naming
public:
	using original = std::unique_ptr<DATA_TYPE, DELETER>;
	using original::original; // NOLINT
	using pointer = typename original::pointer;
	using element_type = typename original::element_type;

public:
	typename std::add_lvalue_reference<DATA_TYPE>::type operator*() const { // NOLINT: hiding on purpose
		const auto ptr = original::get();
		if (MemorySafety<SAFE>::ENABLED) {
			AccessFailureHandler::OP(!ptr);
		}
		return *ptr;
	}

	typename original::pointer operator->() const { // NOLINT: hiding on purpose
		const auto ptr = original::get();
		if (MemorySafety<SAFE>::ENABLED) {
			AccessFailureHandler::OP(!ptr);
		}
		return ptr;
	}

#ifdef DUCKDB_CLANG_TIDY
	// This is necessary to tell clang-tidy that it reinitializes the variable after a move
	[[clang::reinitializes]]
#endif
	inline void
	reset(typename original::pointer ptr = typename original::pointer()) noexcept { // NOLINT: hiding on purpose
		original::reset(ptr);
	}
};

// FIXME: DELETER is defined, but we use std::default_delete???
template <class DATA_TYPE, class DELETER, bool SAFE, class AccessFailureHandler>
class unique_ptr<DATA_TYPE[], DELETER, SAFE, AccessFailureHandler>
    : public std::unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE[]>> {
public:
	using original = std::unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE[]>>;
	using original::original;

public:
	typename std::add_lvalue_reference<DATA_TYPE>::type operator[](size_t __i) const { // NOLINT: hiding on purpose
		const auto ptr = original::get();
		if (MemorySafety<SAFE>::ENABLED) {
			AccessFailureHandler::OP(!ptr);
		}
		return ptr[__i];
	}
};

template <typename T>
using unique_array = unique_ptr<T[], std::default_delete<T>, true>;

template <typename T>
using unsafe_unique_array = unique_ptr<T[], std::default_delete<T>, false>;

template <typename T>
using unsafe_unique_ptr = unique_ptr<T, std::default_delete<T>, false>;

} // namespace duckdb
