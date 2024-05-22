//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/shared_ptr.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/memory_safety.hpp"

#include <memory>
#include <type_traits>

namespace duckdb {

// This implementation is taken from the llvm-project, at this commit hash:
// https://github.com/llvm/llvm-project/blob/08bb121835be432ac52372f92845950628ce9a4a/libcxx/include/__memory/shared_ptr.h#353
// originally named '__compatible_with'

#if _LIBCPP_STD_VER >= 17
template <class U, class T>
struct __bounded_convertible_to_unbounded : std::false_type {};

template <class _Up, std::size_t _Np, class T>
struct __bounded_convertible_to_unbounded<_Up[_Np], T> : std::is_same<std::remove_cv<T>, _Up[]> {};

template <class U, class T>
struct compatible_with_t : std::_Or<std::is_convertible<U *, T *>, __bounded_convertible_to_unbounded<U, T>> {};
#else
template <class U, class T>
struct compatible_with_t : std::is_convertible<U *, T *> {}; // NOLINT: invalid case style
#endif // _LIBCPP_STD_VER >= 17

} // namespace duckdb

//===--------------------------------------------------------------------===//
// Shared Pointer
//===--------------------------------------------------------------------===//

namespace duckdb {

template <typename T, bool SAFE = true>
class weak_ptr;

template <class T>
class enable_shared_from_this;

template <typename T, bool SAFE = true>
class shared_ptr { // NOLINT: invalid case style
public:
	using original = std::shared_ptr<T>;
	using element_type = typename original::element_type;
	using weak_type = weak_ptr<T, SAFE>;

private:
	static inline void AssertNotNull(const bool null) {
#if defined(DUCKDB_DEBUG_NO_SAFETY) || defined(DUCKDB_CLANG_TIDY)
		return;
#else
		if (DUCKDB_UNLIKELY(null)) {
			throw duckdb::InternalException("Attempted to dereference shared_ptr that is NULL!");
		}
#endif
	}

private:
	template <class U, bool SAFE_P>
	friend class weak_ptr;

	template <class U, bool SAFE_P>
	friend class shared_ptr;

	template <typename U, typename S>
	friend shared_ptr<S> shared_ptr_cast(shared_ptr<U> src); // NOLINT: invalid case style

private:
	original internal;

public:
	// Constructors
	shared_ptr() : internal() {
	}
	shared_ptr(std::nullptr_t) : internal(nullptr) { // NOLINT: not marked as explicit
	}

	// From raw pointer of type U convertible to T
	template <class U, typename std::enable_if<compatible_with_t<U, T>::value, int>::type = 0>
	explicit shared_ptr(U *ptr) : internal(ptr) {
		__enable_weak_this(internal.get(), internal.get());
	}
	// From raw pointer of type T with custom DELETER
	template <typename DELETER>
	shared_ptr(T *ptr, DELETER deleter) : internal(ptr, deleter) {
		__enable_weak_this(internal.get(), internal.get());
	}
	// Aliasing constructor: shares ownership information with ref but contains ptr instead
	// When the created shared_ptr goes out of scope, it will call the DELETER of ref, will not delete ptr
	template <class U>
	shared_ptr(const shared_ptr<U> &ref, T *ptr) noexcept : internal(ref.internal, ptr) {
	}
#if _LIBCPP_STD_VER >= 20
	template <class U>
	shared_ptr(shared_ptr<U> &&ref, T *ptr) noexcept : internal(std::move(ref.internal), ptr) {
	}
#endif

	// Copy constructor, share ownership with ref
	template <class U, typename std::enable_if<compatible_with_t<U, T>::value, int>::type = 0>
	shared_ptr(const shared_ptr<U> &ref) noexcept : internal(ref.internal) { // NOLINT: not marked as explicit
	}
	shared_ptr(const shared_ptr &other) : internal(other.internal) { // NOLINT: not marked as explicit
	}
	// Move constructor, share ownership with ref
	template <class U, typename std::enable_if<compatible_with_t<U, T>::value, int>::type = 0>
#ifdef DUCKDB_CLANG_TIDY
	[[clang::reinitializes]]
#endif
	shared_ptr(shared_ptr<U> &&ref) noexcept // NOLINT: not marked as explicit
	    : internal(std::move(ref.internal)) {
	}
#ifdef DUCKDB_CLANG_TIDY
	[[clang::reinitializes]]
#endif
	shared_ptr(shared_ptr<T> &&other) // NOLINT: not marked as explicit
	    : internal(std::move(other.internal)) {
	}

	// Construct from std::shared_ptr
	explicit shared_ptr(std::shared_ptr<T> other) : internal(other) {
		// FIXME: should we __enable_weak_this here?
		// *our* enable_shared_from_this hasn't initialized yet, so I think so?
		__enable_weak_this(internal.get(), internal.get());
	}

	// Construct from weak_ptr
	template <class U>
	explicit shared_ptr(weak_ptr<U> other) : internal(other.internal) {
	}

	// Construct from unique_ptr, takes over ownership of the unique_ptr
	template <class U, class DELETER, bool SAFE_P,
	          typename std::enable_if<compatible_with_t<U, T>::value &&
	                                      std::is_convertible<typename unique_ptr<U, DELETER>::pointer, T *>::value,
	                                  int>::type = 0>
#ifdef DUCKDB_CLANG_TIDY
	[[clang::reinitializes]]
#endif
	shared_ptr(unique_ptr<U, DELETER, SAFE_P> &&other) // NOLINT: not marked as explicit
	    : internal(std::move(other)) {
		__enable_weak_this(internal.get(), internal.get());
	}

	// Destructor
	~shared_ptr() = default;

	// Assign from shared_ptr copy
	shared_ptr<T> &operator=(const shared_ptr &other) noexcept {
		if (this == &other) {
			return *this;
		}
		// Create a new shared_ptr using the copy constructor, then swap out the ownership to *this
		shared_ptr(other).swap(*this);
		return *this;
	}
	template <class U, typename std::enable_if<compatible_with_t<U, T>::value, int>::type = 0>
	shared_ptr<T> &operator=(const shared_ptr<U> &other) {
		shared_ptr(other).swap(*this);
		return *this;
	}

	// Assign from moved shared_ptr
	shared_ptr<T> &operator=(shared_ptr &&other) noexcept {
		// Create a new shared_ptr using the move constructor, then swap out the ownership to *this
		shared_ptr(std::move(other)).swap(*this);
		return *this;
	}
	template <class U, typename std::enable_if<compatible_with_t<U, T>::value, int>::type = 0>
	shared_ptr<T> &operator=(shared_ptr<U> &&other) {
		shared_ptr(std::move(other)).swap(*this);
		return *this;
	}

	// Assign from moved unique_ptr
	template <class U, class DELETER, bool SAFE_P,
	          typename std::enable_if<compatible_with_t<U, T>::value &&
	                                      std::is_convertible<typename unique_ptr<U, DELETER>::pointer, T *>::value,
	                                  int>::type = 0>
	shared_ptr<T> &operator=(unique_ptr<U, DELETER, SAFE_P> &&ref) {
		shared_ptr(std::move(ref)).swap(*this);
		return *this;
	}

#ifdef DUCKDB_CLANG_TIDY
	[[clang::reinitializes]]
#endif
	void
	reset() { // NOLINT: invalid case style
		internal.reset();
	}
	template <typename U>
#ifdef DUCKDB_CLANG_TIDY
	[[clang::reinitializes]]
#endif
	void
	reset(U *ptr) { // NOLINT: invalid case style
		internal.reset(ptr);
	}
	template <typename U, typename DELETER>
#ifdef DUCKDB_CLANG_TIDY
	[[clang::reinitializes]]
#endif
	void
	reset(U *ptr, DELETER deleter) { // NOLINT: invalid case style
		internal.reset(ptr, deleter);
	}

	void swap(shared_ptr &r) noexcept { // NOLINT: invalid case style
		internal.swap(r.internal);
	}

	T *get() const { // NOLINT: invalid case style
		return internal.get();
	}

	long use_count() const { // NOLINT: invalid case style
		return internal.use_count();
	}

	explicit operator bool() const noexcept {
		return internal.operator bool();
	}

	typename std::add_lvalue_reference<T>::type operator*() const {
		if (MemorySafety<SAFE>::ENABLED) {
			const auto ptr = internal.get();
			AssertNotNull(!ptr);
			return *ptr;
		} else {
			return *internal;
		}
	}

	T *operator->() const {
		if (MemorySafety<SAFE>::ENABLED) {
			const auto ptr = internal.get();
			AssertNotNull(!ptr);
			return ptr;
		} else {
			return internal.operator->();
		}
	}

	// Relational operators
	template <typename U>
	bool operator==(const shared_ptr<U> &other) const noexcept {
		return internal == other.internal;
	}
	template <typename U>
	bool operator!=(const shared_ptr<U> &other) const noexcept {
		return internal != other.internal;
	}

	bool operator==(std::nullptr_t) const noexcept {
		return internal == nullptr;
	}
	bool operator!=(std::nullptr_t) const noexcept {
		return internal != nullptr;
	}

	template <typename U>
	bool operator<(const shared_ptr<U> &other) const noexcept {
		return internal < other.internal;
	}
	template <typename U>
	bool operator<=(const shared_ptr<U> &other) const noexcept {
		return internal <= other.internal;
	}
	template <typename U>
	bool operator>(const shared_ptr<U> &other) const noexcept {
		return internal > other.internal;
	}
	template <typename U>
	bool operator>=(const shared_ptr<U> &other) const noexcept {
		return internal >= other.internal;
	}

private:
	// This overload is used when the class inherits from 'enable_shared_from_this<U>'
	template <class U, class V,
	          typename std::enable_if<std::is_convertible<V *, const enable_shared_from_this<U> *>::value,
	                                  int>::type = 0>
	void __enable_weak_this(const enable_shared_from_this<U> *object, // NOLINT: invalid case style
	                        V *ptr) noexcept {
		typedef typename std::remove_cv<U>::type non_const_u_t;
		if (object && object->__weak_this_.expired()) {
			// __weak_this__ is the mutable variable returned by 'shared_from_this'
			// it is initialized here
			auto non_const = const_cast<non_const_u_t *>(static_cast<const U *>(ptr)); // NOLINT: const cast
			object->__weak_this_ = shared_ptr<non_const_u_t>(*this, non_const);
		}
	}

	void __enable_weak_this(...) noexcept { // NOLINT: invalid case style
	}
};

} // namespace duckdb

//===--------------------------------------------------------------------===//
// Weak Pointer
//===--------------------------------------------------------------------===//

namespace duckdb {

template <typename T, bool SAFE>
class weak_ptr { // NOLINT: invalid case style
public:
	using original = std::weak_ptr<T>;
	using element_type = typename original::element_type;

private:
	template <class U, bool SAFE_P>
	friend class shared_ptr;

private:
	original internal;

public:
	// Constructors
	weak_ptr() : internal() {
	}

	// NOLINTBEGIN
	template <class U, typename std::enable_if<compatible_with_t<U, T>::value, int>::type = 0>
	weak_ptr(shared_ptr<U, SAFE> const &ptr) noexcept : internal(ptr.internal) {
	}
	weak_ptr(weak_ptr const &other) noexcept : internal(other.internal) {
	}
	template <class U, typename std::enable_if<compatible_with_t<U, T>::value, int>::type = 0>
	weak_ptr(weak_ptr<U> const &ptr) noexcept : internal(ptr.internal) {
	}
#ifdef DUCKDB_CLANG_TIDY
	[[clang::reinitializes]]
#endif
	weak_ptr(weak_ptr &&ptr) noexcept
	    : internal(std::move(ptr.internal)) {
	}
	template <class U, typename std::enable_if<compatible_with_t<U, T>::value, int>::type = 0>
#ifdef DUCKDB_CLANG_TIDY
	[[clang::reinitializes]]
#endif
	weak_ptr(weak_ptr<U> &&ptr) noexcept
	    : internal(std::move(ptr.internal)) {
	}
	// NOLINTEND
	// Destructor
	~weak_ptr() = default;

	// Assignment operators
	weak_ptr &operator=(const weak_ptr &other) {
		if (this == &other) {
			return *this;
		}
		internal = other.internal;
		return *this;
	}

	template <class U, typename std::enable_if<compatible_with_t<U, T>::value, int>::type = 0>
	weak_ptr &operator=(const shared_ptr<U, SAFE> &ptr) {
		internal = ptr.internal;
		return *this;
	}

	// Modifiers
#ifdef DUCKDB_CLANG_TIDY
	// This is necessary to tell clang-tidy that it reinitializes the variable after a move
	[[clang::reinitializes]]
#endif
	void
	reset() { // NOLINT: invalid case style
		internal.reset();
	}

	// Observers
	long use_count() const { // NOLINT: invalid case style
		return internal.use_count();
	}

	bool expired() const { // NOLINT: invalid case style
		return internal.expired();
	}

	shared_ptr<T, SAFE> lock() const { // NOLINT: invalid case style
		return shared_ptr<T, SAFE>(internal.lock());
	}

	// Relational operators
	template <typename U>
	bool operator==(const weak_ptr<U> &other) const noexcept {
		return internal == other.internal;
	}

	template <typename U>
	bool operator!=(const weak_ptr<U> &other) const noexcept {
		return internal != other.internal;
	}

	template <typename U>
	bool operator<(const weak_ptr<U> &other) const noexcept {
		return internal < other.internal;
	}

	template <typename U>
	bool operator<=(const weak_ptr<U> &other) const noexcept {
		return internal <= other.internal;
	}

	template <typename U>
	bool operator>(const weak_ptr<U> &other) const noexcept {
		return internal > other.internal;
	}

	template <typename U>
	bool operator>=(const weak_ptr<U> &other) const noexcept {
		return internal >= other.internal;
	}
};

} // namespace duckdb

//===--------------------------------------------------------------------===//
// Enable Shared From This
//===--------------------------------------------------------------------===//

namespace duckdb {

template <class T>
class enable_shared_from_this { // NOLINT: invalid case style
public:
	template <class U, bool SAFE>
	friend class shared_ptr;

private:
	mutable weak_ptr<T> __weak_this_; // NOLINT: __weak_this_ is reserved

protected:
	constexpr enable_shared_from_this() noexcept {
	}
	enable_shared_from_this(enable_shared_from_this const &) noexcept { // NOLINT: not marked as explicit
	}
	enable_shared_from_this &operator=(enable_shared_from_this const &) noexcept {
		return *this;
	}
	~enable_shared_from_this() {
	}

public:
	shared_ptr<T> shared_from_this() { // NOLINT: invalid case style
		return shared_ptr<T>(__weak_this_);
	}
	shared_ptr<T const> shared_from_this() const { // NOLINT: invalid case style
		return shared_ptr<const T>(__weak_this_);
	}

#if _LIBCPP_STD_VER >= 17
	weak_ptr<T> weak_from_this() noexcept { // NOLINT: invalid case style
		return __weak_this_;
	}

	weak_ptr<const T> weak_from_this() const noexcept { // NOLINT: invalid case style
		return __weak_this_;
	}
#endif // _LIBCPP_STD_VER >= 17
};

} // namespace duckdb

namespace duckdb {

template <typename T>
using unsafe_shared_ptr = shared_ptr<T, false>;

template <typename T>
using unsafe_weak_ptr = weak_ptr<T, false>;

} // namespace duckdb
