#pragma once

#include <type_traits>
#include "duckdb/common/unique_ptr.hpp"
#include <memory>
#include <ostream>

using std::common_type;
using std::false_type;
using std::true_type;

using std::enable_if;

using std::is_array;
using std::is_assignable;
using std::is_constructible;
using std::is_convertible;
using std::is_default_constructible;
using std::is_function;
using std::is_lvalue_reference;
using std::is_pointer;
using std::is_reference;
using std::is_same;
using std::is_void;

using std::add_lvalue_reference;
using std::allocator;
using std::allocator_traits;
using std::basic_ostream;
using std::binary_function;
using std::forward;
using std::integral_constant;
using std::less;
using std::memory_order;
using std::pointer_traits;
using std::reference_wrapper;
using std::remove_reference;
using std::runtime_error;
using std::swap;
using std::type_info;

using std::__compatible_with;
using std::__compressed_pair;
using std::__dependent_type;
using std::__enable_hash_helper;
using std::__is_array_deletable;
using std::__is_deletable;
using std::__is_swappable;
using std::__pointer;
using std::__shared_weak_count;

using std::__allocation_guard;
using std::__allocator_traits_rebind;
using std::__shared_ptr_deleter_ctor_reqs;
using std::__shared_ptr_dummy_rebind_allocator_type;
using std::_And;
using std::_EnableIf;
using std::_If;
using std::remove_cv;

namespace duckdb {

//===--------------------------------------------------------------------===//
// Internals
//===--------------------------------------------------------------------===//

template <class _Alloc>
class __allocator_destructor {
	typedef allocator_traits<_Alloc> __alloc_traits;

public:
	typedef typename __alloc_traits::pointer pointer;
	typedef typename __alloc_traits::size_type size_type;

private:
	_Alloc &__alloc_;
	size_type __s_;

public:
	__allocator_destructor(_Alloc &__a, size_type __s) throw() : __alloc_(__a), __s_(__s) {
	}

	void operator()(pointer __p) throw() {
		__alloc_traits::deallocate(__alloc_, __p, __s_);
	}
};

// NOTE: Relaxed and acq/rel atomics (for increment and decrement respectively)
// should be sufficient for thread safety.
// See https://llvm.org/PR22803
#if defined(__clang__) && __has_builtin(__atomic_add_fetch) && defined(__ATOMIC_RELAXED) && defined(__ATOMIC_ACQ_REL)
#define _LIBCPP_HAS_BUILTIN_ATOMIC_SUPPORT
#elif defined(_LIBCPP_COMPILER_GCC)
#define _LIBCPP_HAS_BUILTIN_ATOMIC_SUPPORT
#endif

template <class _ValueType>
inline _ValueType __libcpp_relaxed_load(_ValueType const *__value) {
#if !defined(_LIBCPP_HAS_NO_THREADS) && defined(__ATOMIC_RELAXED) &&                                                   \
    (__has_builtin(__atomic_load_n) || defined(_LIBCPP_COMPILER_GCC))
	return __atomic_load_n(__value, __ATOMIC_RELAXED);
#else
	return *__value;
#endif
}

template <class _ValueType>
inline _ValueType __libcpp_acquire_load(_ValueType const *__value) {
#if !defined(_LIBCPP_HAS_NO_THREADS) && defined(__ATOMIC_ACQUIRE) &&                                                   \
    (__has_builtin(__atomic_load_n) || defined(_LIBCPP_COMPILER_GCC))
	return __atomic_load_n(__value, __ATOMIC_ACQUIRE);
#else
	return *__value;
#endif
}

template <class _Tp>
inline _Tp __libcpp_atomic_refcount_increment(_Tp &__t) throw() {
#if defined(_LIBCPP_HAS_BUILTIN_ATOMIC_SUPPORT) && !defined(_LIBCPP_HAS_NO_THREADS)
	return __atomic_add_fetch(&__t, 1, __ATOMIC_RELAXED);
#else
	return __t += 1;
#endif
}

template <class _Tp>
inline _Tp __libcpp_atomic_refcount_decrement(_Tp &__t) throw() {
#if defined(_LIBCPP_HAS_BUILTIN_ATOMIC_SUPPORT) && !defined(_LIBCPP_HAS_NO_THREADS)
	return __atomic_add_fetch(&__t, -1, __ATOMIC_ACQ_REL);
#else
	return __t -= 1;
#endif
}

class _LIBCPP_EXCEPTION_ABI bad_weak_ptr : public std::exception {
public:
	bad_weak_ptr() throw() = default;
	bad_weak_ptr(const bad_weak_ptr &) throw() = default;
	virtual ~bad_weak_ptr() throw();
	virtual const char *what() const throw();
};

_LIBCPP_NORETURN inline void __throw_bad_weak_ptr() {
#ifndef _LIBCPP_NO_EXCEPTIONS
	throw bad_weak_ptr();
#else
	std::abort();
#endif
}

class _LIBCPP_TYPE_VIS __shared_count {
	__shared_count(const __shared_count &);
	__shared_count &operator=(const __shared_count &);

protected:
	long __shared_owners_;
	virtual ~__shared_count();

private:
	virtual void __on_zero_shared() throw() = 0;

public:
	explicit __shared_count(long __refs = 0) throw() : __shared_owners_(__refs) {
	}

#if defined(_LIBCPP_BUILDING_LIBRARY) && defined(_LIBCPP_DEPRECATED_ABI_LEGACY_LIBRARY_DEFINITIONS_FOR_INLINE_FUNCTIONS)
	void __add_shared() throw();
	bool __release_shared() throw();
#else

	void __add_shared() throw() {
		__libcpp_atomic_refcount_increment(__shared_owners_);
	}

	bool __release_shared() throw() {
		if (__libcpp_atomic_refcount_decrement(__shared_owners_) == -1) {
			__on_zero_shared();
			return true;
		}
		return false;
	}
#endif

	long use_count() const throw() {
		return __libcpp_relaxed_load(&__shared_owners_) + 1;
	}
};

class _LIBCPP_TYPE_VIS __shared_weak_count : private __shared_count {
	long __shared_weak_owners_;

public:
	explicit __shared_weak_count(long __refs = 0) throw() : __shared_count(__refs), __shared_weak_owners_(__refs) {
	}

protected:
	virtual ~__shared_weak_count();

public:
#if defined(_LIBCPP_BUILDING_LIBRARY) && defined(_LIBCPP_DEPRECATED_ABI_LEGACY_LIBRARY_DEFINITIONS_FOR_INLINE_FUNCTIONS)
	void __add_shared() throw();
	void __add_weak() throw();
	void __release_shared() throw();
#else

	void __add_shared() throw() {
		__shared_count::__add_shared();
	}

	void __add_weak() throw() {
		__libcpp_atomic_refcount_increment(__shared_weak_owners_);
	}

	void __release_shared() throw() {
		if (__shared_count::__release_shared())
			__release_weak();
	}
#endif
	void __release_weak() throw();

	long use_count() const throw() {
		return __shared_count::use_count();
	}
	__shared_weak_count *lock() throw();

	virtual const void *__get_deleter(const type_info &) const throw();

private:
	virtual void __on_zero_shared_weak() throw() = 0;
};

template <class _Tp, class _Dp, class _Alloc>
class __shared_ptr_pointer : public __shared_weak_count {
	__compressed_pair<__compressed_pair<_Tp, _Dp>, _Alloc> __data_;

public:
	__shared_ptr_pointer(_Tp __p, _Dp __d, _Alloc __a)
	    : __data_(__compressed_pair<_Tp, _Dp>(__p, std::move(__d)), std::move(__a)) {
	}

#ifndef _LIBCPP_NO_RTTI
	virtual const void *__get_deleter(const type_info &) const throw();
#endif

private:
	virtual void __on_zero_shared() throw();
	virtual void __on_zero_shared_weak() throw();
};

#ifndef _LIBCPP_NO_RTTI

template <class _Tp, class _Dp, class _Alloc>
const void *__shared_ptr_pointer<_Tp, _Dp, _Alloc>::__get_deleter(const type_info &__t) const throw() {
	return __t == typeid(_Dp) ? std::addressof(__data_.first().second()) : nullptr;
}

#endif // _LIBCPP_NO_RTTI

template <class _Tp, class _Dp, class _Alloc>
void __shared_ptr_pointer<_Tp, _Dp, _Alloc>::__on_zero_shared() throw() {
	__data_.first().second()(__data_.first().first());
	__data_.first().second().~_Dp();
}

template <class _Tp, class _Dp, class _Alloc>
void __shared_ptr_pointer<_Tp, _Dp, _Alloc>::__on_zero_shared_weak() throw() {
	typedef typename __allocator_traits_rebind<_Alloc, __shared_ptr_pointer>::type _Al;
	typedef allocator_traits<_Al> _ATraits;
	typedef pointer_traits<typename _ATraits::pointer> _PTraits;

	_Al __a(__data_.second());
	__data_.second().~_Alloc();
	__a.deallocate(_PTraits::pointer_to(*this), 1);
}

template <class _Tp, class _Alloc>
struct __shared_ptr_emplace : __shared_weak_count {
	template <class... _Args>
	explicit __shared_ptr_emplace(_Alloc __a, _Args &&...__args) : __storage_(std::move(__a)) {
#if _LIBCPP_STD_VER > 17
		using _TpAlloc = typename __allocator_traits_rebind<_Alloc, _Tp>::type;
		_TpAlloc __tmp(*__get_alloc());
		allocator_traits<_TpAlloc>::construct(__tmp, __get_elem(), std::forward<_Args>(__args)...);
#else
		::new ((void *)__get_elem()) _Tp(std::forward<_Args>(__args)...);
#endif
	}

	_Alloc *__get_alloc() throw() {
		return __storage_.__get_alloc();
	}

	_Tp *__get_elem() throw() {
		return __storage_.__get_elem();
	}

private:
	virtual void __on_zero_shared() throw() {
#if _LIBCPP_STD_VER > 17
		using _TpAlloc = typename __allocator_traits_rebind<_Alloc, _Tp>::type;
		_TpAlloc __tmp(*__get_alloc());
		allocator_traits<_TpAlloc>::destroy(__tmp, __get_elem());
#else
		__get_elem()->~_Tp();
#endif
	}

	virtual void __on_zero_shared_weak() throw() {
		using _ControlBlockAlloc = typename __allocator_traits_rebind<_Alloc, __shared_ptr_emplace>::type;
		using _ControlBlockPointer = typename allocator_traits<_ControlBlockAlloc>::pointer;
		_ControlBlockAlloc __tmp(*__get_alloc());
		__storage_.~_Storage();
		allocator_traits<_ControlBlockAlloc>::deallocate(__tmp, pointer_traits<_ControlBlockPointer>::pointer_to(*this),
		                                                 1);
	}

	// This class implements the control block for non-array shared pointers created
	// through `std::allocate_shared` and `std::make_shared`.
	//
	// In previous versions of the library, we used a compressed pair to store
	// both the _Alloc and the _Tp. This implies using EBO, which is incompatible
	// with Allocator construction for _Tp. To allow implementing P0674 in C++20,
	// we now use a properly aligned char buffer while making sure that we maintain
	// the same layout that we had when we used a compressed pair.
	using _CompressedPair = __compressed_pair<_Alloc, _Tp>;
	struct _ALIGNAS_TYPE(_CompressedPair) _Storage {
		char __blob_[sizeof(_CompressedPair)];

		explicit _Storage(_Alloc &&__a) {
			::new ((void *)__get_alloc()) _Alloc(std::move(__a));
		}
		~_Storage() {
			__get_alloc()->~_Alloc();
		}
		_Alloc *__get_alloc() throw() {
			_CompressedPair *__as_pair = reinterpret_cast<_CompressedPair *>(__blob_);
			typename _CompressedPair::_Base1 *__first = _CompressedPair::__get_first_base(__as_pair);
			_Alloc *__alloc = reinterpret_cast<_Alloc *>(__first);
			return __alloc;
		}
		_LIBCPP_NO_CFI _Tp *__get_elem() throw() {
			_CompressedPair *__as_pair = reinterpret_cast<_CompressedPair *>(__blob_);
			typename _CompressedPair::_Base2 *__second = _CompressedPair::__get_second_base(__as_pair);
			_Tp *__elem = reinterpret_cast<_Tp *>(__second);
			return __elem;
		}
	};

	static_assert(_LIBCPP_ALIGNOF(_Storage) == _LIBCPP_ALIGNOF(_CompressedPair), "");
	static_assert(sizeof(_Storage) == sizeof(_CompressedPair), "");
	_Storage __storage_;
};

} // namespace duckdb

namespace duckdb {

//===--------------------------------------------------------------------===//
// Forward declarations
//===--------------------------------------------------------------------===//

template <class _Tp>
class weak_ptr;
template <class _Tp>
class enable_shared_from_this;

//===--------------------------------------------------------------------===//
// Shared pointer declaration
//===--------------------------------------------------------------------===//

template <class _Tp>
class shared_ptr {
public:
#if _LIBCPP_STD_VER > 14
	typedef weak_ptr<_Tp> weak_type;
	typedef remove_extent_t<_Tp> element_type;
#else
	typedef _Tp element_type;
#endif

private:
	element_type *__ptr_;
	__shared_weak_count *__cntrl_;

	struct __nat {
		int __for_bool_;
	};

public:
	inline constexpr shared_ptr() throw();
	inline constexpr shared_ptr(nullptr_t) throw();

	template <class _Yp,
	          class = _EnableIf<_And<__compatible_with<_Yp, _Tp>
	// In C++03 we get errors when trying to do SFINAE with the
	// delete operator, so we always pretend that it's deletable.
	// The same happens on GCC.
#if !defined(_LIBCPP_CXX03_LANG) && !defined(_LIBCPP_COMPILER_GCC)
	                                 ,
	                                 _If<is_array<_Tp>::value, __is_array_deletable<_Yp *>, __is_deletable<_Yp *>>
#endif
	                                 >::value>>
	explicit shared_ptr(_Yp *__p) : __ptr_(__p) {
		unique_ptr<_Yp> __hold(__p);
		typedef typename __shared_ptr_default_allocator<_Yp>::type _AllocT;
		typedef __shared_ptr_pointer<_Yp *, __shared_ptr_default_delete<_Tp, _Yp>, _AllocT> _CntrlBlk;
		__cntrl_ = new _CntrlBlk(__p, __shared_ptr_default_delete<_Tp, _Yp>(), _AllocT());
		__hold.release();
		__enable_weak_this(__p, __p);
	}

	template <class _Yp, class _Dp>
	shared_ptr(
	    _Yp *__p, _Dp __d,
	    typename enable_if<__shared_ptr_deleter_ctor_reqs<_Dp, _Yp, element_type>::value, __nat>::type = __nat());
	template <class _Yp, class _Dp, class _Alloc>
	shared_ptr(
	    _Yp *__p, _Dp __d, _Alloc __a,
	    typename enable_if<__shared_ptr_deleter_ctor_reqs<_Dp, _Yp, element_type>::value, __nat>::type = __nat());
	template <class _Dp>
	shared_ptr(nullptr_t __p, _Dp __d);
	template <class _Dp, class _Alloc>
	shared_ptr(nullptr_t __p, _Dp __d, _Alloc __a);
	template <class _Yp>
	inline shared_ptr(const shared_ptr<_Yp> &__r, element_type *__p) throw();
	inline shared_ptr(const shared_ptr &__r) throw();
	template <class _Yp>
	inline shared_ptr(const shared_ptr<_Yp> &__r,
	                  typename enable_if<__compatible_with<_Yp, element_type>::value, __nat>::type = __nat()) throw();
	inline shared_ptr(shared_ptr &&__r) throw();
	template <class _Yp>
	inline shared_ptr(shared_ptr<_Yp> &&__r,
	                  typename enable_if<__compatible_with<_Yp, element_type>::value, __nat>::type = __nat()) throw();
	template <class _Yp>
	explicit shared_ptr(const weak_ptr<_Yp> &__r,
	                    typename enable_if<is_convertible<_Yp *, element_type *>::value, __nat>::type = __nat());
	template <class _Yp, class _Dp>
	shared_ptr(unique_ptr<_Yp, _Dp> &&,
	           typename enable_if<!is_lvalue_reference<_Dp>::value &&
	                                  is_convertible<typename unique_ptr<_Yp, _Dp>::pointer, element_type *>::value,
	                              __nat>::type = __nat());
	template <class _Yp, class _Dp>
	shared_ptr(unique_ptr<_Yp, _Dp> &&,
	           typename enable_if<is_lvalue_reference<_Dp>::value &&
	                                  is_convertible<typename unique_ptr<_Yp, _Dp>::pointer, element_type *>::value,
	                              __nat>::type = __nat());

	~shared_ptr();

	inline shared_ptr &operator=(const shared_ptr &__r) throw();
	template <class _Yp>
	typename enable_if<__compatible_with<_Yp, element_type>::value, shared_ptr &>::type inline
	operator=(const shared_ptr<_Yp> &__r) throw();
	inline shared_ptr &operator=(shared_ptr &&__r) throw();
	template <class _Yp>
	typename enable_if<__compatible_with<_Yp, element_type>::value, shared_ptr &>::type inline
	operator=(shared_ptr<_Yp> &&__r);
	template <class _Yp, class _Dp>
	typename enable_if<is_convertible<typename unique_ptr<_Yp, _Dp>::pointer, element_type *>::value,
	                   shared_ptr &>::type inline
	operator=(unique_ptr<_Yp, _Dp> &&__r);

	inline void swap(shared_ptr &__r) throw();
	inline void reset() throw();
	template <class _Yp>
	typename enable_if<__compatible_with<_Yp, element_type>::value, void>::type inline reset(_Yp *__p);
	template <class _Yp, class _Dp>
	typename enable_if<__compatible_with<_Yp, element_type>::value, void>::type inline reset(_Yp *__p, _Dp __d);
	template <class _Yp, class _Dp, class _Alloc>
	typename enable_if<__compatible_with<_Yp, element_type>::value, void>::type inline reset(_Yp *__p, _Dp __d,
	                                                                                         _Alloc __a);

	inline element_type *get() const throw() {
		return __ptr_;
	}
	inline typename add_lvalue_reference<element_type>::type operator*() const throw() {
		return *__ptr_;
	}
	inline element_type *operator->() const throw() {
		static_assert(!is_array<_Tp>::value, "shared_ptr<T>::operator-> is only valid when T is not an array type.");
		return __ptr_;
	}
	inline long use_count() const throw() {
		return __cntrl_ ? __cntrl_->use_count() : 0;
	}
	inline bool unique() const throw() {
		return use_count() == 1;
	}
	inline explicit operator bool() const throw() {
		return get() != nullptr;
	}
	template <class _Up>
	inline bool owner_before(shared_ptr<_Up> const &__p) const throw() {
		return __cntrl_ < __p.__cntrl_;
	}
	template <class _Up>
	inline bool owner_before(weak_ptr<_Up> const &__p) const throw() {
		return __cntrl_ < __p.__cntrl_;
	}
	inline bool __owner_equivalent(const shared_ptr &__p) const {
		return __cntrl_ == __p.__cntrl_;
	}

#if _LIBCPP_STD_VER > 14
	typename add_lvalue_reference<element_type>::type inline operator[](ptrdiff_t __i) const {
		static_assert(is_array<_Tp>::value, "shared_ptr<T>::operator[] is only valid when T is an array type.");
		return __ptr_[__i];
	}
#endif

#ifndef _LIBCPP_NO_RTTI
	template <class _Dp>
	inline _Dp *__get_deleter() const throw() {
		return static_cast<_Dp *>(__cntrl_ ? const_cast<void *>(__cntrl_->__get_deleter(typeid(_Dp))) : nullptr);
	}
#endif // _LIBCPP_NO_RTTI

	template <class _Yp, class _CntrlBlk>
	static shared_ptr<_Tp> __create_with_control_block(_Yp *__p, _CntrlBlk *__cntrl) throw() {
		shared_ptr<_Tp> __r;
		__r.__ptr_ = __p;
		__r.__cntrl_ = __cntrl;
		__r.__enable_weak_this(__r.__ptr_, __r.__ptr_);
		return __r;
	}

private:
	template <class _Yp, bool = is_function<_Yp>::value>
	struct __shared_ptr_default_allocator {
		typedef allocator<_Yp> type;
	};

	template <class _Yp>
	struct __shared_ptr_default_allocator<_Yp, true> {
		typedef allocator<__shared_ptr_dummy_rebind_allocator_type> type;
	};

	template <class _Yp, class _OrigPtr>
	inline typename enable_if<is_convertible<_OrigPtr *, const enable_shared_from_this<_Yp> *>::value, void>::type
	__enable_weak_this(const enable_shared_from_this<_Yp> *__e, _OrigPtr *__ptr) throw() {
		typedef typename remove_cv<_Yp>::type _RawYp;
		if (__e && __e->__weak_this_.expired()) {
			__e->__weak_this_ = shared_ptr<_RawYp>(*this, const_cast<_RawYp *>(static_cast<const _Yp *>(__ptr)));
		}
	}

	inline void __enable_weak_this(...) throw() {
	}

	template <class, class _Yp>
	struct __shared_ptr_default_delete : default_delete<_Yp> {};

	template <class _Yp, class _Un, size_t _Sz>
	struct __shared_ptr_default_delete<_Yp[_Sz], _Un> : default_delete<_Yp[]> {};

	template <class _Yp, class _Un>
	struct __shared_ptr_default_delete<_Yp[], _Un> : default_delete<_Yp[]> {};

	template <class _Up>
	friend class shared_ptr;
	template <class _Up>
	friend class weak_ptr;
};

//===--------------------------------------------------------------------===//
// Shared pointer implementation
//===--------------------------------------------------------------------===//

#ifndef _LIBCPP_HAS_NO_DEDUCTION_GUIDES
template <class _Tp>
shared_ptr(weak_ptr<_Tp>) -> shared_ptr<_Tp>;
template <class _Tp, class _Dp>
shared_ptr(unique_ptr<_Tp, _Dp>) -> shared_ptr<_Tp>;
#endif

template <class _Tp>
inline _LIBCPP_CONSTEXPR shared_ptr<_Tp>::shared_ptr() throw() : __ptr_(nullptr), __cntrl_(nullptr) {
}

template <class _Tp>
inline _LIBCPP_CONSTEXPR shared_ptr<_Tp>::shared_ptr(nullptr_t) throw() : __ptr_(nullptr), __cntrl_(nullptr) {
}

template <class _Tp>
template <class _Yp, class _Dp>
shared_ptr<_Tp>::shared_ptr(
    _Yp *__p, _Dp __d, typename enable_if<__shared_ptr_deleter_ctor_reqs<_Dp, _Yp, element_type>::value, __nat>::type)
    : __ptr_(__p) {
#ifndef _LIBCPP_NO_EXCEPTIONS
	try {
#endif // _LIBCPP_NO_EXCEPTIONS
		typedef typename __shared_ptr_default_allocator<_Yp>::type _AllocT;
		typedef __shared_ptr_pointer<_Yp *, _Dp, _AllocT> _CntrlBlk;
#ifndef _LIBCPP_CXX03_LANG
		__cntrl_ = new _CntrlBlk(__p, std::move(__d), _AllocT());
#else
	__cntrl_ = new _CntrlBlk(__p, __d, _AllocT());
#endif // not _LIBCPP_CXX03_LANG
		__enable_weak_this(__p, __p);
#ifndef _LIBCPP_NO_EXCEPTIONS
	} catch (...) {
		__d(__p);
		throw;
	}
#endif // _LIBCPP_NO_EXCEPTIONS
}

template <class _Tp>
template <class _Dp>
shared_ptr<_Tp>::shared_ptr(nullptr_t __p, _Dp __d) : __ptr_(nullptr) {
#ifndef _LIBCPP_NO_EXCEPTIONS
	try {
#endif // _LIBCPP_NO_EXCEPTIONS
		typedef typename __shared_ptr_default_allocator<_Tp>::type _AllocT;
		typedef __shared_ptr_pointer<nullptr_t, _Dp, _AllocT> _CntrlBlk;
#ifndef _LIBCPP_CXX03_LANG
		__cntrl_ = new _CntrlBlk(__p, std::move(__d), _AllocT());
#else
	__cntrl_ = new _CntrlBlk(__p, __d, _AllocT());
#endif // not _LIBCPP_CXX03_LANG
#ifndef _LIBCPP_NO_EXCEPTIONS
	} catch (...) {
		__d(__p);
		throw;
	}
#endif // _LIBCPP_NO_EXCEPTIONS
}

template <class _Tp>
template <class _Yp, class _Dp, class _Alloc>
shared_ptr<_Tp>::shared_ptr(
    _Yp *__p, _Dp __d, _Alloc __a,
    typename enable_if<__shared_ptr_deleter_ctor_reqs<_Dp, _Yp, element_type>::value, __nat>::type)
    : __ptr_(__p) {
#ifndef _LIBCPP_NO_EXCEPTIONS
	try {
#endif // _LIBCPP_NO_EXCEPTIONS
		typedef __shared_ptr_pointer<_Yp *, _Dp, _Alloc> _CntrlBlk;
		typedef typename __allocator_traits_rebind<_Alloc, _CntrlBlk>::type _A2;
		typedef __allocator_destructor<_A2> _D2;
		_A2 __a2(__a);
		unique_ptr<_CntrlBlk, _D2> __hold2(__a2.allocate(1), _D2(__a2, 1));
		::new ((void *)std::addressof(*__hold2.get()))
#ifndef _LIBCPP_CXX03_LANG
		    _CntrlBlk(__p, std::move(__d), __a);
#else
	    _CntrlBlk(__p, __d, __a);
#endif // not _LIBCPP_CXX03_LANG
		__cntrl_ = std::addressof(*__hold2.release());
		__enable_weak_this(__p, __p);
#ifndef _LIBCPP_NO_EXCEPTIONS
	} catch (...) {
		__d(__p);
		throw;
	}
#endif // _LIBCPP_NO_EXCEPTIONS
}

template <class _Tp>
template <class _Dp, class _Alloc>
shared_ptr<_Tp>::shared_ptr(nullptr_t __p, _Dp __d, _Alloc __a) : __ptr_(nullptr) {
#ifndef _LIBCPP_NO_EXCEPTIONS
	try {
#endif // _LIBCPP_NO_EXCEPTIONS
		typedef __shared_ptr_pointer<nullptr_t, _Dp, _Alloc> _CntrlBlk;
		typedef typename __allocator_traits_rebind<_Alloc, _CntrlBlk>::type _A2;
		typedef __allocator_destructor<_A2> _D2;
		_A2 __a2(__a);
		unique_ptr<_CntrlBlk, _D2> __hold2(__a2.allocate(1), _D2(__a2, 1));
		::new ((void *)std::addressof(*__hold2.get()))
#ifndef _LIBCPP_CXX03_LANG
		    _CntrlBlk(__p, std::move(__d), __a);
#else
	    _CntrlBlk(__p, __d, __a);
#endif // not _LIBCPP_CXX03_LANG
		__cntrl_ = std::addressof(*__hold2.release());
#ifndef _LIBCPP_NO_EXCEPTIONS
	} catch (...) {
		__d(__p);
		throw;
	}
#endif // _LIBCPP_NO_EXCEPTIONS
}

template <class _Tp>
template <class _Yp>
inline shared_ptr<_Tp>::shared_ptr(const shared_ptr<_Yp> &__r, element_type *__p) throw()
    : __ptr_(__p), __cntrl_(__r.__cntrl_) {
	if (__cntrl_)
		__cntrl_->__add_shared();
}

template <class _Tp>
inline shared_ptr<_Tp>::shared_ptr(const shared_ptr &__r) throw() : __ptr_(__r.__ptr_), __cntrl_(__r.__cntrl_) {
	if (__cntrl_)
		__cntrl_->__add_shared();
}

template <class _Tp>
template <class _Yp>
inline shared_ptr<_Tp>::shared_ptr(const shared_ptr<_Yp> &__r,
                                   typename enable_if<__compatible_with<_Yp, element_type>::value, __nat>::type) throw()
    : __ptr_(__r.__ptr_), __cntrl_(__r.__cntrl_) {
	if (__cntrl_)
		__cntrl_->__add_shared();
}

template <class _Tp>
inline shared_ptr<_Tp>::shared_ptr(shared_ptr &&__r) throw() : __ptr_(__r.__ptr_), __cntrl_(__r.__cntrl_) {
	__r.__ptr_ = nullptr;
	__r.__cntrl_ = nullptr;
}

template <class _Tp>
template <class _Yp>
inline shared_ptr<_Tp>::shared_ptr(shared_ptr<_Yp> &&__r,
                                   typename enable_if<__compatible_with<_Yp, element_type>::value, __nat>::type) throw()
    : __ptr_(__r.__ptr_), __cntrl_(__r.__cntrl_) {
	__r.__ptr_ = nullptr;
	__r.__cntrl_ = nullptr;
}

template <class _Tp>
template <class _Yp, class _Dp>
shared_ptr<_Tp>::shared_ptr(
    unique_ptr<_Yp, _Dp> &&__r,
    typename enable_if<!is_lvalue_reference<_Dp>::value &&
                           is_convertible<typename unique_ptr<_Yp, _Dp>::pointer, element_type *>::value,
                       __nat>::type)
    : __ptr_(__r.get()) {
#if _LIBCPP_STD_VER > 11
	if (__ptr_ == nullptr)
		__cntrl_ = nullptr;
	else
#endif
	{
		typedef typename __shared_ptr_default_allocator<_Yp>::type _AllocT;
		typedef __shared_ptr_pointer<typename unique_ptr<_Yp, _Dp>::pointer, _Dp, _AllocT> _CntrlBlk;
		__cntrl_ = new _CntrlBlk(__r.get(), __r.get_deleter(), _AllocT());
		__enable_weak_this(__r.get(), __r.get());
	}
	__r.release();
}

template <class _Tp>
template <class _Yp, class _Dp>
shared_ptr<_Tp>::shared_ptr(
    unique_ptr<_Yp, _Dp> &&__r,
    typename enable_if<is_lvalue_reference<_Dp>::value &&
                           is_convertible<typename unique_ptr<_Yp, _Dp>::pointer, element_type *>::value,
                       __nat>::type)
    : __ptr_(__r.get()) {
#if _LIBCPP_STD_VER > 11
	if (__ptr_ == nullptr)
		__cntrl_ = nullptr;
	else
#endif
	{
		typedef typename __shared_ptr_default_allocator<_Yp>::type _AllocT;
		typedef __shared_ptr_pointer<typename unique_ptr<_Yp, _Dp>::pointer,
		                             reference_wrapper<typename remove_reference<_Dp>::type>, _AllocT>
		    _CntrlBlk;
		__cntrl_ = new _CntrlBlk(__r.get(), std::ref(__r.get_deleter()), _AllocT());
		__enable_weak_this(__r.get(), __r.get());
	}
	__r.release();
}

template <class _Tp>
shared_ptr<_Tp>::~shared_ptr() {
	if (__cntrl_)
		__cntrl_->__release_shared();
}

template <class _Tp>
inline shared_ptr<_Tp> &shared_ptr<_Tp>::operator=(const shared_ptr &__r) throw() {
	shared_ptr(__r).swap(*this);
	return *this;
}

template <class _Tp>
template <class _Yp>
inline
    typename enable_if<__compatible_with<_Yp, typename shared_ptr<_Tp>::element_type>::value, shared_ptr<_Tp> &>::type
    shared_ptr<_Tp>::operator=(const shared_ptr<_Yp> &__r) throw() {
	shared_ptr(__r).swap(*this);
	return *this;
}

template <class _Tp>
inline shared_ptr<_Tp> &shared_ptr<_Tp>::operator=(shared_ptr &&__r) throw() {
	shared_ptr(std::move(__r)).swap(*this);
	return *this;
}

template <class _Tp>
template <class _Yp>
inline
    typename enable_if<__compatible_with<_Yp, typename shared_ptr<_Tp>::element_type>::value, shared_ptr<_Tp> &>::type
    shared_ptr<_Tp>::operator=(shared_ptr<_Yp> &&__r) {
	shared_ptr(std::move(__r)).swap(*this);
	return *this;
}

template <class _Tp>
template <class _Yp, class _Dp>
inline typename enable_if<
    is_convertible<typename unique_ptr<_Yp, _Dp>::pointer, typename shared_ptr<_Tp>::element_type *>::value,
    shared_ptr<_Tp> &>::type
shared_ptr<_Tp>::operator=(unique_ptr<_Yp, _Dp> &&__r) {
	shared_ptr(std::move(__r)).swap(*this);
	return *this;
}

template <class _Tp>
inline void shared_ptr<_Tp>::swap(shared_ptr &__r) throw() {
	std::swap(__ptr_, __r.__ptr_);
	std::swap(__cntrl_, __r.__cntrl_);
}

template <class _Tp>
inline void shared_ptr<_Tp>::reset() throw() {
	shared_ptr().swap(*this);
}

template <class _Tp>
template <class _Yp>
inline typename enable_if<__compatible_with<_Yp, typename shared_ptr<_Tp>::element_type>::value, void>::type
shared_ptr<_Tp>::reset(_Yp *__p) {
	shared_ptr(__p).swap(*this);
}

template <class _Tp>
template <class _Yp, class _Dp>
inline typename enable_if<__compatible_with<_Yp, typename shared_ptr<_Tp>::element_type>::value, void>::type
shared_ptr<_Tp>::reset(_Yp *__p, _Dp __d) {
	shared_ptr(__p, __d).swap(*this);
}

template <class _Tp>
template <class _Yp, class _Dp, class _Alloc>
inline typename enable_if<__compatible_with<_Yp, typename shared_ptr<_Tp>::element_type>::value, void>::type
shared_ptr<_Tp>::reset(_Yp *__p, _Dp __d, _Alloc __a) {
	shared_ptr(__p, __d, __a).swap(*this);
}

//===--------------------------------------------------------------------===//
// Make Shared
//===--------------------------------------------------------------------===//

template <class _Tp, class _Alloc, class... _Args, class = _EnableIf<!is_array<_Tp>::value>>
shared_ptr<_Tp> allocate_shared(const _Alloc &__a, _Args &&...__args) {
	using _ControlBlock = __shared_ptr_emplace<_Tp, _Alloc>;
	using _ControlBlockAllocator = typename __allocator_traits_rebind<_Alloc, _ControlBlock>::type;
	__allocation_guard<_ControlBlockAllocator> __guard(__a, 1);
	::new ((void *)std::addressof(*__guard.__get())) _ControlBlock(__a, std::forward<_Args>(__args)...);
	auto __control_block = __guard.__release_ptr();
	return shared_ptr<_Tp>::__create_with_control_block((*__control_block).__get_elem(),
	                                                    std::addressof(*__control_block));
}

template <class _Tp, class... _Args, class = _EnableIf<!is_array<_Tp>::value>>
shared_ptr<_Tp> make_shared(_Args &&...__args) {
	return duckdb::allocate_shared<_Tp>(allocator<_Tp>(), std::forward<_Args>(__args)...);
}

//===--------------------------------------------------------------------===//
// Shared pointer comparison operators
//===--------------------------------------------------------------------===//

template <class _Tp, class _Up>
inline bool operator==(const shared_ptr<_Tp> &__x, const shared_ptr<_Up> &__y) throw() {
	return __x.get() == __y.get();
}

template <class _Tp, class _Up>
inline bool operator!=(const shared_ptr<_Tp> &__x, const shared_ptr<_Up> &__y) throw() {
	return !(__x == __y);
}

template <class _Tp, class _Up>
inline bool operator<(const shared_ptr<_Tp> &__x, const shared_ptr<_Up> &__y) throw() {
#if _LIBCPP_STD_VER <= 11
	typedef typename common_type<_Tp *, _Up *>::type _Vp;
	return less<_Vp>()(__x.get(), __y.get());
#else
	return less<>()(__x.get(), __y.get());
#endif
}

template <class _Tp, class _Up>
inline bool operator>(const shared_ptr<_Tp> &__x, const shared_ptr<_Up> &__y) throw() {
	return __y < __x;
}

template <class _Tp, class _Up>
inline bool operator<=(const shared_ptr<_Tp> &__x, const shared_ptr<_Up> &__y) throw() {
	return !(__y < __x);
}

template <class _Tp, class _Up>
inline bool operator>=(const shared_ptr<_Tp> &__x, const shared_ptr<_Up> &__y) throw() {
	return !(__x < __y);
}

template <class _Tp>
inline bool operator==(const shared_ptr<_Tp> &__x, nullptr_t) throw() {
	return !__x;
}

template <class _Tp>
inline bool operator==(nullptr_t, const shared_ptr<_Tp> &__x) throw() {
	return !__x;
}

template <class _Tp>
inline bool operator!=(const shared_ptr<_Tp> &__x, nullptr_t) throw() {
	return static_cast<bool>(__x);
}

template <class _Tp>
inline bool operator!=(nullptr_t, const shared_ptr<_Tp> &__x) throw() {
	return static_cast<bool>(__x);
}

template <class _Tp>
inline bool operator<(const shared_ptr<_Tp> &__x, nullptr_t) throw() {
	return less<_Tp *>()(__x.get(), nullptr);
}

template <class _Tp>
inline bool operator<(nullptr_t, const shared_ptr<_Tp> &__x) throw() {
	return less<_Tp *>()(nullptr, __x.get());
}

template <class _Tp>
inline bool operator>(const shared_ptr<_Tp> &__x, nullptr_t) throw() {
	return nullptr < __x;
}

template <class _Tp>
inline bool operator>(nullptr_t, const shared_ptr<_Tp> &__x) throw() {
	return __x < nullptr;
}

template <class _Tp>
inline bool operator<=(const shared_ptr<_Tp> &__x, nullptr_t) throw() {
	return !(nullptr < __x);
}

template <class _Tp>
inline bool operator<=(nullptr_t, const shared_ptr<_Tp> &__x) throw() {
	return !(__x < nullptr);
}

template <class _Tp>
inline bool operator>=(const shared_ptr<_Tp> &__x, nullptr_t) throw() {
	return !(__x < nullptr);
}

template <class _Tp>
inline bool operator>=(nullptr_t, const shared_ptr<_Tp> &__x) throw() {
	return !(nullptr < __x);
}

template <class _Tp>
inline void swap(shared_ptr<_Tp> &__x, shared_ptr<_Tp> &__y) throw() {
	__x.swap(__y);
}

template <class _Tp, class _Up>
inline shared_ptr<_Tp> static_pointer_cast(const shared_ptr<_Up> &__r) throw() {
	return shared_ptr<_Tp>(__r, static_cast<typename shared_ptr<_Tp>::element_type *>(__r.get()));
}

template <class _Tp, class _Up>
inline shared_ptr<_Tp> dynamic_pointer_cast(const shared_ptr<_Up> &__r) throw() {
	typedef typename shared_ptr<_Tp>::element_type _ET;
	_ET *__p = dynamic_cast<_ET *>(__r.get());
	return __p ? shared_ptr<_Tp>(__r, __p) : shared_ptr<_Tp>();
}

template <class _Tp, class _Up>
shared_ptr<_Tp> const_pointer_cast(const shared_ptr<_Up> &__r) throw() {
	typedef typename shared_ptr<_Tp>::element_type _RTp;
	return shared_ptr<_Tp>(__r, const_cast<_RTp *>(__r.get()));
}

template <class _Tp, class _Up>
shared_ptr<_Tp> reinterpret_pointer_cast(const shared_ptr<_Up> &__r) throw() {
	return shared_ptr<_Tp>(__r, reinterpret_cast<typename shared_ptr<_Tp>::element_type *>(__r.get()));
}

#ifndef _LIBCPP_NO_RTTI

template <class _Dp, class _Tp>
inline _Dp *get_deleter(const shared_ptr<_Tp> &__p) throw() {
	return __p.template __get_deleter<_Dp>();
}

#endif // _LIBCPP_NO_RTTI

//===--------------------------------------------------------------------===//
// Weak pointer
//===--------------------------------------------------------------------===//

template <class _Tp>
class weak_ptr {
public:
	typedef _Tp element_type;

private:
	element_type *__ptr_;
	__shared_weak_count *__cntrl_;

	struct __nat {
		int __for_bool_;
	};

public:
	inline _LIBCPP_CONSTEXPR weak_ptr() throw();
	template <class _Yp>
	inline weak_ptr(shared_ptr<_Yp> const &__r,
	                typename enable_if<is_convertible<_Yp *, _Tp *>::value, __nat *>::type = 0) throw();
	inline weak_ptr(weak_ptr const &__r) throw();
	template <class _Yp>
	inline weak_ptr(weak_ptr<_Yp> const &__r,
	                typename enable_if<is_convertible<_Yp *, _Tp *>::value, __nat *>::type = 0) throw();

	inline weak_ptr(weak_ptr &&__r) throw();
	template <class _Yp>
	inline weak_ptr(weak_ptr<_Yp> &&__r,
	                typename enable_if<is_convertible<_Yp *, _Tp *>::value, __nat *>::type = 0) throw();
	~weak_ptr();

	inline weak_ptr &operator=(weak_ptr const &__r) throw();
	template <class _Yp>
	typename enable_if<is_convertible<_Yp *, element_type *>::value, weak_ptr &>::type inline
	operator=(weak_ptr<_Yp> const &__r) throw();

	inline weak_ptr &operator=(weak_ptr &&__r) throw();
	template <class _Yp>
	typename enable_if<is_convertible<_Yp *, element_type *>::value, weak_ptr &>::type inline
	operator=(weak_ptr<_Yp> &&__r) throw();

	template <class _Yp>
	typename enable_if<is_convertible<_Yp *, element_type *>::value, weak_ptr &>::type inline
	operator=(shared_ptr<_Yp> const &__r) throw();

	inline void swap(weak_ptr &__r) throw();
	inline void reset() throw();

	inline long use_count() const throw() {
		return __cntrl_ ? __cntrl_->use_count() : 0;
	}
	inline bool expired() const throw() {
		return __cntrl_ == nullptr || __cntrl_->use_count() == 0;
	}
	shared_ptr<_Tp> lock() const throw();
	template <class _Up>
	inline bool owner_before(const shared_ptr<_Up> &__r) const throw() {
		return __cntrl_ < __r.__cntrl_;
	}
	template <class _Up>
	inline bool owner_before(const weak_ptr<_Up> &__r) const throw() {
		return __cntrl_ < __r.__cntrl_;
	}

	template <class _Up>
	friend class weak_ptr;
	template <class _Up>
	friend class shared_ptr;
};

//===--------------------------------------------------------------------===//
// Weak pointer implementation
//===--------------------------------------------------------------------===//

template <class _Tp>
inline _LIBCPP_CONSTEXPR weak_ptr<_Tp>::weak_ptr() throw() : __ptr_(nullptr), __cntrl_(nullptr) {
}

template <class _Tp>
inline weak_ptr<_Tp>::weak_ptr(weak_ptr const &__r) throw() : __ptr_(__r.__ptr_), __cntrl_(__r.__cntrl_) {
	if (__cntrl_)
		__cntrl_->__add_weak();
}

template <class _Tp>
template <class _Yp>
inline weak_ptr<_Tp>::weak_ptr(shared_ptr<_Yp> const &__r,
                               typename enable_if<is_convertible<_Yp *, _Tp *>::value, __nat *>::type) throw()
    : __ptr_(__r.__ptr_), __cntrl_(__r.__cntrl_) {
	if (__cntrl_)
		__cntrl_->__add_weak();
}

template <class _Tp>
template <class _Yp>
inline weak_ptr<_Tp>::weak_ptr(weak_ptr<_Yp> const &__r,
                               typename enable_if<is_convertible<_Yp *, _Tp *>::value, __nat *>::type) throw()
    : __ptr_(__r.__ptr_), __cntrl_(__r.__cntrl_) {
	if (__cntrl_)
		__cntrl_->__add_weak();
}

template <class _Tp>
inline weak_ptr<_Tp>::weak_ptr(weak_ptr &&__r) throw() : __ptr_(__r.__ptr_), __cntrl_(__r.__cntrl_) {
	__r.__ptr_ = nullptr;
	__r.__cntrl_ = nullptr;
}

template <class _Tp>
template <class _Yp>
inline weak_ptr<_Tp>::weak_ptr(weak_ptr<_Yp> &&__r,
                               typename enable_if<is_convertible<_Yp *, _Tp *>::value, __nat *>::type) throw()
    : __ptr_(__r.__ptr_), __cntrl_(__r.__cntrl_) {
	__r.__ptr_ = nullptr;
	__r.__cntrl_ = nullptr;
}

template <class _Tp>
weak_ptr<_Tp>::~weak_ptr() {
	if (__cntrl_)
		__cntrl_->__release_weak();
}

template <class _Tp>
inline weak_ptr<_Tp> &weak_ptr<_Tp>::operator=(weak_ptr const &__r) throw() {
	weak_ptr(__r).swap(*this);
	return *this;
}

template <class _Tp>
template <class _Yp>
inline typename enable_if<is_convertible<_Yp *, _Tp *>::value, weak_ptr<_Tp> &>::type
weak_ptr<_Tp>::operator=(weak_ptr<_Yp> const &__r) throw() {
	weak_ptr(__r).swap(*this);
	return *this;
}

template <class _Tp>
inline weak_ptr<_Tp> &weak_ptr<_Tp>::operator=(weak_ptr &&__r) throw() {
	weak_ptr(std::move(__r)).swap(*this);
	return *this;
}

template <class _Tp>
template <class _Yp>
inline typename enable_if<is_convertible<_Yp *, _Tp *>::value, weak_ptr<_Tp> &>::type
weak_ptr<_Tp>::operator=(weak_ptr<_Yp> &&__r) throw() {
	weak_ptr(std::move(__r)).swap(*this);
	return *this;
}

template <class _Tp>
template <class _Yp>
inline typename enable_if<is_convertible<_Yp *, _Tp *>::value, weak_ptr<_Tp> &>::type
weak_ptr<_Tp>::operator=(shared_ptr<_Yp> const &__r) throw() {
	weak_ptr(__r).swap(*this);
	return *this;
}

template <class _Tp>
inline void weak_ptr<_Tp>::swap(weak_ptr &__r) throw() {
	std::swap(__ptr_, __r.__ptr_);
	std::swap(__cntrl_, __r.__cntrl_);
}

template <class _Tp>
inline void swap(weak_ptr<_Tp> &__x, weak_ptr<_Tp> &__y) throw() {
	__x.swap(__y);
}

template <class _Tp>
inline void weak_ptr<_Tp>::reset() throw() {
	weak_ptr().swap(*this);
}

template <class _Tp>
template <class _Yp>
shared_ptr<_Tp>::shared_ptr(const weak_ptr<_Yp> &__r,
                            typename enable_if<is_convertible<_Yp *, element_type *>::value, __nat>::type)
    : __ptr_(__r.__ptr_), __cntrl_(__r.__cntrl_ ? __r.__cntrl_->lock() : __r.__cntrl_) {
	if (__cntrl_ == nullptr)
		__throw_bad_weak_ptr();
}

template <class _Tp>
shared_ptr<_Tp> weak_ptr<_Tp>::lock() const throw() {
	shared_ptr<_Tp> __r;
	__r.__cntrl_ = __cntrl_ ? __cntrl_->lock() : __cntrl_;
	if (__r.__cntrl_)
		__r.__ptr_ = __ptr_;
	return __r;
}

#if _LIBCPP_STD_VER > 14
template <class _Tp = void>
struct owner_less;
#else
template <class _Tp>
struct owner_less;
#endif

_LIBCPP_SUPPRESS_DEPRECATED_PUSH
template <class _Tp>
struct owner_less<shared_ptr<_Tp>>
#if !defined(_LIBCPP_ABI_NO_BINDER_BASES)
    : binary_function<shared_ptr<_Tp>, shared_ptr<_Tp>, bool>
#endif
{
	_LIBCPP_SUPPRESS_DEPRECATED_POP
#if _LIBCPP_STD_VER <= 17 || defined(_LIBCPP_ENABLE_CXX20_REMOVED_BINDER_TYPEDEFS)
	_LIBCPP_DEPRECATED_IN_CXX17 typedef bool result_type;
	_LIBCPP_DEPRECATED_IN_CXX17 typedef shared_ptr<_Tp> first_argument_type;
	_LIBCPP_DEPRECATED_IN_CXX17 typedef shared_ptr<_Tp> second_argument_type;
#endif

	bool operator()(shared_ptr<_Tp> const &__x, shared_ptr<_Tp> const &__y) const throw() {
		return __x.owner_before(__y);
	}

	bool operator()(shared_ptr<_Tp> const &__x, weak_ptr<_Tp> const &__y) const throw() {
		return __x.owner_before(__y);
	}

	bool operator()(weak_ptr<_Tp> const &__x, shared_ptr<_Tp> const &__y) const throw() {
		return __x.owner_before(__y);
	}
};

_LIBCPP_SUPPRESS_DEPRECATED_PUSH
template <class _Tp>
struct owner_less<weak_ptr<_Tp>>
#if !defined(_LIBCPP_ABI_NO_BINDER_BASES)
    : binary_function<weak_ptr<_Tp>, weak_ptr<_Tp>, bool>
#endif
{
	_LIBCPP_SUPPRESS_DEPRECATED_POP
#if _LIBCPP_STD_VER <= 17 || defined(_LIBCPP_ENABLE_CXX20_REMOVED_BINDER_TYPEDEFS)
	_LIBCPP_DEPRECATED_IN_CXX17 typedef bool result_type;
	_LIBCPP_DEPRECATED_IN_CXX17 typedef weak_ptr<_Tp> first_argument_type;
	_LIBCPP_DEPRECATED_IN_CXX17 typedef weak_ptr<_Tp> second_argument_type;
#endif

	bool operator()(weak_ptr<_Tp> const &__x, weak_ptr<_Tp> const &__y) const throw() {
		return __x.owner_before(__y);
	}

	bool operator()(shared_ptr<_Tp> const &__x, weak_ptr<_Tp> const &__y) const throw() {
		return __x.owner_before(__y);
	}

	bool operator()(weak_ptr<_Tp> const &__x, shared_ptr<_Tp> const &__y) const throw() {
		return __x.owner_before(__y);
	}
};

#if _LIBCPP_STD_VER > 14
template <>
struct owner_less<void> {
	template <class _Tp, class _Up>

	bool operator()(shared_ptr<_Tp> const &__x, shared_ptr<_Up> const &__y) const throw() {
		return __x.owner_before(__y);
	}
	template <class _Tp, class _Up>

	bool operator()(shared_ptr<_Tp> const &__x, weak_ptr<_Up> const &__y) const throw() {
		return __x.owner_before(__y);
	}
	template <class _Tp, class _Up>

	bool operator()(weak_ptr<_Tp> const &__x, shared_ptr<_Up> const &__y) const throw() {
		return __x.owner_before(__y);
	}
	template <class _Tp, class _Up>

	bool operator()(weak_ptr<_Tp> const &__x, weak_ptr<_Up> const &__y) const throw() {
		return __x.owner_before(__y);
	}
	typedef void is_transparent;
};
#endif

//===--------------------------------------------------------------------===//
// Enable shared from this
//===--------------------------------------------------------------------===//

template <class _Tp>
class enable_shared_from_this {
	mutable weak_ptr<_Tp> __weak_this_;

protected:
	_LIBCPP_CONSTEXPR
	enable_shared_from_this() throw() {
	}

	enable_shared_from_this(enable_shared_from_this const &) throw() {
	}

	enable_shared_from_this &operator=(enable_shared_from_this const &) throw() {
		return *this;
	}

	~enable_shared_from_this() {
	}

public:
	shared_ptr<_Tp> shared_from_this() {
		return shared_ptr<_Tp>(__weak_this_);
	}

	shared_ptr<_Tp const> shared_from_this() const {
		return shared_ptr<const _Tp>(__weak_this_);
	}

#if _LIBCPP_STD_VER > 14

	weak_ptr<_Tp> weak_from_this() throw() {
		return __weak_this_;
	}

	weak_ptr<const _Tp> weak_from_this() const throw() {
		return __weak_this_;
	}
#endif // _LIBCPP_STD_VER > 14

	template <class _Up>
	friend class shared_ptr;
};

template <class _Tp>
struct hash;

template <class _Tp>
struct hash<shared_ptr<_Tp>> {
#if _LIBCPP_STD_VER <= 17 || defined(_LIBCPP_ENABLE_CXX20_REMOVED_BINDER_TYPEDEFS)
	_LIBCPP_DEPRECATED_IN_CXX17 typedef shared_ptr<_Tp> argument_type;
	_LIBCPP_DEPRECATED_IN_CXX17 typedef size_t result_type;
#endif

	size_t operator()(const shared_ptr<_Tp> &__ptr) const throw() {
		return hash<typename shared_ptr<_Tp>::element_type *>()(__ptr.get());
	}
};

template <class _CharT, class _Traits, class _Yp>
inline basic_ostream<_CharT, _Traits> &operator<<(basic_ostream<_CharT, _Traits> &__os, shared_ptr<_Yp> const &__p);

#if !defined(_LIBCPP_HAS_NO_THREADS)

class _LIBCPP_TYPE_VIS __sp_mut {
	void *__lx;

public:
	void lock() throw();
	void unlock() throw();

private:
	_LIBCPP_CONSTEXPR __sp_mut(void *) throw();
	__sp_mut(const __sp_mut &);
	__sp_mut &operator=(const __sp_mut &);

	friend _LIBCPP_FUNC_VIS __sp_mut &__get_sp_mut(const void *);
};

_LIBCPP_FUNC_VIS _LIBCPP_AVAILABILITY_ATOMIC_SHARED_PTR __sp_mut &__get_sp_mut(const void *);

template <class _Tp>
inline bool atomic_is_lock_free(const shared_ptr<_Tp> *) {
	return false;
}

template <class _Tp>
_LIBCPP_AVAILABILITY_ATOMIC_SHARED_PTR shared_ptr<_Tp> atomic_load(const shared_ptr<_Tp> *__p) {
	__sp_mut &__m = __get_sp_mut(__p);
	__m.lock();
	shared_ptr<_Tp> __q = *__p;
	__m.unlock();
	return __q;
}

template <class _Tp>
inline _LIBCPP_AVAILABILITY_ATOMIC_SHARED_PTR shared_ptr<_Tp> atomic_load_explicit(const shared_ptr<_Tp> *__p,
                                                                                   memory_order) {
	return atomic_load(__p);
}

template <class _Tp>
_LIBCPP_AVAILABILITY_ATOMIC_SHARED_PTR void atomic_store(shared_ptr<_Tp> *__p, shared_ptr<_Tp> __r) {
	__sp_mut &__m = __get_sp_mut(__p);
	__m.lock();
	__p->swap(__r);
	__m.unlock();
}

template <class _Tp>
inline _LIBCPP_AVAILABILITY_ATOMIC_SHARED_PTR void atomic_store_explicit(shared_ptr<_Tp> *__p, shared_ptr<_Tp> __r,
                                                                         memory_order) {
	atomic_store(__p, __r);
}

template <class _Tp>
_LIBCPP_AVAILABILITY_ATOMIC_SHARED_PTR shared_ptr<_Tp> atomic_exchange(shared_ptr<_Tp> *__p, shared_ptr<_Tp> __r) {
	__sp_mut &__m = __get_sp_mut(__p);
	__m.lock();
	__p->swap(__r);
	__m.unlock();
	return __r;
}

template <class _Tp>
inline _LIBCPP_AVAILABILITY_ATOMIC_SHARED_PTR shared_ptr<_Tp>
atomic_exchange_explicit(shared_ptr<_Tp> *__p, shared_ptr<_Tp> __r, memory_order) {
	return atomic_exchange(__p, __r);
}

template <class _Tp>
_LIBCPP_AVAILABILITY_ATOMIC_SHARED_PTR bool atomic_compare_exchange_strong(shared_ptr<_Tp> *__p, shared_ptr<_Tp> *__v,
                                                                           shared_ptr<_Tp> __w) {
	shared_ptr<_Tp> __temp;
	__sp_mut &__m = __get_sp_mut(__p);
	__m.lock();
	if (__p->__owner_equivalent(*__v)) {
		std::swap(__temp, *__p);
		*__p = __w;
		__m.unlock();
		return true;
	}
	std::swap(__temp, *__v);
	*__v = *__p;
	__m.unlock();
	return false;
}

template <class _Tp>
inline _LIBCPP_AVAILABILITY_ATOMIC_SHARED_PTR bool
atomic_compare_exchange_weak(shared_ptr<_Tp> *__p, shared_ptr<_Tp> *__v, shared_ptr<_Tp> __w) {
	return atomic_compare_exchange_strong(__p, __v, __w);
}

template <class _Tp>
inline _LIBCPP_AVAILABILITY_ATOMIC_SHARED_PTR bool
atomic_compare_exchange_strong_explicit(shared_ptr<_Tp> *__p, shared_ptr<_Tp> *__v, shared_ptr<_Tp> __w, memory_order,
                                        memory_order) {
	return atomic_compare_exchange_strong(__p, __v, __w);
}

template <class _Tp>
inline _LIBCPP_AVAILABILITY_ATOMIC_SHARED_PTR bool
atomic_compare_exchange_weak_explicit(shared_ptr<_Tp> *__p, shared_ptr<_Tp> *__v, shared_ptr<_Tp> __w, memory_order,
                                      memory_order) {
	return atomic_compare_exchange_weak(__p, __v, __w);
}

#endif // !defined(_LIBCPP_HAS_NO_THREADS)

} // namespace duckdb
