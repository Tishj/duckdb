//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/unordered_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_set>
#include <set>

#define DUCKDB_DEBUG_UNORDERED_SET 1

namespace duckdb {

template <typename T, typename Hash = std::hash<T>, typename KeyEqual = std::equal_to<T>>
class unordered_set {
#ifdef DUCKDB_DEBUG_UNORDERED_SET
	struct SetCompare {
		bool operator()(const T &__x, const T &__y) const {
			return KeyEqual()(__x, __y);
		}
	};
	using original = std::set<T, SetCompare>;
#else
	using original = std::unordered_set<T, Hash, KeyEqual>;
#endif
public:
	using value_type = T;
	using key_type = T;
	using hasher = Hash;
	using key_equal = KeyEqual;
	using allocator_type = typename original::allocator_type;
	using reference = typename original::reference;
	using const_reference = typename original::const_reference;
	using pointer = typename original::pointer;

#ifdef DUCKDB_DEBUG_UNORDERED_SET
	using iterator = typename original::reverse_iterator;
	using const_iterator = typename original::const_reverse_iterator;
#else
	using iterator = typename original::iterator;
	using const_iterator = typename original::const_iterator;
#endif

	using size_type = typename original::size_type;
	using difference_type = typename original::difference_type;

	unordered_set();
	unordered_set(std::initializer_list<value_type> init);
	explicit unordered_set(size_type bucket_count, const Hash &hash = Hash(), const KeyEqual &equal = KeyEqual());

	iterator erase(iterator pos);
	iterator erase(iterator first, iterator last);
	size_type erase(const value_type &value);
	iterator find(const value_type &value);
	const_iterator find(const value_type &value) const;
	size_type count(const value_type &value) const;
	void clear();
	size_type size() const;
	bool empty() const;
	iterator begin();
	iterator end();
	const_iterator begin() const;
	const_iterator end() const;
	float load_factor() const;
	float max_load_factor() const;
	void rehash(size_type count);
	void reserve(size_type count);

	// Emplace method
	template <typename... Args>
	std::pair<iterator, bool> emplace(Args &&...args);

	// Comparison operators
	bool operator==(const unordered_set &other) const;
	bool operator!=(const unordered_set &other) const;

	std::pair<iterator, bool> insert(const value_type &value);
	template <typename P>
	std::pair<iterator, bool> insert(P &&value);
	iterator insert(const_iterator hint, const value_type &value);
	template <typename P>
	iterator insert(const_iterator hint, P &&value);
	template <typename InputIt>
	void insert(InputIt first, InputIt last);
	void insert(std::initializer_list<value_type> ilist);

private:
	original _set;
};

template <typename T, typename Hash, typename KeyEqual>
template <typename... Args>
std::pair<typename unordered_set<T, Hash, KeyEqual>::iterator, bool>
unordered_set<T, Hash, KeyEqual>::emplace(Args &&...args) {
#ifdef DUCKDB_DEBUG_UNORDERED_SET
	auto result = _set.emplace(std::forward<Args>(args)...);
	return std::make_pair(std::reverse_iterator<typename original::iterator>(result.first), result.second);
#else
	return _set.emplace(std::forward<Args>(args)...);
#endif
}

template <typename T, typename Hash, typename KeyEqual>
unordered_set<T, Hash, KeyEqual>::unordered_set() : _set() {
}

template <typename T, typename Hash, typename KeyEqual>
unordered_set<T, Hash, KeyEqual>::unordered_set(size_type bucket_count, const Hash &hash, const KeyEqual &equal)
    : _set(bucket_count, hash, equal) {
}

template <typename T, typename Hash, typename KeyEqual>
unordered_set<T, Hash, KeyEqual>::unordered_set(std::initializer_list<value_type> init) : _set(init) {
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::iterator unordered_set<T, Hash, KeyEqual>::find(const value_type &value) {
#ifdef DUCKDB_DEBUG_UNORDERED_SET
	auto iter = _set.find(value);
	if (iter == _set.end()) {
		return _set.rend();
	}
	return std::reverse_iterator<typename original::iterator>(iter);
#else
	return _set.find(value);
#endif
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::const_iterator
unordered_set<T, Hash, KeyEqual>::find(const value_type &value) const {
#ifdef DUCKDB_DEBUG_UNORDERED_SET
	auto iter = _set.find(value);
	if (iter == _set.end()) {
		return _set.rend();
	}
	return std::reverse_iterator<typename original::const_iterator>(iter);
#else
	return _set.find(value);
#endif
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::size_type
unordered_set<T, Hash, KeyEqual>::count(const value_type &value) const {
	return _set.count(value);
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::iterator unordered_set<T, Hash, KeyEqual>::erase(iterator pos) {
#ifdef DUCKDB_DEBUG_UNORDERED_SET
	return std::reverse_iterator<typename original::iterator>(_set.erase((pos++).base()));
#else
	return _set.erase(pos);
#endif
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::iterator unordered_set<T, Hash, KeyEqual>::erase(iterator first,
                                                                                            iterator last) {
#ifdef DUCKDB_DEBUG_UNORDERED_SET
	return std::reverse_iterator<typename original::iterator>(_set.erase((first++).base(), (last++).base()));
#else
	return _set.erase(first, last);
#endif
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::size_type unordered_set<T, Hash, KeyEqual>::erase(const value_type &value) {
	return _set.erase(value);
}

template <typename T, typename Hash, typename KeyEqual>
void unordered_set<T, Hash, KeyEqual>::clear() {
	_set.clear();
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::size_type unordered_set<T, Hash, KeyEqual>::size() const {
	return _set.size();
}

template <typename T, typename Hash, typename KeyEqual>
bool unordered_set<T, Hash, KeyEqual>::empty() const {
	return _set.empty();
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::iterator unordered_set<T, Hash, KeyEqual>::begin() {
#ifdef DUCKDB_DEBUG_UNORDERED_SET
	return _set.rbegin();
#else
	return _set.begin();
#endif
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::iterator unordered_set<T, Hash, KeyEqual>::end() {
#ifdef DUCKDB_DEBUG_UNORDERED_SET
	return _set.rend();
#else
	return _set.end();
#endif
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::const_iterator unordered_set<T, Hash, KeyEqual>::begin() const {
#ifdef DUCKDB_DEBUG_UNORDERED_SET
	return _set.rbegin();
#else
	return _set.begin();
#endif
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::const_iterator unordered_set<T, Hash, KeyEqual>::end() const {
#ifdef DUCKDB_DEBUG_UNORDERED_SET
	return _set.rend();
#else
	return _set.end();
#endif
}

template <typename T, typename Hash, typename KeyEqual>
float unordered_set<T, Hash, KeyEqual>::load_factor() const {
	return _set.load_factor();
}

template <typename T, typename Hash, typename KeyEqual>
float unordered_set<T, Hash, KeyEqual>::max_load_factor() const {
	return _set.max_load_factor();
}

template <typename T, typename Hash, typename KeyEqual>
void unordered_set<T, Hash, KeyEqual>::rehash(size_type count) {
	_set.rehash(count);
}

template <typename T, typename Hash, typename KeyEqual>
void unordered_set<T, Hash, KeyEqual>::reserve(size_type count) {
#ifdef DUCKDB_DEBUG_UNORDERED_SET
	return;
#else
	_set.reserve(count);
#endif
}

template <typename T, typename Hash, typename KeyEqual>
bool unordered_set<T, Hash, KeyEqual>::operator==(const unordered_set &other) const {
	return _set == other._set;
}

template <typename T, typename Hash, typename KeyEqual>
bool unordered_set<T, Hash, KeyEqual>::operator!=(const unordered_set &other) const {
	return _set != other._set;
}

template <typename T, typename Hash, typename KeyEqual>
std::pair<typename unordered_set<T, Hash, KeyEqual>::iterator, bool>
unordered_set<T, Hash, KeyEqual>::insert(const value_type &value) {
#ifdef DUCKDB_DEBUG_UNORDERED_SET
	auto result = _set.insert(value);
	return std::make_pair(std::reverse_iterator<typename original::iterator>(result.first), result.second);
#else
	return _set.insert(value);
#endif
}

template <typename T, typename Hash, typename KeyEqual>
template <typename P>
std::pair<typename unordered_set<T, Hash, KeyEqual>::iterator, bool>
unordered_set<T, Hash, KeyEqual>::insert(P &&value) {
#ifdef DUCKDB_DEBUG_UNORDERED_SET
	auto result = _set.insert(std::forward<P>(value));
	return std::make_pair(std::reverse_iterator<typename original::iterator>(result.first), result.second);
#else
	return _set.insert(std::forward<P>(value));
#endif
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::iterator unordered_set<T, Hash, KeyEqual>::insert(const_iterator hint,
                                                                                             const value_type &value) {
#ifdef DUCKDB_DEBUG_UNORDERED_SET
	return std::reverse_iterator<typename original::iterator>(_set.insert(hint, value));
#else
	return _set.insert(hint, value);
#endif
}

template <typename T, typename Hash, typename KeyEqual>
template <typename P>
typename unordered_set<T, Hash, KeyEqual>::iterator unordered_set<T, Hash, KeyEqual>::insert(const_iterator hint,
                                                                                             P &&value) {
#ifdef DUCKDB_DEBUG_UNORDERED_SET
	return std::reverse_iterator<typename original::iterator>(_set.insert(hint, std::forward<P>(value)));
#else
	return _set.insert(hint, std::forward<P>(value));
#endif
}

template <typename T, typename Hash, typename KeyEqual>
template <typename InputIt>
void unordered_set<T, Hash, KeyEqual>::insert(InputIt first, InputIt last) {
	_set.insert(first, last);
}

template <typename T, typename Hash, typename KeyEqual>
void unordered_set<T, Hash, KeyEqual>::insert(std::initializer_list<value_type> ilist) {
	_set.insert(ilist);
}

} // namespace duckdb
