//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/unordered_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_set>

namespace duckdb {

template <typename T, typename Hash = std::hash<T>, typename KeyEqual = std::equal_to<T>>
class unordered_set {
public:
	using value_type = T;
	using key_type = T;
	using hasher = Hash;
	using key_equal = KeyEqual;
	using allocator_type = typename std::unordered_set<T, Hash, KeyEqual>::allocator_type;
	using reference = typename std::unordered_set<T, Hash, KeyEqual>::reference;
	using const_reference = typename std::unordered_set<T, Hash, KeyEqual>::const_reference;
	using pointer = typename std::unordered_set<T, Hash, KeyEqual>::pointer;
	using const_pointer = typename std::unordered_set<T, Hash, KeyEqual>::const_pointer;
	using iterator = typename std::unordered_set<T, Hash, KeyEqual>::iterator;
	using const_iterator = typename std::unordered_set<T, Hash, KeyEqual>::const_iterator;
	using local_iterator = typename std::unordered_set<T, Hash, KeyEqual>::local_iterator;
	using const_local_iterator = typename std::unordered_set<T, Hash, KeyEqual>::const_local_iterator;
	using size_type = typename std::unordered_set<T, Hash, KeyEqual>::size_type;
	using difference_type = typename std::unordered_set<T, Hash, KeyEqual>::difference_type;

	unordered_set();
	unordered_set(std::initializer_list<value_type> init, size_type bucket_count = 1, const Hash &hash = Hash(),
	              const KeyEqual &equal = KeyEqual());
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
	std::unordered_set<T, Hash, KeyEqual> _set;
};

template <typename T, typename Hash, typename KeyEqual>
template <typename... Args>
std::pair<typename unordered_set<T, Hash, KeyEqual>::iterator, bool>
unordered_set<T, Hash, KeyEqual>::emplace(Args &&...args) {
	return _set.emplace(std::forward<Args>(args)...);
}

template <typename T, typename Hash, typename KeyEqual>
unordered_set<T, Hash, KeyEqual>::unordered_set() : _set() {
}

template <typename T, typename Hash, typename KeyEqual>
unordered_set<T, Hash, KeyEqual>::unordered_set(size_type bucket_count, const Hash &hash, const KeyEqual &equal)
    : _set(bucket_count, hash, equal) {
}

template <typename T, typename Hash, typename KeyEqual>
unordered_set<T, Hash, KeyEqual>::unordered_set(std::initializer_list<value_type> init, size_type bucket_count,
                                                const Hash &hash, const KeyEqual &equal)
    : _set(init, bucket_count, hash, equal) {
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::iterator unordered_set<T, Hash, KeyEqual>::find(const value_type &value) {
	return _set.find(value);
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::const_iterator
unordered_set<T, Hash, KeyEqual>::find(const value_type &value) const {
	return _set.find(value);
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::size_type
unordered_set<T, Hash, KeyEqual>::count(const value_type &value) const {
	return _set.count(value);
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::iterator unordered_set<T, Hash, KeyEqual>::erase(iterator pos) {
	return _set.erase(pos);
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::iterator unordered_set<T, Hash, KeyEqual>::erase(iterator first,
                                                                                            iterator last) {
	return _set.erase(first, last);
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
	return _set.begin();
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::iterator unordered_set<T, Hash, KeyEqual>::end() {
	return _set.end();
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::const_iterator unordered_set<T, Hash, KeyEqual>::begin() const {
	return _set.begin();
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::const_iterator unordered_set<T, Hash, KeyEqual>::end() const {
	return _set.end();
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
	_set.reserve(count);
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
	return _set.insert(value);
}

template <typename T, typename Hash, typename KeyEqual>
template <typename P>
std::pair<typename unordered_set<T, Hash, KeyEqual>::iterator, bool>
unordered_set<T, Hash, KeyEqual>::insert(P &&value) {
	return _set.insert(std::forward<P>(value));
}

template <typename T, typename Hash, typename KeyEqual>
typename unordered_set<T, Hash, KeyEqual>::iterator unordered_set<T, Hash, KeyEqual>::insert(const_iterator hint,
                                                                                             const value_type &value) {
	return _set.insert(hint, value);
}

template <typename T, typename Hash, typename KeyEqual>
template <typename P>
typename unordered_set<T, Hash, KeyEqual>::iterator unordered_set<T, Hash, KeyEqual>::insert(const_iterator hint,
                                                                                             P &&value) {
	return _set.insert(hint, std::forward<P>(value));
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
