//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/name_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include <type_traits>

namespace duckdb {

template <class V>
class NameMap {
public:
	NameMap() {
	}
	virtual ~NameMap() {
	}

public:
	using iterator = typename unordered_map<string, V>::iterator;
	using const_iterator = typename unordered_map<string, V>::const_iterator;

public:
	virtual iterator begin() = 0;                             // NOLINT: match stl API
	virtual iterator end() = 0;                               // NOLINT: match stl API
	virtual const_iterator begin() const = 0;                 // NOLINT: match stl API
	virtual const_iterator end() const = 0;                   // NOLINT: match stl API
	virtual iterator find(const string &key) = 0;             // NOLINT: match stl API
	virtual const_iterator find(const string &key) const = 0; // NOLINT: match stl API
	virtual idx_t size() const = 0;                           // NOLINT: match stl API
	virtual bool empty() const = 0;                           // NOLINT: match stl API
	virtual void insert(pair<string, V> &&value) = 0;         // NOLINT: match stl API
	virtual void erase(const string &key) = 0;                // NOLINT: match stl API
	virtual void erase(iterator it) = 0;                      // NOLINT: match stl API
	virtual bool contains(const string &key) const = 0;       // NOLINT: match stl API
	virtual const V &at(const string &key) const = 0;         // NOLINT: match stl API
	virtual V &operator[](const string &key) = 0;             // NOLINT: match stl API
};

template <class V>
class CaseSensitiveNameMap : public NameMap<V> {
public:
	CaseSensitiveNameMap() {
	}

public:
	using iterator = typename NameMap<V>::iterator;
	using const_iterator = typename NameMap<V>::const_iterator;

public:
	iterator begin() { // NOLINT: match stl API
		return name_map.begin();
	}

	iterator end() { // NOLINT: match stl API
		return name_map.end();
	}

	const_iterator begin() const { // NOLINT: match stl API
		return name_map.begin();
	}

	const_iterator end() const { // NOLINT: match stl API
		return name_map.end();
	}

	iterator find(const string &key) { // NOLINT: match stl API
		return name_map.find(key);
	}

	const_iterator find(const string &key) const { // NOLINT: match stl API
		return name_map.find(key);
	}

	idx_t size() const { // NOLINT: match stl API
		return name_map.size();
	}

	bool empty() const { // NOLINT: match stl API
		return name_map.empty();
	}

	void insert(pair<string, V> &&value) { // NOLINT: match stl API
		name_map.insert(std::move(value));
	}

	void erase(iterator it) { // NOLINT: match stl API
		name_map.erase(it);
	}

	void erase(const string &key) { // NOLINT: match stl API
		name_map.erase(key);
	}

	bool contains(const string &key) const { // NOLINT: match stl API
		return name_map.count(key);
	}

	const V &at(const string &key) const { // NOLINT: match stl API
		return name_map.at(key);
	}

	V &operator[](const string &key) { // NOLINT: match stl API
		return name_map[key];
	}

private:
	unordered_map<string, V> name_map;
};

template <class V>
class CaseInsensitiveNameMap : public NameMap<V> {
public:
	CaseInsensitiveNameMap() {
	}

public:
	using iterator = typename NameMap<V>::iterator;
	using const_iterator = typename NameMap<V>::const_iterator;

public:
	iterator begin() { // NOLINT: match stl API
		return name_map.begin();
	}

	iterator end() { // NOLINT: match stl API
		return name_map.end();
	}

	const_iterator begin() const { // NOLINT: match stl API
		return name_map.begin();
	}

	const_iterator end() const { // NOLINT: match stl API
		return name_map.end();
	}

	iterator find(const string &key) { // NOLINT: match stl API
		return name_map.find(key);
	}

	const_iterator find(const string &key) const { // NOLINT: match stl API
		return name_map.find(key);
	}

	idx_t size() const { // NOLINT: match stl API
		return name_map.size();
	}

	bool empty() const { // NOLINT: match stl API
		return name_map.empty();
	}

	void insert(pair<string, V> &&value) { // NOLINT: match stl API
		name_map.insert(std::move(value));
	}

	void erase(iterator it) { // NOLINT: match stl API
		name_map.erase(it);
	}

	void erase(const string &key) { // NOLINT: match stl API
		name_map.erase(key);
	}

	bool contains(const string &key) const { // NOLINT: match stl API
		return name_map.count(key);
	}

	const V &at(const string &key) const { // NOLINT: match stl API
		return name_map.at(key);
	}

	V &operator[](const string &key) { // NOLINT: match stl API
		return name_map[key];
	}

private:
	case_insensitive_map_t<V> name_map;
};

} // namespace duckdb
