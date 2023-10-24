//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dependency.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/enums/catalog_type.hpp"

namespace duckdb {
class CatalogEntry;

enum class DependencyType {
	DEPENDENCY_REGULAR = 0,
	DEPENDENCY_AUTOMATIC = 1,
	DEPENDENCY_OWNS = 2,
	DEPENDENCY_OWNED_BY = 3
};

struct LogicalCatalogEntry {
public:
	LogicalCatalogEntry(const string &name, const string &schema, const string &catalog, CatalogType type)
	    : name(name), schema(schema), catalog(catalog), type(type) {
	}
	LogicalCatalogEntry(const LogicalCatalogEntry &other)
	    : name(other.name), schema(other.schema), catalog(other.schema), type(other.type) {
	}
	LogicalCatalogEntry &operator=(const LogicalCatalogEntry &other) {
		name = other.name;
		schema = other.schema;
		catalog = other.catalog;
		type = other.type;
		return *this;
	}
	LogicalCatalogEntry(CatalogEntry &entry);
	LogicalCatalogEntry() : name(), schema(), catalog(), type(CatalogType::INVALID) {
	}

public:
	bool operator==(const LogicalCatalogEntry &other) const;
	bool operator!=(const LogicalCatalogEntry &other) const {
		return !(*this == other);
	}

public:
	void Serialize(Serializer &serializer) const;
	LogicalCatalogEntry Deserialize(Deserializer &deserializer);

public:
	string name;
	string schema;
	string catalog;
	CatalogType type;
};

struct Dependency {
	Dependency(const string &name, const string &schema, const string &catalog, CatalogType type,
	           DependencyType dependency_type = DependencyType::DEPENDENCY_REGULAR)
	    : // NOLINT: Allow implicit conversion from `CatalogEntry`
	      entry(name, schema, catalog, type), dependency_type(dependency_type) {
	}
	Dependency(const LogicalCatalogEntry &entry, DependencyType dependency_type = DependencyType::DEPENDENCY_REGULAR)
	    : entry(entry), dependency_type(dependency_type) {
	}
	Dependency(CatalogEntry &entry, DependencyType dependency_type = DependencyType::DEPENDENCY_REGULAR)
	    : entry(entry), dependency_type(dependency_type) {
	}

	LogicalCatalogEntry entry;
	DependencyType dependency_type;
};

struct DependencyHashFunction {
	uint64_t operator()(const Dependency &a) const;
};

struct DependencyEquality {
	bool operator()(const Dependency &a, const Dependency &b) const;
};

using dependency_set_t = unordered_set<Dependency, DependencyHashFunction, DependencyEquality>;

template <class T>
using dependency_map_t = unordered_map<Dependency, T, DependencyHashFunction, DependencyEquality>;

} // namespace duckdb
