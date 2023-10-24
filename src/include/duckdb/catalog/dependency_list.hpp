//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dependency_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry_map.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/dependency.hpp"

namespace duckdb {
class Catalog;
class CatalogEntry;
struct CreateInfo;
class SchemaCatalogEntry;
struct CatalogTransaction;
class LogicalDependencyList;

//! The DependencyList containing CatalogEntry references, looked up in the catalog
class DependencyList {
	friend class DependencyManager;

public:
	DUCKDB_API void AddDependency(CatalogEntry &entry);

	DUCKDB_API void VerifyDependencies(Catalog &catalog, const string &name);

	DUCKDB_API bool Contains(CatalogEntry &entry);

	DUCKDB_API LogicalDependencyList GetLogical() const;

private:
	dependency_set_t set;
};

//! The DependencyList containing LogicalDependency objects, not looked up in the catalog yet
class LogicalDependencyList {
	using create_info_set_t = unordered_set<Dependency, DependencyHashFunction, DependencyEquality>;

public:
	DUCKDB_API void AddDependency(CatalogEntry &entry);
	DUCKDB_API void AddDependency(const Dependency &entry);
	// DUCKDB_API DependencyList GetPhysical(ClientContext &context, Catalog &catalog) const;
	DUCKDB_API bool Contains(CatalogEntry &entry);

public:
	DUCKDB_API void VerifyDependencies(Catalog &catalog, const string &name);
	void Serialize(Serializer &serializer) const;
	static LogicalDependencyList Deserialize(Deserializer &deserializer);
	bool operator==(const LogicalDependencyList &other) const;
	const create_info_set_t &Set() const;

private:
	create_info_set_t set;
};

} // namespace duckdb
