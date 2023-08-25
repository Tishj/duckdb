//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dependency_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/field_writer.hpp"
#include "duckdb/catalog/catalog_entry_map.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {
class Catalog;
class CatalogEntry;
struct CreateInfo;
class SchemaCatalogEntry;
struct CatalogTransaction;

//! The DependencyList containing CatalogEntry references, looked up in the catalog
class PhysicalDependencyList {
	friend class DependencyManager;

public:
	DUCKDB_API void AddDependency(CatalogEntry &entry);

	DUCKDB_API void VerifyDependencies(Catalog &catalog, const string &name);

	DUCKDB_API bool Contains(CatalogEntry &entry);

private:
	catalog_entry_set_t set;
};

//! A minimal representation of a CreateInfo / CatalogEntry
//! enough to look up the entry inside SchemaCatalogEntry::GetEntry
struct LogicalDependency {
	string name;
	CatalogType type;
};

struct CreateInfoHashFunction {
	uint64_t operator()(const LogicalDependency &a) const;
};

struct CreateInfoEquality {
	bool operator()(const LogicalDependency &a, const LogicalDependency &b) const;
};

//! The DependencyList containing LogicalDependency objects, not looked up in the catalog yet
class LogicalDependencyList {
	using create_info_set_t = unordered_set<LogicalDependency, CreateInfoHashFunction, CreateInfoEquality>;
public:
	DUCKDB_API void AddDependency(LogicalDependency entry);
	DUCKDB_API void AddDependency(CatalogEntry &entry);
	DUCKDB_API bool Contains(LogicalDependency& entry);
	DUCKDB_API PhysicalDependencyList GetPhysical(SchemaCatalogEntry &schema, CatalogTransaction &transaction) const;

private:
	create_info_set_t set;
};

} // namespace duckdb
