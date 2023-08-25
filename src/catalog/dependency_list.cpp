#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

void PhysicalDependencyList::AddDependency(CatalogEntry &entry) {
	if (entry.internal) {
		return;
	}
	set.insert(entry);
}

void PhysicalDependencyList::VerifyDependencies(Catalog &catalog, const string &name) {
	for (auto &dep_entry : set) {
		auto &dep = dep_entry.get();
		if (&dep.ParentCatalog() != &catalog) {
			throw DependencyException(
			    "Error adding dependency for object \"%s\" - dependency \"%s\" is in catalog "
			    "\"%s\", which does not match the catalog \"%s\".\nCross catalog dependencies are not supported.",
			    name, dep.name, dep.ParentCatalog().GetName(), catalog.GetName());
		}
	}
}

bool PhysicalDependencyList::Contains(CatalogEntry &entry) {
	return set.count(entry);
}

uint64_t CreateInfoHashFunction::operator()(const LogicalDependency &a) const {
	hash_t hash = duckdb::Hash<string_t>(a.name);
	hash = CombineHash(hash, duckdb::Hash<uint8_t>(static_cast<uint8_t>(a.type)));
	return hash;
}

bool CreateInfoEquality::operator()(const LogicalDependency &a, const LogicalDependency &b) const {
	if (a.type != b.type) {
		return false;
	}
	if (a.name != b.name) {
		return false;
	}
	return true;
}

void LogicalDependencyList::AddDependency(LogicalDependency entry) {
	set.insert(entry);
}

void LogicalDependencyList::AddDependency(CatalogEntry &entry) {
	LogicalDependency dependency = {entry.name, entry.type};
	set.insert(dependency);
}

bool LogicalDependencyList::Contains(LogicalDependency& entry) {
	return set.count(entry);
}

PhysicalDependencyList LogicalDependencyList::GetPhysical(SchemaCatalogEntry &schema, CatalogTransaction &transaction) const {
	PhysicalDependencyList dependencies;
	for (auto &entry : set) {
		auto &name = entry.name;
		auto &type = entry.type;

		auto catalog_entry = schema.GetEntry(transaction, type, name);
		dependencies.AddDependency(*catalog_entry);
	}
	return dependencies;
}

} // namespace duckdb
