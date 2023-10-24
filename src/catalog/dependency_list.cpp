#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

void DependencyList::AddDependency(CatalogEntry &entry) {
	if (entry.internal) {
		return;
	}
	set.insert(entry);
}

void DependencyList::VerifyDependencies(Catalog &catalog, const string &name) {
	for (auto &dep_entry : set) {
		auto &dep = dep_entry;
		if (dep.entry.catalog != catalog.GetName()) {
			throw DependencyException(
			    "Error adding dependency for object \"%s\" - dependency \"%s\" is in catalog "
			    "\"%s\", which does not match the catalog \"%s\".\nCross catalog dependencies are not supported.",
			    name, dep.entry.name, dep.entry.catalog, catalog.GetName());
		}
	}
}

LogicalDependencyList DependencyList::GetLogical() const {
	LogicalDependencyList result;
	for (auto &entry : set) {
		result.AddDependency(entry);
	}
	return result;
}

bool DependencyList::Contains(CatalogEntry &entry) {
	return set.count(entry);
}

void LogicalDependencyList::AddDependency(CatalogEntry &entry) {
	Dependency dependency(entry);
	set.insert(std::move(dependency));
}

void LogicalDependencyList::AddDependency(const Dependency &entry) {
	set.insert(entry);
}

bool LogicalDependencyList::Contains(CatalogEntry &entry_p) {
	Dependency logical_entry(entry_p);
	return set.count(logical_entry);
}

// DependencyList LogicalDependencyList::GetPhysical(ClientContext &context, Catalog &catalog) const {
//	DependencyList dependencies;

//	for (auto &entry : set) {
//		auto &name = entry.name;
//		// Don't use the serialized catalog name, could be attached with a different name
//		auto &schema = entry.schema;
//		auto &type = entry.type;

//		CatalogEntryLookup lookup;
//		if (type == CatalogType::SCHEMA_ENTRY) {
//			auto lookup = catalog.GetSchema(context, name, OnEntryNotFound::THROW_EXCEPTION);
//			D_ASSERT(lookup);
//			dependencies.AddDependency(*lookup);
//		} else {
//			auto lookup = catalog.LookupEntry(context, type, schema, name, OnEntryNotFound::THROW_EXCEPTION);
//			D_ASSERT(lookup.Found());
//			auto catalog_entry = lookup.entry;
//			dependencies.AddDependency(*catalog_entry);
//		}
//	}
//	return dependencies;
//}

// void LogicalDependencyList::VerifyDependencies(Catalog &catalog, const string &name) {
//	for (auto &dep : set) {
//		if (dep.catalog != catalog.GetName()) {
//			throw DependencyException(
//			    "Error adding dependency for object \"%s\" - dependency \"%s\" is in catalog "
//			    "\"%s\", which does not match the catalog \"%s\".\nCross catalog dependencies are not supported.",
//			    name, dep.name, dep.catalog, catalog.GetName());
//		}
//	}
//}

void LogicalDependencyList::Serialize(Serializer &serializer) const {
	// serializer.WriteProperty(0, "logical_dependencies", set);
}

const LogicalDependencyList::create_info_set_t &LogicalDependencyList::Set() const {
	return set;
}

LogicalDependencyList LogicalDependencyList::Deserialize(Deserializer &deserializer) {
	LogicalDependencyList dependency;
	// dependency.set = deserializer.ReadProperty<create_info_set_t>(0, "logical_dependencies");
	return dependency;
}

bool LogicalDependencyList::operator==(const LogicalDependencyList &other) const {
	if (set.size() != other.set.size()) {
		return false;
	}

	for (auto &entry : set) {
		if (!other.set.count(entry)) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
