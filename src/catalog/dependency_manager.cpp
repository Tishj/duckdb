#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/catalog/mapping_value.hpp"
#include "duckdb/catalog/dependency_list.hpp"

namespace duckdb {

DependencyManager::DependencyManager(DuckCatalog &catalog) : catalog(catalog) {
}

static CatalogEntryLookup LookupEntry(const Dependency &dep, Catalog &catalog, CatalogTransaction &transaction) {
	auto &entry = dep.entry;
	auto &name = entry.name;
	auto &schema = entry.schema;
	auto &type = entry.type;

	if (entry.type == CatalogType::SCHEMA_ENTRY /*|| entry.type == CatalogType::DATABASE_ENTRY */) {
		auto schema = catalog.GetSchema(transaction, name, OnEntryNotFound::THROW_EXCEPTION);
		auto entry = optional_ptr<CatalogEntry>(schema.get());
		return CatalogEntryLookup {nullptr, entry, PreservedError()};
	} else {
		return catalog.TryLookupEntryInternal(transaction, type, schema, name);
	}
}

void DependencyManager::AddObject(CatalogTransaction transaction, CatalogEntry &object, DependencyList &dependencies) {
	// check for each object in the sources if they were not deleted yet
	for (auto &dep : dependencies.set) {
		auto catalog_entry = LookupEntry(dep, catalog, transaction);
		if (!catalog_entry.entry) {
			throw InternalException("Dependency has already been deleted?");
		}
	}
	// indexes do not require CASCADE to be dropped, they are simply always dropped along with the table
	auto dependency_type = object.type == CatalogType::INDEX_ENTRY ? DependencyType::DEPENDENCY_AUTOMATIC
	                                                               : DependencyType::DEPENDENCY_REGULAR;
	// add the object to the dependents_map of each object that it depends on
	for (auto &dependency : dependencies.set) {
		auto &set = dependents_map[dependency];
		set.insert(Dependency(object, dependency_type));
	}
	// create the dependents map for this object: it starts out empty
	dependents_map[object] = dependency_set_t();
	dependencies_map[object] = dependencies.set;
}

void DependencyManager::DropObject(CatalogTransaction transaction, CatalogEntry &object, bool cascade) {
	auto dep_entry = Dependency(object);
	D_ASSERT(dependents_map.find(dep_entry) != dependents_map.end());

	// first check the objects that depend on this object
	auto &dependent_objects = dependents_map.at(object);
	for (auto &dep : dependent_objects) {
		// look up the entry in the catalog set
		auto lookup = LookupEntry(dep, catalog, transaction);
		D_ASSERT(!lookup.error);
		if (!lookup.entry) {
			// the dependent object was already deleted, no conflict
			continue;
		}
		auto &dependency_entry = *lookup.entry;
		D_ASSERT(dependency_entry.set);
		auto &catalog_set = *dependency_entry.set;
		auto mapping_value = catalog_set.GetMapping(transaction, dep.entry.name, /* get_latest = */ true);
		if (mapping_value == nullptr) {
			continue;
		}
		// conflict: attempting to delete this object but the dependent object still exists
		if (cascade || dep.dependency_type == DependencyType::DEPENDENCY_AUTOMATIC ||
		    dep.dependency_type == DependencyType::DEPENDENCY_OWNS) {
			// cascade: drop the dependent object
			catalog_set.DropEntryInternal(transaction, mapping_value->index.Copy(), dependency_entry, cascade);
		} else {
			// no cascade and there are objects that depend on this object: throw error
			throw DependencyException("Cannot drop entry \"%s\" because there are entries that "
			                          "depend on it. Use DROP...CASCADE to drop all dependents.",
			                          object.name);
		}
	}
}

void DependencyManager::AlterObject(CatalogTransaction transaction, CatalogEntry &old_obj, CatalogEntry &new_obj) {
	D_ASSERT(dependents_map.find(old_obj) != dependents_map.end());
	D_ASSERT(dependencies_map.find(old_obj) != dependencies_map.end());

	// first check the objects that depend on this object
	dependency_set_t owned_objects_to_add;
	auto &dependent_objects = dependents_map.at(old_obj);
	for (auto &dep : dependent_objects) {
		// look up the entry in the catalog set
		auto lookup = LookupEntry(dep, catalog, transaction);
		D_ASSERT(!lookup.error);
		if (!lookup.entry) {
			// the dependent object was already deleted, no conflict
			continue;
		}
		if (dep.dependency_type == DependencyType::DEPENDENCY_OWNS) {
			// the dependent object is owned by the current object
			owned_objects_to_add.insert(dep);
			continue;
		}
		// conflict: attempting to alter this object but the dependent object still exists
		// no cascade and there are objects that depend on this object: throw error
		throw DependencyException("Cannot alter entry \"%s\" because there are entries that "
		                          "depend on it.",
		                          old_obj.name);
	}
	// add the new object to the dependents_map of each object that it depends on
	auto &old_dependencies = dependencies_map[old_obj];
	for (auto &dep : old_dependencies) {
		auto &dependency = dep;
		dependents_map[dependency].insert(new_obj);
	}

	// We might have to add a type dependency
	// add the new object to the dependency manager
	dependents_map[new_obj] = dependency_set_t();
	dependencies_map[new_obj] = old_dependencies;

	for (auto &dependency : owned_objects_to_add) {
		dependents_map[new_obj].insert(dependency);
		dependents_map[dependency].insert(Dependency(new_obj, DependencyType::DEPENDENCY_OWNED_BY));
		dependencies_map[new_obj].insert(dependency);
	}
}

void DependencyManager::EraseObject(CatalogEntry &object) {
	// obtain the writing lock
	EraseObjectInternal(object);
}

void DependencyManager::EraseObjectInternal(CatalogEntry &object) {
	if (dependents_map.find(object) == dependents_map.end()) {
		// dependencies already removed
		return;
	}
	D_ASSERT(dependents_map.find(object) != dependents_map.end());
	D_ASSERT(dependencies_map.find(object) != dependencies_map.end());
	// now for each of the dependencies, erase the entries from the dependents_map
	for (auto &dependency : dependencies_map[object]) {
		auto entry = dependents_map.find(dependency);
		if (entry != dependents_map.end()) {
			D_ASSERT(entry->second.find(object) != entry->second.end());
			entry->second.erase(object);
		}
	}
	// erase the dependents and dependencies for this object
	dependents_map.erase(object);
	dependencies_map.erase(object);
}

void DependencyManager::Scan(ClientContext &context,
                             const std::function<void(CatalogEntry &, CatalogEntry &, DependencyType)> &callback) {
	lock_guard<mutex> write_lock(catalog.GetWriteLock());
	auto transaction = catalog.GetCatalogTransaction(context);
	for (auto &object : dependents_map) {
		for (auto &dependent_p : object.second) {
			auto lookup = LookupEntry(object.first, catalog, transaction);
			D_ASSERT(lookup.entry);
			auto &entry = *lookup.entry;

			lookup = LookupEntry(dependent_p, catalog, transaction);
			D_ASSERT(lookup.entry);
			auto &dependent = *lookup.entry;
			callback(entry, dependent, dependent_p.dependency_type);
		}
	}
}

void DependencyManager::AddOwnership(CatalogTransaction transaction, CatalogEntry &owner_p, CatalogEntry &entry) {
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.GetWriteLock());

	// If the owner is already owned by something else, throw an error
	for (auto &dep : dependents_map[owner_p]) {
		if (dep.dependency_type == DependencyType::DEPENDENCY_OWNED_BY) {
			throw DependencyException(owner_p.name + " already owned by " + dep.entry.name);
		}
	}

	// If the entry is already owned, throw an error
	auto owner = Dependency(owner_p);
	for (auto &dep : dependents_map[entry]) {
		// if the entry is already owned, throw error
		if (dep.entry != owner.entry) {
			throw DependencyException(entry.name + " already depends on " + dep.entry.name);
		}
		// if the entry owns the owner, throw error
		if (dep.entry == owner.entry && dep.dependency_type == DependencyType::DEPENDENCY_OWNS) {
			throw DependencyException(entry.name + " already owns " + owner.entry.name +
			                          ". Cannot have circular dependencies");
		}
	}

	// Emplace guarantees that the same object cannot be inserted twice in the unordered_set
	// In the case AddOwnership is called twice, because of emplace, the object will not be repeated in the set.
	// We use an automatic dependency because if the Owner gets deleted, then the owned objects are also deleted
	dependents_map[owner].emplace(entry, DependencyType::DEPENDENCY_OWNS);
	dependents_map[entry].emplace(owner.entry, DependencyType::DEPENDENCY_OWNED_BY);
	dependencies_map[owner].emplace(entry);
}

} // namespace duckdb
