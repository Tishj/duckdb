#include "duckdb/catalog/dependency_store.hpp"
#include "duckdb/catalog/dependency_catalog_set.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry/dependency/dependency_subject_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency/dependency_dependent_entry.hpp"

namespace duckdb {

static void AssertMangledName(const string &mangled_name, idx_t expected_null_bytes) {
#ifdef DEBUG
	idx_t nullbyte_count = 0;
	for (auto &ch : mangled_name) {
		nullbyte_count += ch == '\0';
	}
	D_ASSERT(nullbyte_count == expected_null_bytes);
#endif
}

MangledEntryName::MangledEntryName(const CatalogEntryInfo &info) {
	auto &type = info.type;
	auto &schema_path = info.schema_path;
	auto &name = info.name;
	auto &table = info.table;

	// Format: Type\0[Schema\0 for each containing schema]Name[\0Table] - the schema path is null-separated so distinct
	// produce distinct keys (SQL identifiers cannot contain null bytes).
	string mangled = CatalogTypeToString(type) + '\0';
	for (auto &schema : schema_path) {
		mangled += schema.GetIdentifierName() + '\0';
	}
	mangled += name;
	idx_t expected_null_bytes = 1 + schema_path.size();
	if (!table.empty()) {
		mangled += '\0' + table.GetIdentifierName();
		expected_null_bytes++;
	}
	this->name = Identifier(mangled);
	AssertMangledName(this->name.GetIdentifierName(), expected_null_bytes);
}

MangledDependencyName::MangledDependencyName(const MangledEntryName &from, const MangledEntryName &to) {
	this->name = Identifier(from.name + '\0' + to.name);
#ifdef DEBUG
	auto count_nulls = [](const Identifier &id) {
		idx_t count = 0;
		for (auto ch : id.GetIdentifierName()) {
			count += ch == '\0';
		}
		return count;
	};
	// the two mangled entry names (each Type\0[Schema\0...]Name) joined by a separator null byte
	AssertMangledName(this->name.GetIdentifierName(), count_nulls(from.name) + count_nulls(to.name) + 1);
#endif
}

DependencyStore::DependencyStore(DuckCatalog &catalog) : catalog(catalog), subjects(catalog), dependents(catalog) {
}

void DependencyStore::RemoveDependency(CatalogTransaction transaction, const DependencyInfo &info) {
	auto &dependent = info.dependent;
	auto &subject = info.subject;

	// The dependents of the dependency (target)
	DependencyCatalogSet dependents(this->dependents, subject.entry);
	// The subjects of the dependencies of the dependent
	DependencyCatalogSet subjects(this->subjects, dependent.entry);

	auto dependent_mangled = MangledEntryName(dependent.entry);
	auto subject_mangled = MangledEntryName(subject.entry);

	auto dependent_p = dependents.GetEntry(transaction, dependent_mangled);
	if (dependent_p) {
		// 'dependent' is no longer inhibiting the deletion of 'dependency'
		dependents.DropEntry(transaction, dependent_mangled, false);
	}
	auto subject_p = subjects.GetEntry(transaction, subject_mangled);
	if (subject_p) {
		// 'dependency' is no longer required by 'dependent'
		subjects.DropEntry(transaction, subject_mangled, false);
	}
}

void DependencyStore::CreateSubject(CatalogTransaction transaction, const DependencyInfo &info) {
	auto &from = info.dependent.entry;

	DependencyCatalogSet set(this->subjects, from);
	auto dep = make_uniq_base<DependencyEntry, DependencySubjectEntry>(catalog, info);
	auto entry_name = dep->EntryMangledName();

	//! Add to the list of objects that 'dependent' has a dependency on
	set.CreateEntry(transaction, entry_name, std::move(dep));
}

void DependencyStore::CreateDependent(CatalogTransaction transaction, const DependencyInfo &info) {
	auto &from = info.subject.entry;

	DependencyCatalogSet set(this->dependents, from);
	auto dep = make_uniq_base<DependencyEntry, DependencyDependentEntry>(catalog, info);
	auto entry_name = dep->EntryMangledName();

	//! Add to the list of object that depend on 'subject'
	set.CreateEntry(transaction, entry_name, std::move(dep));
}

void DependencyStore::MergeDependency(CatalogTransaction transaction, DependencyInfo info) {
	D_ASSERT(info.subject.oid.IsValid());
	DependencyCatalogSet subjects(this->subjects, info.dependent.entry);
	DependencyCatalogSet dependents(this->dependents, info.subject.entry);

	auto subject_mangled = MangledEntryName(info.subject.entry);
	auto dependent_mangled = MangledEntryName(info.dependent.entry);

	auto &dependent_flags = info.dependent.flags;
	auto &subject_flags = info.subject.flags;

	auto existing_subject = subjects.GetEntry(transaction, subject_mangled);
	auto existing_dependent = dependents.GetEntry(transaction, dependent_mangled);

	// Inherit the existing flags and drop the existing entry if present
	if (existing_subject) {
		auto &existing = existing_subject->Cast<DependencyEntry>();
		auto existing_flags = existing.Subject().flags;
		if (existing_flags != subject_flags) {
			subject_flags.Apply(existing_flags);
		}
		subjects.DropEntry(transaction, subject_mangled, false, false);
	}
	if (existing_dependent) {
		auto &existing = existing_dependent->Cast<DependencyEntry>();
		auto existing_flags = existing.Dependent().flags;
		if (existing_flags != dependent_flags) {
			dependent_flags.Apply(existing_flags);
		}
		dependents.DropEntry(transaction, dependent_mangled, false, false);
	}

	// Create an entry in the dependents map of the object that is the target of the dependency
	CreateDependent(transaction, info);
	// Create an entry in the subjects map of the object that is targeting another entry
	CreateSubject(transaction, info);
}

void DependencyStore::Scan(CatalogTransaction transaction, const scan_callback_t &callback) {
	dependents.Scan(transaction, [&](CatalogEntry &entry) {
		auto &dependency = entry.Cast<DependencyEntry>();
		callback(dependency.GetDependencyInfo(), dependency.timestamp.load());
	});
}

void DependencyStore::ScanDependenciesOf(CatalogTransaction transaction, const CatalogEntryInfo &info,
                                         const scan_callback_t &callback) {
	ScanSetInternal(transaction, info, DependencyEntryType::SUBJECT, callback);
}

void DependencyStore::ScanDependentsOf(CatalogTransaction transaction, const CatalogEntryInfo &info,
                                       const scan_callback_t &callback) {
	ScanSetInternal(transaction, info, DependencyEntryType::DEPENDENT, callback);
}

void DependencyStore::ScanSetInternal(CatalogTransaction transaction, const CatalogEntryInfo &info,
                                      DependencyEntryType side, const scan_callback_t &callback) {
	catalog_entry_set_t entries;
	auto &set = side == DependencyEntryType::SUBJECT ? subjects : dependents;
	DependencyCatalogSet dependencies(set, info);
	dependencies.Scan(transaction, [&](CatalogEntry &entry) {
		auto &dependency = entry.Cast<DependencyEntry>();
		D_ASSERT(dependency.Side() == side);
		entries.insert(dependency);
		callback(dependency.GetDependencyInfo(), dependency.timestamp.load());
	});
	// Release the scanned set's lock before looking up the opposite rows.
	VerifyMirrors(transaction, entries);
}

void DependencyStore::VerifyMirrors(CatalogTransaction transaction, const catalog_entry_set_t &entries) {
#ifdef DEBUG
	for (auto &entry : entries) {
		auto &dependency = entry.get().Cast<DependencyEntry>();
		auto &other_set = dependency.Side() == DependencyEntryType::SUBJECT ? dependents : subjects;
		DependencyCatalogSet mirrors(other_set, dependency.EntryInfo());
		auto mirror = mirrors.GetEntryDetailed(transaction, dependency.SourceMangledName());
		D_ASSERT(mirror.reason != CatalogSet::EntryLookup::FailureReason::NOT_PRESENT);
		// Commit validation can see only one row; compare payloads only for matching visible versions.
		if (!mirror.result || mirror.result->timestamp.load() != dependency.timestamp.load()) {
			continue;
		}
		auto &other = mirror.result->Cast<DependencyEntry>();
		D_ASSERT(other.Side() != dependency.Side());
		D_ASSERT(other.Dependent().entry == dependency.Dependent().entry);
		D_ASSERT(other.Subject().entry == dependency.Subject().entry);
		D_ASSERT(other.Dependent().flags == dependency.Dependent().flags);
		D_ASSERT(other.Subject().flags == dependency.Subject().flags);
		D_ASSERT(other.Subject().oid == dependency.Subject().oid);
	}
#endif
}

} // namespace duckdb
