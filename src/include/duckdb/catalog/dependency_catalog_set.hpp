#pragma once

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/dependency.hpp"

namespace duckdb {

struct MangledEntryName {
public:
	explicit MangledEntryName(const CatalogEntryInfo &info);
	MangledEntryName() = delete;

public:
	//! Format: Type\0Schema\0Name
	Identifier name;

public:
	bool operator==(const MangledEntryName &other) const {
		return other.name == name;
	}
	bool operator!=(const MangledEntryName &other) const {
		return !(*this == other);
	}
};

struct MangledDependencyName {
public:
	MangledDependencyName(const MangledEntryName &from, const MangledEntryName &to);
	MangledDependencyName() = delete;

public:
	//! Format: MangledEntryName\0MangledEntryName
	Identifier name;
};

//! This class mocks the CatalogSet interface, but does not actually store CatalogEntries
class DependencyCatalogSet {
public:
	DependencyCatalogSet(CatalogSet &set, const CatalogEntryInfo &info)
	    : set(set), info(info), mangled_name(MangledEntryName(info)) {
	}

public:
	bool CreateEntry(CatalogTransaction transaction, const MangledEntryName &name, unique_ptr<CatalogEntry> value);
	CatalogSet::EntryLookup GetEntryDetailed(CatalogTransaction transaction, const MangledEntryName &name);
	optional_ptr<CatalogEntry> GetEntry(CatalogTransaction transaction, const MangledEntryName &name);
	void Scan(CatalogTransaction transaction, const std::function<void(CatalogEntry &)> &callback);
	bool DropEntry(CatalogTransaction transaction, const MangledEntryName &name, bool cascade,
	               bool allow_drop_internal = false);

private:
	MangledDependencyName ApplyPrefix(const MangledEntryName &name) const;

public:
	CatalogSet &set;
	CatalogEntryInfo info;
	MangledEntryName mangled_name;
};

} // namespace duckdb
