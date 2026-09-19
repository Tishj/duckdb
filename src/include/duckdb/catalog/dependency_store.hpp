//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dependency_store.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/catalog_entry_map.hpp"
#include "duckdb/catalog/dependency.hpp"

namespace duckdb {
class DuckCatalog;
enum class DependencyEntryType : uint8_t;

//! Stores each relationship in two transactional catalog sets, one per endpoint.
class DependencyStore {
public:
	explicit DependencyStore(DuckCatalog &catalog);

	//! The manager resolves the subject and supplies its OID before merging.
	void MergeDependency(CatalogTransaction transaction, DependencyInfo info);
	void RemoveDependency(CatalogTransaction transaction, const DependencyInfo &info);

	using scan_callback_t = std::function<void(const DependencyInfo &, transaction_t)>;
	//! Callbacks run under the scanned set's lock and receive that row's timestamp.
	void Scan(CatalogTransaction transaction, const scan_callback_t &callback);
	//! Both directional scans return the same dependent-to-subject orientation.
	void ScanDependenciesOf(CatalogTransaction transaction, const CatalogEntryInfo &info,
	                        const scan_callback_t &callback);
	void ScanDependentsOf(CatalogTransaction transaction, const CatalogEntryInfo &info,
	                      const scan_callback_t &callback);

private:
	void CreateSubject(CatalogTransaction transaction, const DependencyInfo &info);
	void CreateDependent(CatalogTransaction transaction, const DependencyInfo &info);
	void ScanSetInternal(CatalogTransaction transaction, const CatalogEntryInfo &info, DependencyEntryType side,
	                     const scan_callback_t &callback);
	void VerifyMirrors(CatalogTransaction transaction, const catalog_entry_set_t &entries);

private:
	DuckCatalog &catalog;
	CatalogSet subjects;
	CatalogSet dependents;
};

} // namespace duckdb
