#pragma once

#include "duckdb/main/client_context_state.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace duckdb {

class ReplacementCache {
	using create_replacement_t = std::function<unique_ptr<TableRef>(void)>;

public:
	ReplacementCache();

public:
	//! Look up the cache item, null if not present
	unique_ptr<TableRef> Lookup(const string &name);
	//! Add the item to the cache
	void Add(const string &name, unique_ptr<TableRef> result);
	//! Throw away our replacement cache
	void Evict();

public:
	case_insensitive_map_t<unique_ptr<TableRef>> cache;
};

class RecordBatchReaderRegistry {
public:
	RecordBatchReaderRegistry();

public:
	void AddRecordBatchReader(PyObject *record_batch_reader);

public:
	//! Scanned record batch readers
	unordered_set<PyObject *> consumed;
};

class PythonContextState : public ClientContextState {
public:
	PythonContextState();
	~PythonContextState() override;

public:
	void QueryEnd(ClientContext &context) override;

public:
	//! Cache the replacement scan lookups
	ReplacementCache cache;
	//! Used to keep track of which record batch readers have been consumed
	// FIXME: this should probably be global, nothing is protecting multiple connections from using the same record
	// batch reader
	RecordBatchReaderRegistry registry;
};

} // namespace duckdb
