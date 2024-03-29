#include "duckdb_python/python_context_state.hpp"

namespace duckdb {

RecordBatchReaderRegistry::RecordBatchReaderRegistry() {
}

void RecordBatchReaderRegistry::AddRecordBatchReader(PyObject *record_batch_reader) {
	// FIXME: we should differentiate between replacement scanned and consumed
	// a plan could be canceled, making it so the reader was never consumed
	auto result = consumed.insert(record_batch_reader).second;
	if (!result) {
		throw InvalidInputException("Attempted to read from the same RecordBatchReader more than once!");
	}
}

// Replacement Cache

ReplacementCache::ReplacementCache() {
}

unique_ptr<TableRef> ReplacementCache::Lookup(const string &name) {
	auto it = cache.find(name);
	if (it != cache.end()) {
		return it->second->Copy();
	}
	return nullptr;
}

void ReplacementCache::Add(const string &name, unique_ptr<TableRef> result) {
	D_ASSERT(result);
	cache.emplace(std::make_pair(name, std::move(result)));
}

void ReplacementCache::Evict() {
	cache.clear();
}

// Client Context State

PythonContextState::PythonContextState() {
}
PythonContextState::~PythonContextState() {
}

void PythonContextState::QueryEnd(ClientContext &context) {
	cache.Evict();
}

} // namespace duckdb
