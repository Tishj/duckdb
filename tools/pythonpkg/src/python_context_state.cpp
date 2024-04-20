#include "duckdb_python/python_context_state.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

// Override

ReplacementCacheOverride::ReplacementCacheOverride(case_insensitive_map_t<unique_ptr<TableRef>> cache)
    : cache(std::move(cache)) {
}

unique_ptr<TableRef> ReplacementCacheOverride::Lookup(const string &name) {
	auto it = cache.find(name);
	if (it == cache.end()) {
		return nullptr;
	}
	return it->second->Copy();
}

// Replacement Cache

ReplacementCache::ReplacementCache() {
}

unique_ptr<TableRef> ReplacementCache::Lookup(const string &name) {
	D_ASSERT(overrides.empty());
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

void ReplacementCache::AddOverride(shared_ptr<ReplacementCacheOverride> override) {
	overrides.push(std::move(override));
}

void ReplacementCache::RemoveOverride(shared_ptr<ReplacementCacheOverride> override) {
	D_ASSERT(!overrides.empty());
	auto &top = overrides.top();
	D_ASSERT(top.get() == override.get());
	overrides.pop();
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

PythonContextState &PythonContextState::GetState(ClientContext &context) {
	D_ASSERT(context.registered_state.count("python_state"));
	return dynamic_cast<PythonContextState &>(*context.registered_state.at("python_state"));
}

} // namespace duckdb
