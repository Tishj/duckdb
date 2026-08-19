#pragma once

#include "duckdb/common/common.hpp"

#include <cstring>
#include <typeinfo>

namespace duckdb {

struct DUCKDB_API TransformResultValue {
	virtual ~TransformResultValue() = default;
	//! RTTI identities are not stable across loadable extension binaries, so compare the type name in the producer.
	virtual void *GetValuePointer(const char *type_name) = 0;
};

template <class T>
struct DUCKDB_API TypedTransformResult : public TransformResultValue {
	explicit TypedTransformResult(T value_p) : value(std::move(value_p)) {
	}

	void *GetValuePointer(const char *type_name) override {
		return strcmp(typeid(T).name(), type_name) == 0 ? &value : nullptr;
	}

	T value;
};

template <class T>
T *TryGetTransformResult(TransformResultValue &result) {
	return reinterpret_cast<T *>(result.GetValuePointer(typeid(T).name()));
}

} // namespace duckdb
