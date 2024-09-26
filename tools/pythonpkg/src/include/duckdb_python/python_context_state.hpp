//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/python_context_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/main/client_context_state.hpp"

namespace duckdb {

class PythonContextState : public ClientContextState {
public:
	PythonContextState() {
	}
	~PythonContextState() {
	}

public:
	bool HasUserProvidedDict() const;
	void SetUserProvidedDict(py::dict object_dict);
	void ClearUserProvidedDict();
	py::object LookupObject(const string &name);

private:
	//! The objects provided by the user to scan
	py::object object_dict;
};

} // namespace duckdb
