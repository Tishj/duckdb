#include "duckdb_python/python_context_state.hpp"

namespace duckdb {

bool PythonContextState::HasUserProvidedDict() const {
	return !py::none().is(object_dict);
}

void PythonContextState::SetUserProvidedDict(py::dict object_dict_p) {
	object_dict = object_dict_p;
}

py::object PythonContextState::LookupObject(const string &name) {
	auto dict = py::reinterpret_borrow<py::dict>(object_dict);
	if (!dict.contains(name)) {
		return py::none();
	}
	return dict[name.c_str()];
}

void PythonContextState::ClearUserProvidedDict() {
	auto dict = py::reinterpret_borrow<py::dict>(object_dict);
	dict.clear();
}

} // namespace duckdb
