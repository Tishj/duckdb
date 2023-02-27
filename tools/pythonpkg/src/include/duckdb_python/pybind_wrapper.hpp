//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pybind_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <vector>
#include "duckdb/common/assert.hpp"

#define PYBIND11_DETAILED_ERROR_MESSAGES

namespace py = pybind11;

namespace PYBIND11_NAMESPACE {
namespace detail {
template <>
struct type_caster<std::string> {
public:
	PYBIND11_TYPE_CASTER(std::string, const_name("string"));

	bool load(handle src, bool implicit) {
		if (pybind11::none().is(src)) {
			return false;
		}
		if (!implicit && !pybind11::isinstance(src, pybind11::module_::import("pathlib").attr("Path")) &&
		    !pybind11::isinstance<py::str>(src) && !pybind11::isinstance<py::bytes>(src)) {
			return false;
		}
		value = py::str(src);
		return true;
	}

	static handle cast(const std::string &src, return_value_policy policy, handle parent) {
		return PyUnicode_FromStringAndSize(src.data(), src.size());
	}
};
} // namespace detail
} // namespace PYBIND11_NAMESPACE

namespace pybind11 {

bool gil_check();
void gil_assert();

} // namespace pybind11

namespace duckdb {
#ifdef __GNUG__
#define PYBIND11_NAMESPACE pybind11 __attribute__((visibility("hidden")))
#else
#define PYBIND11_NAMESPACE pybind11
#endif
namespace py = pybind11;

template <class T, typename... ARGS>
void DefineMethod(std::vector<const char *> aliases, T &mod, ARGS &&... args) {
	for (auto &alias : aliases) {
		mod.def(alias, args...);
	}
}

} // namespace duckdb
