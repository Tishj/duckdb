#include "duckdb_python/map.hpp"
#include "duckdb_python/vector_conversion.hpp"
#include "duckdb_python/array_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb_python/pyconnection.hpp"

namespace duckdb {

PythonFunction::PythonFunction(table_function_bind_t bind, table_in_out_function_t func)
    : TableFunction("python_map_function", {LogicalType::TABLE, LogicalType::POINTER}, nullptr, bind) {
	in_out_function = func;
}

py::handle PythonFunction::FunctionCall(const py::object &input, PyObject *function) {
	// Call the function, providing the object as argument
	D_ASSERT(function);
	auto *object_ptr = PyObject_CallObject(function, PyTuple_Pack(1, input.ptr()));
	if (!object_ptr) {
		PyErr_PrintEx(1);
		throw InvalidInputException("Python error. See above for a stack trace.");
	}

	py::handle object(object_ptr);
	if (object.is_none()) { // no return, probably modified in place
		throw InvalidInputException("No return value from Python function");
	}

	// Return the python object
	return object;
}

string PythonFunction::TypeVectorToString(vector<LogicalType> &types) {
	return StringUtil::Join(types, types.size(), ", ", [](const LogicalType &argument) { return argument.ToString(); });
}

} // namespace duckdb
