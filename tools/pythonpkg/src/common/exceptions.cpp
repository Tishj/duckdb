#include "duckdb_python/pybind11/exceptions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/list.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace py = pybind11;

namespace duckdb {

class Warning : public std::exception {};

// This is the error structure defined in the DBAPI spec
// StandardError
// |__ Warning
// |__ Error
//    |__ InterfaceError
//    |__ DatabaseError
//       |__ DataError
//       |__ OperationalError
//       |__ IntegrityError
//       |__ InternalError
//       |__ ProgrammingError
//       |__ NotSupportedError
//===--------------------------------------------------------------------===//
// Base Error
//===--------------------------------------------------------------------===//
class PyError : public std::exception {
public:
	explicit PyError(ErrorData &&error) : std::exception(), error(std::move(error)) {
	}
	const char *what() const noexcept override {
		return error.Message().c_str();
	}

private:
	ErrorData error;
};

class DatabaseError : public PyError {
public:
	explicit DatabaseError(ErrorData &&err) : PyError(std::move(err)) {
	}
};

//===--------------------------------------------------------------------===//
// Unknown Errors
//===--------------------------------------------------------------------===//
class PyFatalException : public DatabaseError {
public:
	explicit PyFatalException(ErrorData &&err) : DatabaseError(std::move(err)) {
	}
};

class PyInterruptException : public DatabaseError {
public:
	explicit PyInterruptException(ErrorData &&err) : DatabaseError(std::move(err)) {
	}
};

class PyPermissionException : public DatabaseError {
public:
	explicit PyPermissionException(ErrorData &&err) : DatabaseError(std::move(err)) {
	}
};

class PySequenceException : public DatabaseError {
public:
	explicit PySequenceException(ErrorData &&err) : DatabaseError(std::move(err)) {
	}
};

class PyDependencyException : public DatabaseError {
public:
	explicit PyDependencyException(ErrorData &&err) : DatabaseError(std::move(err)) {
	}
};

//===--------------------------------------------------------------------===//
// Data Error
//===--------------------------------------------------------------------===//
class DataError : public DatabaseError {
public:
	explicit DataError(ErrorData &&err) : DatabaseError(std::move(err)) {
	}
};

class PyOutOfRangeException : public DataError {
public:
	explicit PyOutOfRangeException(ErrorData &&err) : DataError(std::move(err)) {
	}
};

class PyConversionException : public DataError {
public:
	explicit PyConversionException(ErrorData &&err) : DataError(std::move(err)) {
	}
};

class PyTypeMismatchException : public DataError {
public:
	explicit PyTypeMismatchException(ErrorData &&err) : DataError(std::move(err)) {
	}
};

//===--------------------------------------------------------------------===//
// Operational Error
//===--------------------------------------------------------------------===//
class OperationalError : public DatabaseError {
public:
	explicit OperationalError(ErrorData &&err) : DatabaseError(std::move(err)) {
	}
};

class PyTransactionException : public OperationalError {
public:
	explicit PyTransactionException(ErrorData &&err) : OperationalError(std::move(err)) {
	}
};

class PyOutOfMemoryException : public OperationalError {
public:
	explicit PyOutOfMemoryException(ErrorData &&err) : OperationalError(std::move(err)) {
	}
};

class PyConnectionException : public OperationalError {
public:
	explicit PyConnectionException(ErrorData &&err) : OperationalError(std::move(err)) {
	}
};

class PySerializationException : public OperationalError {
public:
	explicit PySerializationException(ErrorData &&err) : OperationalError(std::move(err)) {
	}
};

class PyIOException : public OperationalError {
public:
	explicit PyIOException(ErrorData &&err) : OperationalError(std::move(err)) {
	}
};

class PyHTTPException : public PyIOException {
public:
	explicit PyHTTPException(ErrorData &&err) : PyIOException(std::move(err)) {
	}
};

//===--------------------------------------------------------------------===//
// Integrity Error
//===--------------------------------------------------------------------===//
class IntegrityError : public DatabaseError {
public:
	explicit IntegrityError(ErrorData &&err) : DatabaseError(std::move(err)) {
	}
};

class PyConstraintException : public IntegrityError {
public:
	explicit PyConstraintException(ErrorData &&err) : IntegrityError(std::move(err)) {
	}
};

//===--------------------------------------------------------------------===//
// Internal Error
//===--------------------------------------------------------------------===//
class InternalError : public DatabaseError {
public:
	explicit InternalError(ErrorData &&err) : DatabaseError(std::move(err)) {
	}
};

class PyInternalException : public InternalError {
public:
	explicit PyInternalException(ErrorData &&err) : InternalError(std::move(err)) {
	}
};

//===--------------------------------------------------------------------===//
// Programming Error
//===--------------------------------------------------------------------===//
class ProgrammingError : public DatabaseError {
public:
	explicit ProgrammingError(ErrorData &&err) : DatabaseError(std::move(err)) {
	}
};

class PyParserException : public ProgrammingError {
public:
	explicit PyParserException(ErrorData &&err) : ProgrammingError(std::move(err)) {
	}
};

class PySyntaxException : public ProgrammingError {
public:
	explicit PySyntaxException(ErrorData &&err) : ProgrammingError(std::move(err)) {
	}
};

class PyBinderException : public ProgrammingError {
public:
	explicit PyBinderException(ErrorData &&err) : ProgrammingError(std::move(err)) {
	}
};

class PyInvalidInputException : public ProgrammingError {
public:
	explicit PyInvalidInputException(ErrorData &&err) : ProgrammingError(std::move(err)) {
	}
};

class PyInvalidTypeException : public ProgrammingError {
public:
	explicit PyInvalidTypeException(ErrorData &&err) : ProgrammingError(std::move(err)) {
	}
};

class PyCatalogException : public ProgrammingError {
public:
	explicit PyCatalogException(ErrorData &&err) : ProgrammingError(std::move(err)) {
	}
};

//===--------------------------------------------------------------------===//
// Not Supported Error
//===--------------------------------------------------------------------===//
class NotSupportedError : public DatabaseError {
public:
	explicit NotSupportedError(ErrorData &&err) : DatabaseError(std::move(err)) {
	}
};

class PyNotImplementedException : public NotSupportedError {
public:
	explicit PyNotImplementedException(ErrorData &&err) : NotSupportedError(std::move(err)) {
	}
};

//===--------------------------------------------------------------------===//
// PyThrowException
//===--------------------------------------------------------------------===//
void PyThrowException(ErrorData &&error, PyObject *http_exception_type) {
	switch (error.Type()) {
	case ExceptionType::HTTP: {
		// construct exception object
		//! FIXME: this can't be constructed with the 'std::move(error)' because this can not go through Python.
		auto py_http_exception = py::handle(http_exception_type)();

		auto headers = py::dict();
		for (auto &entry : error.ExtraInfo()) {
			if (entry.first == "status_code") {
				py_http_exception.attr("status_code") = std::stoi(entry.second);
			} else if (entry.first == "response_body") {
				py_http_exception.attr("body") = entry.second;
			} else if (entry.first == "reason") {
				py_http_exception.attr("reason") = entry.second;
			} else if (StringUtil::StartsWith(entry.first, "header_")) {
				headers[py::str(entry.first.substr(7))] = entry.second;
			}
		}
		py_http_exception.attr("headers") = std::move(headers);
		const auto string_type = py::type::of(py::str());
		const auto dict_type = py::module_::import("typing").attr("Dict");
		py_http_exception.attr("__annotations__") = py::dict(
		    py::arg("status_code") = py::type::of(py::int_()), py::arg("body") = string_type,
		    py::arg("reason") = string_type, py::arg("headers") = dict_type[py::make_tuple(string_type, string_type)]);
		py_http_exception.doc() =
		    "Thrown when an error occurs in the httpfs extension, or whilst downloading an extension.";
		PyErr_SetObject(py_http_exception.get_type().ptr(), py_http_exception.ptr());
		throw py::error_already_set();
	}
	case ExceptionType::CATALOG:
		throw PyCatalogException(std::move(error));
	case ExceptionType::FATAL:
		throw PyFatalException(std::move(error));
	case ExceptionType::INTERRUPT:
		throw PyInterruptException(std::move(error));
	case ExceptionType::PERMISSION:
		throw PyPermissionException(std::move(error));
	case ExceptionType::SEQUENCE:
		throw PySequenceException(std::move(error));
	case ExceptionType::DEPENDENCY:
		throw PyDependencyException(std::move(error));
	case ExceptionType::OUT_OF_RANGE:
		throw PyOutOfRangeException(std::move(error));
	case ExceptionType::CONVERSION:
		throw PyConversionException(std::move(error));
	case ExceptionType::MISMATCH_TYPE:
		throw PyTypeMismatchException(std::move(error));
	case ExceptionType::TRANSACTION:
		throw PyTransactionException(std::move(error));
	case ExceptionType::OUT_OF_MEMORY:
		throw PyOutOfMemoryException(std::move(error));
	case ExceptionType::CONNECTION:
		throw PyConnectionException(std::move(error));
	case ExceptionType::SERIALIZATION:
		throw PySerializationException(std::move(error));
	case ExceptionType::CONSTRAINT:
		throw PyConstraintException(std::move(error));
	case ExceptionType::INTERNAL:
		throw PyInternalException(std::move(error));
	case ExceptionType::PARSER:
		throw PyParserException(std::move(error));
	case ExceptionType::SYNTAX:
		throw PySyntaxException(std::move(error));
	case ExceptionType::IO:
		throw PyIOException(std::move(error));
	case ExceptionType::BINDER:
		throw PyBinderException(std::move(error));
	case ExceptionType::INVALID_INPUT:
		throw PyInvalidInputException(std::move(error));
	case ExceptionType::INVALID_TYPE:
		throw PyInvalidTypeException(std::move(error));
	case ExceptionType::NOT_IMPLEMENTED:
		throw PyNotImplementedException(std::move(error));
	default:
		throw PyError(std::move(error));
	}
}

/**
 * @see https://peps.python.org/pep-0249/#exceptions
 */
void RegisterExceptions(const py::module &m) {
	// The base class is mapped to Error in python to somewhat match the DBAPI 2.0 specifications
	py::register_exception<Warning>(m, "Warning");
	auto error = py::register_exception<PyError>(m, "Error").ptr();
	auto db_error = py::register_exception<DatabaseError>(m, "DatabaseError", error).ptr();

	// order of declaration matters, and this needs to be checked last
	// Unknown
	py::register_exception<PyFatalException>(m, "FatalException", db_error);
	py::register_exception<PyInterruptException>(m, "InterruptException", db_error);
	py::register_exception<PyPermissionException>(m, "PermissionException", db_error);
	py::register_exception<PySequenceException>(m, "SequenceException", db_error);
	py::register_exception<PyDependencyException>(m, "DependencyException", db_error);

	// DataError
	auto data_error = py::register_exception<DataError>(m, "DataError", db_error).ptr();
	py::register_exception<PyOutOfRangeException>(m, "OutOfRangeException", data_error);
	py::register_exception<PyConversionException>(m, "ConversionException", data_error);
	// no unknown type error, or decimal type
	py::register_exception<PyTypeMismatchException>(m, "TypeMismatchException", data_error);

	// OperationalError
	auto operational_error = py::register_exception<OperationalError>(m, "OperationalError", db_error).ptr();
	py::register_exception<PyTransactionException>(m, "TransactionException", operational_error);
	py::register_exception<PyOutOfMemoryException>(m, "OutOfMemoryException", operational_error);
	py::register_exception<PyConnectionException>(m, "ConnectionException", operational_error);
	// no object size error
	// no null pointer errors
	auto io_exception = py::register_exception<PyIOException>(m, "IOException", operational_error).ptr();
	py::register_exception<PySerializationException>(m, "SerializationException", operational_error);

	PYBIND11_CONSTINIT static py::gil_safe_call_once_and_store<py::object> http_exception_type_storage;
	http_exception_type_storage.call_once_and_store_result(
	    [&]() { return py::exception<PyHTTPException>(m, "HTTPException", io_exception); });

	static auto &http_exception_type = http_exception_type_storage.get_stored();
	const auto string_type = py::type::of(py::str());
	const auto Dict = py::module_::import("typing").attr("Dict");
	http_exception_type.attr("__annotations__") =
	    py::dict(py::arg("status_code") = py::type::of(py::int_()), py::arg("body") = string_type,
	             py::arg("reason") = string_type, py::arg("headers") = Dict[py::make_tuple(string_type, string_type)]);
	http_exception_type.doc() =
	    "Thrown when an error occurs in the httpfs extension, or whilst downloading an extension.";

	// IntegrityError
	auto integrity_error = py::register_exception<IntegrityError>(m, "IntegrityError", db_error).ptr();
	py::register_exception<PyConstraintException>(m, "ConstraintException", integrity_error);

	// InternalError
	auto internal_error = py::register_exception<InternalError>(m, "InternalError", db_error).ptr();
	py::register_exception<PyInternalException>(m, "InternalException", internal_error);

	//// ProgrammingError
	auto programming_error = py::register_exception<ProgrammingError>(m, "ProgrammingError", db_error).ptr();
	py::register_exception<PyParserException>(m, "ParserException", programming_error);
	py::register_exception<PySyntaxException>(m, "SyntaxException", programming_error);
	py::register_exception<PyBinderException>(m, "BinderException", programming_error);
	py::register_exception<PyInvalidInputException>(m, "InvalidInputException", programming_error);
	py::register_exception<PyInvalidTypeException>(m, "InvalidTypeException", programming_error);
	// no type for expression exceptions?
	py::register_exception<PyCatalogException>(m, "CatalogException", programming_error);

	// NotSupportedError
	auto not_supported_error = py::register_exception<NotSupportedError>(m, "NotSupportedError", db_error).ptr();
	py::register_exception<PyNotImplementedException>(m, "NotImplementedException", not_supported_error);

	py::register_exception_translator([](std::exception_ptr p) { // NOLINT(performance-unnecessary-value-param)
		try {
			if (p) {
				std::rethrow_exception(p);
			}
		} catch (const duckdb::Exception &ex) {
			duckdb::ErrorData error(ex);
			PyThrowException(std::move(error), http_exception_type.ptr());
		} catch (const py::builtin_exception &ex) {
			// These represent Python exceptions, we don't want to catch these
			throw;
		} catch (const std::exception &ex) {
			duckdb::ErrorData error(ex);
			if (error.Type() == ExceptionType::INVALID) {
				// we need to pass non-DuckDB exceptions through as-is
				throw;
			}
			PyThrowException(std::move(error), http_exception_type.ptr());
		}
	});
}
} // namespace duckdb
