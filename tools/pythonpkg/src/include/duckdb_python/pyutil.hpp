#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/types.hpp"
#include <unistd.h>

namespace duckdb {

enum class PyUnicodeType : uint8_t { ASCII = 0, ONE_BYTE = 1, TWO_BYTE = 2, FOUR_BYTE = 3 };

struct PyUnicodeStringData {
	uint32_t count;
	PyUnicodeType type;
};

struct PyUtil {
	static idx_t PyByteArrayGetSize(PyObject *obj) {
		return PyByteArray_GET_SIZE(obj); // NOLINT
	}

	static Py_buffer *PyMemoryViewGetBuffer(PyObject *obj) {
		return PyMemoryView_GET_BUFFER(obj);
	}

	static bool PyUnicodeIsCompactASCII(PyObject *obj) {
		return PyUnicode_IS_COMPACT_ASCII(obj);
	}

	static const char *PyUnicodeData(PyObject *obj) {
		return const_char_ptr_cast(PyUnicode_DATA(obj));
	}

	static char *PyUnicodeDataMutable(PyObject *obj) {
		return char_ptr_cast(PyUnicode_DATA(obj));
	}

	static idx_t PyUnicodeGetLength(PyObject *obj) {
		return PyUnicode_GET_LENGTH(obj);
	}

	static bool PyUnicodeIsCompact(PyCompactUnicodeObject *obj) {
		return PyUnicode_IS_COMPACT(obj);
	}

	static bool PyUnicodeIsASCII(PyCompactUnicodeObject *obj) {
		return PyUnicode_IS_ASCII(obj);
	}

	static PyUnicodeStringData AnalyzeUnicodeString(const uint8_t *s, uint32_t len) {
		uint32_t ascii_codepoints = 0;
		uint32_t one_byte_codepoints = 0;
		uint32_t two_byte_codepoints = 0;
		uint32_t three_byte_codepoints = 0;
		uint32_t four_byte_codepoints = 0;
		for (uint32_t i = 0; i < len; i++) {
			ascii_codepoints += (s[i] & 0x80) == 0;
			// 2Byte Unicode, but only up to 1 byte (255)
			one_byte_codepoints += (s[i] & 0xFC) == 0xC0;
			two_byte_codepoints += (s[i] & 0xE0) == 0xC0 && (s[i] & 0xFC) > 0xC0;
			three_byte_codepoints += (s[i] & 0xF0) == 0xE0;
			four_byte_codepoints += (s[i] & 0xF8) == 0xF0;
		}
		PyUnicodeStringData data;
		if (four_byte_codepoints > 0) {
			data.type = PyUnicodeType::FOUR_BYTE;
		} else if (three_byte_codepoints > 0) {
			data.type = PyUnicodeType::TWO_BYTE;
		} else if (two_byte_codepoints > 0) {
			data.type = PyUnicodeType::TWO_BYTE;
		} else if (one_byte_codepoints > 0) {
			data.type = PyUnicodeType::ONE_BYTE;
		} else {
			data.type = PyUnicodeType::ASCII;
		}
		data.count =
		    ascii_codepoints + one_byte_codepoints + two_byte_codepoints + three_byte_codepoints + four_byte_codepoints;
		return data;
	}

	static int PyUnicodeKind(PyObject *obj) {
		return PyUnicode_KIND(obj);
	}

	static Py_UCS1 *PyUnicode1ByteData(PyObject *obj) {
		return PyUnicode_1BYTE_DATA(obj);
	}

	static Py_UCS2 *PyUnicode2ByteData(PyObject *obj) {
		return PyUnicode_2BYTE_DATA(obj);
	}

	static Py_UCS4 *PyUnicode4ByteData(PyObject *obj) {
		return PyUnicode_4BYTE_DATA(obj);
	}

	static idx_t CurrentMemoryUsage() {
		py::gil_scoped_acquire gil;

		auto psutil = py::module_::import("psutil");

		auto pid = getpid();
		auto process = psutil.attr("Process")(pid);
		auto memory = process.attr("memory_full_info")();
		py::int_ used_memory_p = memory.attr("uss");
		return py::cast<idx_t>(used_memory_p);
	}
};

} // namespace duckdb
