#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/python_objects.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

namespace duckdb_py_convert {

struct RegularConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static NUMPY_T ConvertValue(DUCKDB_T val) {
		return (NUMPY_T)val;
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

struct TimestampConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(timestamp_t val) {
		return Timestamp::GetEpochNanoSeconds(val);
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

struct TimestampConvertSec {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(timestamp_t val) {
		return Timestamp::GetEpochNanoSeconds(Timestamp::FromEpochSeconds(val.value));
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

struct TimestampConvertMilli {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(timestamp_t val) {
		return Timestamp::GetEpochNanoSeconds(Timestamp::FromEpochMs(val.value));
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

struct TimestampConvertNano {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(timestamp_t val) {
		return val.value;
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

struct DateConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(date_t val) {
		return Date::EpochNanoseconds(val);
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

struct IntervalConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static int64_t ConvertValue(interval_t val) {
		return Interval::GetNanoseconds(val);
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

struct TimeConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static PyObject *ConvertValue(dtime_t val) {
		auto str = duckdb::Time::ToString(val);
		return PyUnicode_FromStringAndSize(str.c_str(), str.size());
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return nullptr;
	}
};

struct StringConvert {
	template <class T>
	static void ConvertUnicodeValueTemplated(T *result, const char *data, idx_t ascii_count, idx_t len) {
		if (sizeof(T) == sizeof(char)) {
			// fast path for 1 byte unicode
			memcpy(result, data, len);
			return;
		} else {
			// we first fill in the batch of ascii characters directly
			memcpy(result, data, ascii_count);
		}
		int sz;
		idx_t pos = ascii_count;
		idx_t i = 0;
		// then we fill in the remaining codepoints
		while (pos < len) {
			result[ascii_count + i] = Utf8Proc::UTF8ToCodepoint(data + pos, sz);
			pos += sz;
			i++;
		}
	}

	static void ConvertUnicodeValue(PyObject *result, const char *data, idx_t len, idx_t start_pos) {
		// slow path: check the code points
		// we know that all characters before "start_pos" were ascii characters, so we don't need to check those

		// allocate an array of code points so we only have to convert the codepoints once
		// short-string optimization
		// we know that the max amount of codepoints is the length of the string
		// for short strings (less than 64 bytes) we simply statically allocate an array of 256 bytes (64x int32)
		// this avoids memory allocation for small strings (common case)
		// based on the resulting unicode kind, we fill in the code points
		auto kind = PyUtil::PyUnicodeKind(result);
		switch (kind) {
		case PyUnicode_1BYTE_KIND:
			ConvertUnicodeValueTemplated<Py_UCS1>(PyUtil::PyUnicode1ByteData(result), data, start_pos, len);
			break;
		case PyUnicode_2BYTE_KIND:
			ConvertUnicodeValueTemplated<Py_UCS2>(PyUtil::PyUnicode2ByteData(result), data, start_pos, len);
			break;
		case PyUnicode_4BYTE_KIND:
			ConvertUnicodeValueTemplated<Py_UCS4>(PyUtil::PyUnicode4ByteData(result), data, start_pos, len);
			break;
		default:
			throw NotImplementedException("Unsupported typekind constant '%d' for Python Unicode Compact decode", kind);
		}
	}

	static void ConvertValue(PyObject *result, const string_t &val) {
		// we could use PyUnicode_FromStringAndSize here, but it does a lot of verification that we don't need
		// because of that it is a lot slower than it needs to be
		auto data = const_data_ptr_cast(val.GetData());
		auto len = val.GetSize();
		// check if there are any non-ascii characters in there
		for (idx_t i = 0; i < len; i++) {
			if (data[i] > 127) {
				// there are! fallback to slower case
				return ConvertUnicodeValue(result, const_char_ptr_cast(data), len, i);
			}
		}
		// no unicode: fast path
		// directly construct the string and memcpy it
		auto target_data = PyUtil::PyUnicodeDataMutable(result);
		memcpy(target_data, data, len);
	}

	static PyObject *NullValue() {
		return nullptr;
	}
};

struct BlobConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static PyObject *ConvertValue(string_t val) {
		return PyByteArray_FromStringAndSize(val.GetData(), val.GetSize());
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return nullptr;
	}
};

struct BitConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static PyObject *ConvertValue(string_t val) {
		return PyBytes_FromStringAndSize(val.GetData(), val.GetSize());
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return nullptr;
	}
};

struct UUIDConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static PyObject *ConvertValue(hugeint_t val) {
		auto &import_cache = *DuckDBPyConnection::ImportCache();
		py::handle h = import_cache.uuid().UUID()(UUID::ToString(val)).release();
		return h.ptr();
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return nullptr;
	}
};

struct ListConvert {
	static py::list ConvertValue(Vector &input, idx_t chunk_offset) {
		auto val = input.GetValue(chunk_offset);
		auto &list_children = ListValue::GetChildren(val);
		py::list list;
		for (auto &list_elem : list_children) {
			list.append(PythonObject::FromValue(list_elem, ListType::GetChildType(input.GetType())));
		}
		return list;
	}
};

struct StructConvert {
	static py::dict ConvertValue(Vector &input, idx_t chunk_offset) {
		py::dict py_struct;
		auto val = input.GetValue(chunk_offset);
		auto &child_types = StructType::GetChildTypes(input.GetType());
		auto &struct_children = StructValue::GetChildren(val);

		for (idx_t i = 0; i < struct_children.size(); i++) {
			auto &child_entry = child_types[i];
			auto &child_name = child_entry.first;
			auto &child_type = child_entry.second;
			py_struct[child_name.c_str()] = PythonObject::FromValue(struct_children[i], child_type);
		}
		return py_struct;
	}
};

struct MapConvert {
	static py::dict ConvertValue(Vector &input, idx_t chunk_offset) {
		auto val = input.GetValue(chunk_offset);
		auto &list_children = ListValue::GetChildren(val);

		auto &key_type = MapType::KeyType(input.GetType());
		auto &val_type = MapType::ValueType(input.GetType());

		py::list keys;
		py::list values;
		for (auto &list_elem : list_children) {
			auto &struct_children = StructValue::GetChildren(list_elem);
			keys.append(PythonObject::FromValue(struct_children[0], key_type));
			values.append(PythonObject::FromValue(struct_children[1], val_type));
		}

		py::dict py_struct;
		py_struct["key"] = keys;
		py_struct["value"] = values;
		return py_struct;
	}
};

struct IntegralConvert {
	template <class DUCKDB_T, class NUMPY_T>
	static NUMPY_T ConvertValue(DUCKDB_T val) {
		return NUMPY_T(val);
	}

	template <class NUMPY_T>
	static NUMPY_T NullValue() {
		return 0;
	}
};

template <>
double IntegralConvert::ConvertValue(hugeint_t val) {
	double result;
	Hugeint::TryCast(val, result);
	return result;
}

} // namespace duckdb_py_convert

template <class DUCKDB_T, class NUMPY_T, class CONVERT>
static bool ConvertColumn(idx_t target_offset, data_ptr_t target_data, bool *target_mask, UnifiedVectorFormat &idata,
                          idx_t count) {
	auto src_ptr = UnifiedVectorFormat::GetData<DUCKDB_T>(idata);
	auto out_ptr = reinterpret_cast<NUMPY_T *>(target_data);
	if (!idata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			if (!idata.validity.RowIsValidUnsafe(src_idx)) {
				target_mask[offset] = true;
				out_ptr[offset] = CONVERT::template NullValue<NUMPY_T>();
			} else {
				out_ptr[offset] = CONVERT::template ConvertValue<DUCKDB_T, NUMPY_T>(src_ptr[src_idx]);
				target_mask[offset] = false;
			}
		}
		return true;
	} else {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			out_ptr[offset] = CONVERT::template ConvertValue<DUCKDB_T, NUMPY_T>(src_ptr[src_idx]);
			target_mask[offset] = false;
		}
		return false;
	}
}

static bool ConvertColumnString(idx_t target_offset, data_ptr_t target_data, bool *target_mask,
                                UnifiedVectorFormat &idata, idx_t count) {
	// We have pre-allocated the strings for this column, so we just use the existing memory
	auto src_ptr = UnifiedVectorFormat::GetData<string_t>(idata);
	auto out_ptr = reinterpret_cast<PyObject **>(target_data);
	if (!idata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			if (!idata.validity.RowIsValidUnsafe(src_idx)) {
				target_mask[offset] = true;
			} else {
				duckdb_py_convert::StringConvert::ConvertValue(out_ptr[offset], src_ptr[src_idx]);
				target_mask[offset] = false;
			}
		}
		return true;
	} else {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			duckdb_py_convert::StringConvert::ConvertValue(out_ptr[offset], src_ptr[src_idx]);
			target_mask[offset] = false;
		}
		return false;
	}
}

template <class DUCKDB_T, class NUMPY_T>
static bool ConvertColumnCategoricalTemplate(idx_t target_offset, data_ptr_t target_data, UnifiedVectorFormat &idata,
                                             idx_t count) {
	auto src_ptr = UnifiedVectorFormat::GetData<DUCKDB_T>(idata);
	auto out_ptr = reinterpret_cast<NUMPY_T *>(target_data);
	if (!idata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			if (!idata.validity.RowIsValidUnsafe(src_idx)) {
				out_ptr[offset] = static_cast<NUMPY_T>(-1);
			} else {
				out_ptr[offset] =
				    duckdb_py_convert::RegularConvert::template ConvertValue<DUCKDB_T, NUMPY_T>(src_ptr[src_idx]);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			out_ptr[offset] =
			    duckdb_py_convert::RegularConvert::template ConvertValue<DUCKDB_T, NUMPY_T>(src_ptr[src_idx]);
		}
	}
	// Null values are encoded in the data itself
	return false;
}

template <class NUMPY_T, class CONVERT>
static bool ConvertNested(idx_t target_offset, data_ptr_t target_data, bool *target_mask, Vector &input,
                          UnifiedVectorFormat &idata, idx_t count) {
	auto out_ptr = reinterpret_cast<NUMPY_T *>(target_data);
	if (!idata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			if (!idata.validity.RowIsValidUnsafe(src_idx)) {
				target_mask[offset] = true;
			} else {
				out_ptr[offset] = CONVERT::ConvertValue(input, i);
				target_mask[offset] = false;
			}
		}
		return true;
	} else {
		for (idx_t i = 0; i < count; i++) {
			idx_t offset = target_offset + i;
			out_ptr[offset] = CONVERT::ConvertValue(input, i);
			target_mask[offset] = false;
		}
		return false;
	}
}

template <class NUMPY_T>
static bool ConvertColumnCategorical(idx_t target_offset, data_ptr_t target_data, UnifiedVectorFormat &idata,
                                     idx_t count, PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::UINT8:
		return ConvertColumnCategoricalTemplate<uint8_t, NUMPY_T>(target_offset, target_data, idata, count);
	case PhysicalType::UINT16:
		return ConvertColumnCategoricalTemplate<uint16_t, NUMPY_T>(target_offset, target_data, idata, count);
	case PhysicalType::UINT32:
		return ConvertColumnCategoricalTemplate<uint32_t, NUMPY_T>(target_offset, target_data, idata, count);
	default:
		throw InternalException("Enum Physical Type not Allowed");
	}
}

template <class T>
static bool ConvertColumnRegular(idx_t target_offset, data_ptr_t target_data, bool *target_mask,
                                 UnifiedVectorFormat &idata, idx_t count) {
	return ConvertColumn<T, T, duckdb_py_convert::RegularConvert>(target_offset, target_data, target_mask, idata,
	                                                              count);
}

template <class DUCKDB_T>
static bool ConvertDecimalInternal(idx_t target_offset, data_ptr_t target_data, bool *target_mask,
                                   UnifiedVectorFormat &idata, idx_t count, double division) {
	auto src_ptr = UnifiedVectorFormat::GetData<DUCKDB_T>(idata);
	auto out_ptr = reinterpret_cast<double *>(target_data);
	if (!idata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			if (!idata.validity.RowIsValidUnsafe(src_idx)) {
				target_mask[offset] = true;
			} else {
				out_ptr[offset] =
				    duckdb_py_convert::IntegralConvert::ConvertValue<DUCKDB_T, double>(src_ptr[src_idx]) / division;
				target_mask[offset] = false;
			}
		}
		return true;
	} else {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.sel->get_index(i);
			idx_t offset = target_offset + i;
			out_ptr[offset] =
			    duckdb_py_convert::IntegralConvert::ConvertValue<DUCKDB_T, double>(src_ptr[src_idx]) / division;
			target_mask[offset] = false;
		}
		return false;
	}
}

static bool ConvertDecimal(const LogicalType &decimal_type, idx_t target_offset, data_ptr_t target_data,
                           bool *target_mask, UnifiedVectorFormat &idata, idx_t count) {
	auto dec_scale = DecimalType::GetScale(decimal_type);
	double division = pow(10, dec_scale);
	switch (decimal_type.InternalType()) {
	case PhysicalType::INT16:
		return ConvertDecimalInternal<int16_t>(target_offset, target_data, target_mask, idata, count, division);
	case PhysicalType::INT32:
		return ConvertDecimalInternal<int32_t>(target_offset, target_data, target_mask, idata, count, division);
	case PhysicalType::INT64:
		return ConvertDecimalInternal<int64_t>(target_offset, target_data, target_mask, idata, count, division);
	case PhysicalType::INT128:
		return ConvertDecimalInternal<hugeint_t>(target_offset, target_data, target_mask, idata, count, division);
	default:
		throw NotImplementedException("Unimplemented internal type for DECIMAL");
	}
}

RawArrayWrapper::RawArrayWrapper(py::array array_p, const LogicalType &type)
    : array(std::move(array_p)), data(data_ptr_cast(array.mutable_data())), type(type) {
	type_width = DuckDBToNumpyTypeWidth(type);
}

RawArrayWrapper::RawArrayWrapper(const LogicalType &type) : array(), data(nullptr), type(type) {
	type_width = DuckDBToNumpyTypeWidth(type);
}

idx_t RawArrayWrapper::DuckDBToNumpyTypeWidth(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return sizeof(bool);
	case LogicalTypeId::UTINYINT:
		return sizeof(uint8_t);
	case LogicalTypeId::USMALLINT:
		return sizeof(uint16_t);
	case LogicalTypeId::UINTEGER:
		return sizeof(uint32_t);
	case LogicalTypeId::UBIGINT:
		return sizeof(uint64_t);
	case LogicalTypeId::TINYINT:
		return sizeof(int8_t);
	case LogicalTypeId::SMALLINT:
		return sizeof(int16_t);
	case LogicalTypeId::INTEGER:
		return sizeof(int32_t);
	case LogicalTypeId::BIGINT:
		return sizeof(int64_t);
	case LogicalTypeId::FLOAT:
		return sizeof(float);
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return sizeof(double);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::TIMESTAMP_TZ:
		return sizeof(int64_t);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BIT:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::ENUM:
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::UUID:
		return sizeof(PyObject *);
	default:
		throw NotImplementedException("Unsupported type \"%s\" for DuckDB -> NumPy conversion", type.ToString());
	}
}

string RawArrayWrapper::DuckDBToNumpyDtype(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "bool";
	case LogicalTypeId::TINYINT:
		return "int8";
	case LogicalTypeId::SMALLINT:
		return "int16";
	case LogicalTypeId::INTEGER:
		return "int32";
	case LogicalTypeId::BIGINT:
		return "int64";
	case LogicalTypeId::UTINYINT:
		return "uint8";
	case LogicalTypeId::USMALLINT:
		return "uint16";
	case LogicalTypeId::UINTEGER:
		return "uint32";
	case LogicalTypeId::UBIGINT:
		return "uint64";
	case LogicalTypeId::FLOAT:
		return "float32";
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return "float64";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::DATE:
		return "datetime64[ns]";
	case LogicalTypeId::INTERVAL:
		return "timedelta64[ns]";
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BIT:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::UUID:
		return "object";
	case LogicalTypeId::ENUM: {
		auto size = EnumType::GetSize(type);
		if (size <= (idx_t)NumericLimits<int8_t>::Maximum()) {
			return "int8";
		} else if (size <= (idx_t)NumericLimits<int16_t>::Maximum()) {
			return "int16";
		} else if (size <= (idx_t)NumericLimits<int32_t>::Maximum()) {
			return "int32";
		} else {
			throw InternalException("Size not supported on ENUM types");
		}
	}
	default:
		throw NotImplementedException("Unsupported type \"%s\"", type.ToString());
	}
}

void RawArrayWrapper::Initialize(idx_t capacity) {
	D_ASSERT(py::gil_check());
	string dtype = DuckDBToNumpyDtype(type);

	array = py::array(py::dtype(dtype), capacity);
	data = data_ptr_cast(array.mutable_data());
}

void RawArrayWrapper::Resize(idx_t new_capacity) {
	D_ASSERT(py::gil_check());
	vector<py::ssize_t> new_shape {py::ssize_t(new_capacity)};
	const long *current_shape = array.shape();
	if (current_shape && *current_shape == (long)new_capacity) {
		// Already correct shape
		return;
	}
	array.resize(new_shape, false);
	data = data_ptr_cast(array.mutable_data());
}

ArrayWrapper::ArrayWrapper(unique_ptr<RawArrayWrapper> data_p, unique_ptr<RawArrayWrapper> mask_p, bool requires_mask)
    : data(std::move(data_p)), mask(std::move(mask_p)), requires_mask(requires_mask) {
}

ArrayWrapper::ArrayWrapper(const LogicalType &type) : requires_mask(false) {
	data = make_uniq<RawArrayWrapper>(type);
	mask = make_uniq<RawArrayWrapper>(LogicalType::BOOLEAN);
}

void ArrayWrapper::Initialize(idx_t capacity) {
	data->Initialize(capacity);
	mask->Initialize(capacity);
}

void ArrayWrapper::Resize(idx_t new_capacity) {
	data->Resize(new_capacity);
	mask->Resize(new_capacity);
}

void ArrayWrapper::AllocateStrings(idx_t offset, Vector &source, const uint8_t *codepoints, const uint32_t *lengths,
                                   idx_t count) {
	static const int32_t codepoint_bucket[] = {127, 255, 65535, 65536};
	static constexpr uint8_t BUCKET_LENGTH = sizeof(codepoint_bucket) / sizeof(*codepoint_bucket);
	// TODO: also allow this for nested types that contain VARCHAR
	D_ASSERT(data->type.id() == LogicalTypeId::VARCHAR);

	UnifiedVectorFormat string_format;
	source.ToUnifiedFormat(count, string_format);
	auto string_data = UnifiedVectorFormat::GetData<string_t>(string_format);

	auto dataptr = reinterpret_cast<PyObject **>(data->data);
	if (string_format.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto string_index = string_format.sel->get_index(i);

			auto category = codepoints[i];
			auto length = lengths[i];
			D_ASSERT(category < 4);
			auto max_codepoint = codepoint_bucket[category];

			dataptr[offset + i] = PyUnicode_New(length, max_codepoint);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto string_index = string_format.sel->get_index(i);
			if (!string_format.validity.RowIsValid(string_index)) {
				dataptr[offset + i] = nullptr;
				continue;
			}

			auto category = codepoints[i];
			auto length = lengths[i];
			D_ASSERT(category < 4);
			auto max_codepoint = codepoint_bucket[category];

			dataptr[offset + i] = PyUnicode_New(length, max_codepoint);
		}
	}
}

void ArrayWrapper::Append(idx_t current_offset, Vector &input, idx_t count) {
	auto dataptr = data->data;
	auto maskptr = reinterpret_cast<bool *>(mask->data);
	D_ASSERT(dataptr);
	D_ASSERT(maskptr);
	D_ASSERT(input.GetType() == data->type);
	bool may_have_null;

	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);
	switch (input.GetType().id()) {
	case LogicalTypeId::ENUM: {
		py::gil_scoped_acquire gil;
		auto size = EnumType::GetSize(input.GetType());
		auto physical_type = input.GetType().InternalType();
		if (size <= (idx_t)NumericLimits<int8_t>::Maximum()) {
			may_have_null = ConvertColumnCategorical<int8_t>(current_offset, dataptr, idata, count, physical_type);
		} else if (size <= (idx_t)NumericLimits<int16_t>::Maximum()) {
			may_have_null = ConvertColumnCategorical<int16_t>(current_offset, dataptr, idata, count, physical_type);
		} else if (size <= (idx_t)NumericLimits<int32_t>::Maximum()) {
			may_have_null = ConvertColumnCategorical<int32_t>(current_offset, dataptr, idata, count, physical_type);
		} else {
			throw InternalException("Size not supported on ENUM types");
		}
	} break;
	case LogicalTypeId::BOOLEAN:
		may_have_null = ConvertColumnRegular<bool>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::TINYINT:
		may_have_null = ConvertColumnRegular<int8_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::SMALLINT:
		may_have_null = ConvertColumnRegular<int16_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::INTEGER:
		may_have_null = ConvertColumnRegular<int32_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::BIGINT:
		may_have_null = ConvertColumnRegular<int64_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::UTINYINT:
		may_have_null = ConvertColumnRegular<uint8_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::USMALLINT:
		may_have_null = ConvertColumnRegular<uint16_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::UINTEGER:
		may_have_null = ConvertColumnRegular<uint32_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::UBIGINT:
		may_have_null = ConvertColumnRegular<uint64_t>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::HUGEINT:
		may_have_null = ConvertColumn<hugeint_t, double, duckdb_py_convert::IntegralConvert>(current_offset, dataptr,
		                                                                                     maskptr, idata, count);
		break;
	case LogicalTypeId::FLOAT:
		may_have_null = ConvertColumnRegular<float>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::DOUBLE:
		may_have_null = ConvertColumnRegular<double>(current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::DECIMAL:
		may_have_null = ConvertDecimal(input.GetType(), current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		may_have_null = ConvertColumn<timestamp_t, int64_t, duckdb_py_convert::TimestampConvert>(
		    current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::TIMESTAMP_SEC:
		may_have_null = ConvertColumn<timestamp_t, int64_t, duckdb_py_convert::TimestampConvertSec>(
		    current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::TIMESTAMP_MS:
		may_have_null = ConvertColumn<timestamp_t, int64_t, duckdb_py_convert::TimestampConvertMilli>(
		    current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::TIMESTAMP_NS:
		may_have_null = ConvertColumn<timestamp_t, int64_t, duckdb_py_convert::TimestampConvertNano>(
		    current_offset, dataptr, maskptr, idata, count);
		break;
	case LogicalTypeId::DATE:
		may_have_null = ConvertColumn<date_t, int64_t, duckdb_py_convert::DateConvert>(current_offset, dataptr, maskptr,
		                                                                               idata, count);
		break;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ: {
		py::gil_scoped_acquire gil;
		may_have_null = ConvertColumn<dtime_t, PyObject *, duckdb_py_convert::TimeConvert>(current_offset, dataptr,
		                                                                                   maskptr, idata, count);
		break;
	}
	case LogicalTypeId::INTERVAL:
		may_have_null = ConvertColumn<interval_t, int64_t, duckdb_py_convert::IntervalConvert>(current_offset, dataptr,
		                                                                                       maskptr, idata, count);
		break;
	case LogicalTypeId::VARCHAR: {
		// Note we don't need the GIL here as we have pre-allocated them
		may_have_null = ConvertColumnString(current_offset, dataptr, maskptr, idata, count);
		break;
	}
	case LogicalTypeId::BLOB: {
		py::gil_scoped_acquire gil;
		may_have_null = ConvertColumn<string_t, PyObject *, duckdb_py_convert::BlobConvert>(current_offset, dataptr,
		                                                                                    maskptr, idata, count);
		break;
	}
	case LogicalTypeId::BIT: {
		py::gil_scoped_acquire gil;
		may_have_null = ConvertColumn<string_t, PyObject *, duckdb_py_convert::BitConvert>(current_offset, dataptr,
		                                                                                   maskptr, idata, count);
		break;
	}
	case LogicalTypeId::LIST: {
		py::gil_scoped_acquire gil;
		may_have_null = ConvertNested<py::list, duckdb_py_convert::ListConvert>(current_offset, dataptr, maskptr, input,
		                                                                        idata, count);
		break;
	}
	case LogicalTypeId::MAP: {
		py::gil_scoped_acquire gil;
		may_have_null = ConvertNested<py::dict, duckdb_py_convert::MapConvert>(current_offset, dataptr, maskptr, input,
		                                                                       idata, count);
		break;
	}
	case LogicalTypeId::STRUCT: {
		py::gil_scoped_acquire gil;
		may_have_null = ConvertNested<py::dict, duckdb_py_convert::StructConvert>(current_offset, dataptr, maskptr,
		                                                                          input, idata, count);
		break;
	}
	case LogicalTypeId::UUID: {
		py::gil_scoped_acquire gil;
		may_have_null = ConvertColumn<hugeint_t, PyObject *, duckdb_py_convert::UUIDConvert>(current_offset, dataptr,
		                                                                                     maskptr, idata, count);
		break;
	}

	default:
		throw NotImplementedException("Unsupported type \"%s\"", input.GetType().ToString());
	}
	if (may_have_null) {
		requires_mask = true;
	}
}

const LogicalType &ArrayWrapper::Type() const {
	return data->type;
}

py::object ArrayWrapper::ToArray(idx_t count) const {
	D_ASSERT(py::gil_check());
	D_ASSERT(data->array && mask->array);
	data->Resize(count);
	if (!requires_mask) {
		return std::move(data->array);
	}
	mask->Resize(count);
	// construct numpy arrays from the data and the mask
	auto values = std::move(data->array);
	auto nullmask = std::move(mask->array);

	// create masked array and return it
	auto masked_array = py::module::import("numpy.ma").attr("masked_array")(values, nullmask);
	return masked_array;
}

NumpyResultConversion::NumpyResultConversion(const vector<LogicalType> &types, idx_t initial_capacity)
    : count(0), capacity(0) {
	owned_data.reserve(types.size());
	for (auto &type : types) {
		owned_data.emplace_back(type);
	}
	Resize(initial_capacity);
}

void NumpyResultConversion::Resize(idx_t new_capacity) {
	if (capacity == 0) {
		for (auto &data : owned_data) {
			data.Initialize(new_capacity);
		}
	} else {
		for (auto &data : owned_data) {
			data.Resize(new_capacity);
		}
	}
	capacity = new_capacity;
}

void NumpyResultConversion::SetCategories() {
	for (idx_t col_idx = 0; col_idx < owned_data.size(); col_idx++) {
		auto &type = Type(col_idx);
		if (type.id() == LogicalTypeId::ENUM) {
			// It's an ENUM type, in addition to converting the codes we must convert the categories
			if (categories.find(col_idx) == categories.end()) {
				auto &categories_list = EnumType::GetValuesInsertOrder(type);
				auto categories_size = EnumType::GetSize(type);
				for (idx_t i = 0; i < categories_size; i++) {
					categories[col_idx].append(py::cast(categories_list.GetValue(i).ToString()));
				}
			}
		}
	}
}

int32_t GetMaxCodePoint(const string_t &val) {
	auto data = const_data_ptr_cast(val.GetData());
	auto len = val.GetSize();
	// check if there are any non-ascii characters in there
	int32_t max_codepoint = 127;
	for (idx_t i = 0; i < len; i++) {
		max_codepoint = MaxValue<int32_t>(data[i], max_codepoint);
	}
	return max_codepoint;
}

static void CollectUnicodeStringData(uint8_t *codepoint_data, uint32_t *length_data, const string_t &str, idx_t index) {
	idx_t ascii_count = 0;
	auto string_data = reinterpret_cast<const uint8_t *>(str.GetData());
	auto string_length = str.GetSize();
	for (; ascii_count < string_length && string_data[ascii_count] <= 127; ascii_count++) {
	}
	if (ascii_count == string_length) {
		codepoint_data[index] = static_cast<uint8_t>(PyUnicodeType::ASCII);
		length_data[index] = string_length;
	} else {
		auto unicode_string_data = PyUtil::AnalyzeUnicodeString(string_data + ascii_count, str.GetSize() - ascii_count);
		codepoint_data[index] = static_cast<uint8_t>(unicode_string_data.type);
		length_data[index] = ascii_count + unicode_string_data.count;
	}
}

void NumpyResultConversion::AllocateStrings(DataChunk &chunk, idx_t offset) {
	Vector codepoints(LogicalType::UTINYINT, (idx_t)0);
	Vector lengths(LogicalType::UINTEGER, (idx_t)0);
	uint8_t *codepoint_data = nullptr;
	uint32_t *length_data = nullptr;
	bool initialized = false;

	D_ASSERT(!py::gil_check());
	// For every column that will get filled with strings, pre-allocate them here
	for (idx_t col_idx = 0; col_idx < owned_data.size(); col_idx++) {
		auto &column = chunk.data[col_idx];
		if (column.GetType().id() != LogicalTypeId::VARCHAR) {
			// TODO: also check for nested types if they contain strings
			continue;
		}
		if (!initialized) {
			codepoints.Initialize(false, chunk.size());
			lengths.Initialize(false, chunk.size());
			codepoint_data = FlatVector::GetData<uint8_t>(codepoints);
			length_data = FlatVector::GetData<uint32_t>(lengths);
			initialized = true;
		}
		UnifiedVectorFormat format;

		// Figure out the max codepoints for all of the strings
		column.ToUnifiedFormat(chunk.size(), format);
		auto strings = UnifiedVectorFormat::GetData<string_t>(format);

		if (format.validity.AllValid()) {
			for (idx_t i = 0; i < chunk.size(); i++) {
				idx_t index = format.sel->get_index(i);
				auto &str = strings[index];
				CollectUnicodeStringData(codepoint_data, length_data, str, i);
			}
		} else {
			for (idx_t i = 0; i < chunk.size(); i++) {
				idx_t index = format.sel->get_index(i);
				if (!format.validity.RowIsValid(index)) {
					continue;
				}
				auto &str = strings[index];
				CollectUnicodeStringData(codepoint_data, length_data, str, i);
			}
		}

		// Then allocate the python objects
		py::gil_scoped_acquire gil;
		owned_data[col_idx].AllocateStrings(offset, column, codepoint_data, length_data, chunk.size());
	}
}

void NumpyResultConversion::Append(DataChunk &chunk, idx_t offset) {
	D_ASSERT(offset < capacity || (offset == 0 && chunk.size() == 0));

	// Pre-allocate for the strings up front
	AllocateStrings(chunk, offset);

	auto chunk_types = chunk.GetTypes();
	for (idx_t col_idx = 0; col_idx < owned_data.size(); col_idx++) {
		D_ASSERT(col_idx < chunk.ColumnCount());
		owned_data[col_idx].Append(offset, chunk.data[col_idx], chunk.size());
	}
}

void NumpyResultConversion::SetCardinality(idx_t cardinality) {
	count = cardinality;
}

void NumpyResultConversion::Append(DataChunk &chunk) {
	if (count + chunk.size() > capacity) {
		py::gil_scoped_acquire gil;
		Resize(capacity * 2);
	}
	Append(chunk, count);
	count += chunk.size();
}

} // namespace duckdb
