#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/arrow/arrow.hpp"

// These classes are wrappers around Arrow objects, making sure the conventions around ownership are respected.

namespace duckdb {

template <class ARROW_OBJECT, bool OWNING>
class ArrowObjectBase {
public:
	explicit ArrowObjectBase() {
		object.release = nullptr;
	}
	ArrowObjectBase(ARROW_OBJECT interface) {
		object.release = nullptr;
		*this = interface;
	}
	operator ARROW_OBJECT *() {
		return &object;
	}
	ArrowObjectBase(ARROW_OBJECT *interface) : ArrowObjectBase(*interface) {
		if (OWNING) {
			// We have taken ownership over this schema object, communicate this to the caller
			interface->release = nullptr;
		}
	}
	ArrowObjectBase(ArrowObjectBase &&other) : ArrowObjectBase(&other.object) {
	}
	ArrowObjectBase(ArrowObjectBase &other) = delete;
	~ArrowObjectBase() {
		if (OWNING && object.release) {
			object.release(&object);
			object.release = nullptr;
		}
		object.release = nullptr;
	}

public:
	ArrowObjectBase &operator=(ARROW_OBJECT other) {
		if (Valid()) {
			object.release(&object);
		}
		memcpy(&object, &other, sizeof(object));
		return *this;
	}
	ArrowObjectBase &operator=(ArrowObjectBase &&other) {
		*this = other.object;
		other.object.release = nullptr;
	}

public:
	bool Valid() const {
		return object.release != nullptr;
	}

protected:
	void AssertOwnership() {
		// Only when the release pointer is not null can we be sure the data of the array is valid and accessible
		D_ASSERT(object.release);
	}

protected:
	ARROW_OBJECT object;
};

template <bool OWNING>
class ArrowSchemaCPP : public ArrowObjectBase<ArrowSchema, OWNING> {
private:
	using base = ArrowObjectBase<ArrowSchema, OWNING>;

public:
	using base::base;
	using base::operator=;
	using base::AssertOwnership;
	using base::object;

public:
	const string Format() const {
		AssertOwnership();
		return object.format;
	}
	const string Name() const {
		AssertOwnership();
		return object.name;
	}
	const string MetaData() const {
		AssertOwnership();
		return object.metadata;
	}
	int64_t Flags() const {
		AssertOwnership();
		return object.flags;
	}
	int64_t ChildrenCount() const {
		AssertOwnership();
		return object.n_children;
	}
	struct ArrowSchema **Children() const {
		AssertOwnership();
		return object.children;
	}
	struct ArrowSchema *Dictionary() const {
		AssertOwnership();
		return object.dictionary;
	}
};

template <bool OWNING>
class ArrowArrayCPP : public ArrowObjectBase<ArrowArray, OWNING> {
private:
	using base = ArrowObjectBase<ArrowArray, OWNING>;

public:
	using base::base;
	using base::operator=;
	using base::AssertOwnership;
	using base::object;

public:
	int64_t Length() const {
		AssertOwnership();
		return object.length;
	}
	int64_t NullCount() const {
		AssertOwnership();
		return object.null_count;
	}
	int64_t Offset() const {
		AssertOwnership();
		return object.offset;
	}
	int64_t BufferCount() const {
		AssertOwnership();
		return object.n_buffers;
	}
	int64_t ChildrenCount() const {
		AssertOwnership();
		return object.n_children;
	}
	const void **Buffers() const {
		AssertOwnership();
		return object.buffers;
	}
	struct ArrowArray **Children() const {
		AssertOwnership();
		return object.children;
	}
	struct ArrowArray *Dictionary() const {
		AssertOwnership();
		return object.dictionary;
	}
};

template <bool OWNING>
class ArrowArrayStreamCPP : public ArrowObjectBase<ArrowArrayStream, OWNING> {
private:
	using base = ArrowObjectBase<ArrowArrayStream, OWNING>;

public:
	using base::base;
	using base::operator=;
	using base::AssertOwnership;
	using base::object;

public:
	int GetSchema(ArrowSchemaCPP<true> &out) {
		ArrowSchema result;
		object.get_schema(&object, &result);
		out = result;
	}
	int GetNext(ArrowArrayCPP<true> &out) {
		ArrowArray result;
		object.get_next(&object, &result);
		out = result;
	}
	string GetLastError() {
		auto result = object.get_last_error(&object);
		return result;
	}
};

} // namespace duckdb
