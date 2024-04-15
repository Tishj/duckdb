#include "duckdb/parser/parsed_data/attach_info.hpp"

namespace duckdb {

unique_ptr<AttachInfo> AttachInfo::Copy() const {
	auto result = make_uniq<AttachInfo>();
	result->name = name;
	result->path = path;
	result->options = options;
	result->on_conflict = on_conflict;
	return result;
}

bool AttachInfo::Equals(const AttachInfo *other) const {
	if (name != other->name) {
		return false;
	}
	if (path != other->path) {
		return false;
	}
	if (options != other->options) {
		return false;
	}
	if (on_conflict != other.on_conflict) {
		return false;
	}
	return true;
}

} // namespace duckdb
