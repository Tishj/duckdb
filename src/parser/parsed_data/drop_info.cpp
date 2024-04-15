#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/extra_drop_info.hpp"

namespace duckdb {

DropInfo::DropInfo() : ParseInfo(TYPE), catalog(INVALID_CATALOG), schema(INVALID_SCHEMA), cascade(false) {
}

DropInfo::DropInfo(const DropInfo &info)
    : ParseInfo(info.info_type), type(info.type), catalog(info.catalog), schema(info.schema), name(info.name),
      if_not_found(info.if_not_found), cascade(info.cascade), allow_drop_internal(info.allow_drop_internal),
      extra_drop_info(info.extra_drop_info ? info.extra_drop_info->Copy() : nullptr) {
}

unique_ptr<DropInfo> DropInfo::Copy() const {
	return make_uniq<DropInfo>(*this);
}

bool DropInfo::Equals(const DropInfo &other) const {
	if (other.type != type) {
		return false;
	}
	if (other.catalog != catalog) {
		return false;
	}
	if (other.schema != schema) {
		return false;
	}
	if (other.name != name) {
		return false;
	}
	if (other.if_not_found != if_not_found) {
		return false;
	}
	if (other.cascade != cascade) {
		return false;
	}
	if (other.allow_drop_internal != allow_drop_internal) {
		return false;
	}
	if (extra_drop_info) {
		if (!other.extra_drop_info) {
			return false;
		}
		if (other.extra_drop_info->Equals(*extra_drop_info)) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
