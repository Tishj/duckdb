#include "duckdb/parser/parsed_data/extra_drop_info.hpp"

namespace duckdb {

ExtraDropSecretInfo::ExtraDropSecretInfo() : ExtraDropInfo(ExtraDropInfoType::SECRET_INFO) {
}

ExtraDropSecretInfo::ExtraDropSecretInfo(const ExtraDropSecretInfo &info)
    : ExtraDropInfo(ExtraDropInfoType::SECRET_INFO) {
	persist_mode = info.persist_mode;
	secret_storage = info.secret_storage;
}

unique_ptr<ExtraDropInfo> ExtraDropSecretInfo::Copy() const {
	return std::move(make_uniq<ExtraDropSecretInfo>(*this));
}

bool ExtraDropSecretInfo::Equals(const ExtraDropInfo &other_p) const {
	if (other_p.info_type != ExtraDropInfoType::SECRET_INFO) {
		return false;
	}
	auto &other = other_p.Cast<ExtraDropSecretInfo>();
	if (other.persist_mode != persist_mode) {
		return false;
	}
	if (other.secret_storage != secret_storage) {
		return false;
	}
	return true;
}

} // namespace duckdb
