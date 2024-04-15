#include "duckdb/parser/parsed_data/create_info.hpp"

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

void CreateInfo::CopyProperties(CreateInfo &other) const {
	other.type = type;
	other.catalog = catalog;
	other.schema = schema;
	other.on_conflict = on_conflict;
	other.temporary = temporary;
	other.internal = internal;
	other.sql = sql;
	other.comment = comment;
}

unique_ptr<AlterInfo> CreateInfo::GetAlterInfo() const {
	throw NotImplementedException("GetAlterInfo not implemented for this type");
}

bool CreateInfo::Equals(const CreateInfo *other) const {
	if (type != other->type) {
		return false;
	}
	if (catalog != other->catalog) {
		return false;
	}
	if (schema != other->schema) {
		return false;
	}
	if (on_conflict != other->on_conflict) {
		return false;
	}
	if (temporary != other->temporary) {
		return false;
	}
	if (internal != other->internal) {
		return false;
	}
	if (sql != other->sql) {
		return false;
	}
	return true;
}

} // namespace duckdb
