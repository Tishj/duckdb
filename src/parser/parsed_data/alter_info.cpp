#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"

#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

namespace duckdb {

DependencyUpdate::DependencyUpdate(unique_ptr<LogicalDependencyList> replacement)
    : replacement(std::move(replacement)) {
}

DependencyUpdate::DependencyUpdate(DependencyUpdate &&other) noexcept = default;
DependencyUpdate &DependencyUpdate::operator=(DependencyUpdate &&other) noexcept = default;
DependencyUpdate::~DependencyUpdate() = default;

DependencyUpdate DependencyUpdate::Preserve() {
	return DependencyUpdate(nullptr);
}

DependencyUpdate DependencyUpdate::ReplaceBoundDependencies(const LogicalDependencyList &dependencies) {
	return DependencyUpdate(make_uniq<LogicalDependencyList>(dependencies));
}

bool DependencyUpdate::PreservesDependencies() const {
	return !replacement;
}

const LogicalDependencyList &DependencyUpdate::GetReplacementDependencies() const {
	D_ASSERT(!PreservesDependencies());
	return *replacement;
}

AlterInfo::AlterInfo(AlterType type, QualifiedName name_p, OnEntryNotFound if_not_found)
    : ParseInfo(TYPE), type(type), if_not_found(if_not_found), allow_internal(false),
      qualified_name(std::move(name_p)) {
}

AlterInfo::AlterInfo(AlterType type) : ParseInfo(TYPE), type(type) {
}

AlterInfo::~AlterInfo() {
}

AlterEntryData AlterInfo::GetAlterEntryData() const {
	return AlterEntryData(GetQualifiedName(), if_not_found);
}

bool AlterInfo::IsAddUniqueConstraint() const {
	if (type != AlterType::ALTER_TABLE) {
		return false;
	}

	auto &table_info = Cast<AlterTableInfo>();
	if (table_info.alter_table_type != AlterTableType::ADD_CONSTRAINT) {
		return false;
	}

	auto &constraint_info = table_info.Cast<AddConstraintInfo>();
	return constraint_info.constraint->type == ConstraintType::UNIQUE;
}

bool AlterInfo::IsAddPrimaryKey() const {
	if (!IsAddUniqueConstraint()) {
		return false;
	}

	auto &table_info = Cast<AlterTableInfo>();
	auto &constraint_info = table_info.Cast<AddConstraintInfo>();
	return constraint_info.constraint->Cast<UniqueConstraint>().IsPrimaryKey();
}

} // namespace duckdb
