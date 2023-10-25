#include "duckdb/catalog/dependency.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

uint64_t DependencyHashFunction::operator()(const Dependency &a_p) const {
	auto &a = a_p.entry;
	hash_t hash = duckdb::Hash(a.name.c_str());
	hash = CombineHash(hash, duckdb::Hash(a.schema.c_str()));
	hash = CombineHash(hash, duckdb::Hash<uint8_t>(static_cast<uint8_t>(a.type)));
	return hash;
}

bool DependencyEquality::operator()(const Dependency &a_p, const Dependency &b_p) const {
	auto &a = a_p.entry;
	auto &b = b_p.entry;
	if (a.name != b.name) {
		return false;
	}
	if (a.type != b.type) {
		return false;
	}
	if (a.schema != b.schema) {
		return false;
	}
	return true;
}

void LogicalCatalogEntry::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(0, "name", name);
	serializer.WriteProperty(1, "schema", schema);
	serializer.WriteProperty(2, "catalog", catalog);
	serializer.WriteProperty(3, "type", type);
}

LogicalCatalogEntry LogicalCatalogEntry::Deserialize(Deserializer &deserializer) {
	LogicalCatalogEntry dependency;
	dependency.name = deserializer.ReadProperty<string>(0, "name");
	dependency.schema = deserializer.ReadProperty<string>(1, "schema");
	dependency.catalog = deserializer.ReadProperty<string>(2, "catalog");
	dependency.type = deserializer.ReadProperty<CatalogType>(3, "type");
	return dependency;
}

LogicalCatalogEntry::LogicalCatalogEntry(CatalogEntry &entry) {
	this->name = entry.name;
	this->schema = INVALID_SCHEMA;
	if (entry.type != CatalogType::SCHEMA_ENTRY && entry.type != CatalogType::DATABASE_ENTRY) {
		this->schema = entry.ParentSchema().name;
	}
	this->catalog = entry.ParentCatalog().GetName();
	this->type = entry.type;
}

bool LogicalCatalogEntry::operator==(const LogicalCatalogEntry &other) const {
	if (type != other.type) {
		return false;
	}
	if (name != other.name) {
		return false;
	}
	if (schema != other.schema) {
		return false;
	}
	if (catalog != other.catalog) {
		return false;
	}
	return true;
}

} // namespace duckdb
