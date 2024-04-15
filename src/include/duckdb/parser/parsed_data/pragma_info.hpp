//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/pragma_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

enum class PragmaType : uint8_t { PRAGMA_STATEMENT, PRAGMA_CALL };

struct PragmaInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::PRAGMA_INFO;

public:
	PragmaInfo() : ParseInfo(TYPE) {
	}

	//! Name of the PRAGMA statement
	string name;
	//! Parameter list (if any)
	vector<unique_ptr<ParsedExpression>> parameters;
	//! Named parameter list (if any)
	case_insensitive_map_t<unique_ptr<ParsedExpression>> named_parameters;

public:
	unique_ptr<PragmaInfo> Copy() const {
		auto result = make_uniq<PragmaInfo>();
		result->name = name;
		for (auto &param : parameters) {
			result->parameters.push_back(param->Copy());
		}
		for (auto &entry : named_parameters) {
			result->named_parameters.insert(make_pair(entry.first, entry.second->Copy()));
		}
		return result;
	}

	bool Equals(const PragmaInfo &other) const {
		if (name != other.name) {
			return false;
		}
		if (parameters != other.parameters) {
			return false;
		}
		if (named_parameters.size() != other.named_parameters.size()) {
			return false;
		}
		auto it = named_parameters.begin();
		auto jt = other.named_parameters.begin();
		for (; it != named_parameters.end();) {
			if (it->first != jt->first) {
				return false;
			}
			if (!it->second->Equals(*jt->second)) {
				return false;
			}
			it++;
			jt++;
		}
		return true;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
