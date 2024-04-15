//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/transaction_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

enum class TransactionType : uint8_t { INVALID, BEGIN_TRANSACTION, COMMIT, ROLLBACK };

struct TransactionInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::TRANSACTION_INFO;

public:
	explicit TransactionInfo(TransactionType type);

	//! The type of transaction statement
	TransactionType type;

public:
	unique_ptr<ParseInfo> Copy() const {
		return make_uniq<TransactionInfo>(type);
	}
	virtual bool Equals(const TransactionInfo &other) const {
		return type == other.type;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);

private:
	TransactionInfo();
};

} // namespace duckdb
