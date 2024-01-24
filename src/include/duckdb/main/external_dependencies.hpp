//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/external_dependencies.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

enum ExternalDependenciesType { PYTHON_DEPENDENCY, GENERIC };
class ExternalDependency {
public:
	explicit ExternalDependency(ExternalDependenciesType type_p) : type(type_p) {};
	virtual ~ExternalDependency() {};
	ExternalDependenciesType type;
};

} // namespace duckdb
