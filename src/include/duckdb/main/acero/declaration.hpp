#pragma once

#include "duckdb/main/acero/dataset/exec_node_options.hpp"
#include "duckdb/main/acero/options.hpp"
#include "duckdb/main/acero/dataset/table.hpp"
#include "duckdb/common/shared_ptr.hpp"

#include <string>
#include <vector>

namespace duckdb {
namespace ac {

struct Declaration {
public:
	using Input = Declaration;
	Declaration() {
	}

	/// \brief construct a declaration
	/// \param factory_name the name of the exec node to construct.  The node must have
	///                     been added to the exec node registry with this name.
	/// \param inputs the inputs to the node, these should be other declarations
	/// \param options options that control the behavior of the node.  You must use
	///                the appropriate subclass.  For example, if `factory_name` is
	///                "project" then `options` should be ProjectNodeOptions.
	/// \param label a label to give the node.  Can be used to distinguish it from other
	///              nodes of the same type in the plan.
	Declaration(std::string factory_name, std::vector<Input> inputs,
	            std::shared_ptr<arrow::dataset::ExecNodeOptions> options, std::string label)
	    : factory_name {std::move(factory_name)}, inputs {std::move(inputs)}, options {std::move(options)},
	      label {std::move(label)} {
	}

	template <typename Options>
	Declaration(std::string factory_name, std::vector<Input> inputs, Options options, std::string label)
	    : Declaration {std::move(factory_name), std::move(inputs),
	                   std::shared_ptr<arrow::dataset::ExecNodeOptions>(std::make_shared<Options>(std::move(options))),
	                   std::move(label)} {
	}

	template <typename Options>
	Declaration(std::string factory_name, std::vector<Input> inputs, Options options)
	    : Declaration {std::move(factory_name), std::move(inputs), std::move(options),
	                   /*label=*/""} {
	}

	template <typename Options>
	Declaration(std::string factory_name, Options options)
	    : Declaration {std::move(factory_name), {}, std::move(options), /*label=*/""} {
	}

	template <typename Options>
	Declaration(std::string factory_name, Options options, std::string label)
	    : Declaration {std::move(factory_name), {}, std::move(options), std::move(label)} {
	}

public:
	/// \brief the name of the factory to use when creating a node
	std::string factory_name;
	/// \brief the declarations's inputs
	std::vector<Input> inputs;
	/// \brief options to control the behavior of the node
	std::shared_ptr<arrow::dataset::ExecNodeOptions> options;
	/// \brief a label to give the node in the plan
	std::string label;
};

shared_ptr<arrow::Table> DeclarationToTable(Declaration plan);

} // namespace ac
} // namespace duckdb
