#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/peg/dialect_extension.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

using namespace duckdb;

static unique_ptr<TransformResultValue> TransformDialectDemoExpression(PEGTransformer &, ParseResult &) {
	auto result = make_uniq<ConstantExpression>(Value("Hello from the dialect extension demo"));
	return make_uniq<TypedTransformResult<unique_ptr<ParsedExpression>>>(std::move(result));
}

class LoadableDialectExtensionDemo final : public DialectExtension {
public:
	LoadableDialectExtensionDemo() : DialectExtension("loadable_dialect_extension_demo") {
	}

	void ApplyGrammarChanges(ParsedGrammar &grammar) override {
		grammar.AddRule("DialectDemoExpression <- 'DIALECT_DEMO'", TransformDialectDemoExpression);
		grammar.PrependChoice("SingleExpression", "DialectDemoExpression");
	}
};

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(loadable_dialect_extension_demo, loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	DialectExtension::Register(config, make_uniq<LoadableDialectExtensionDemo>());
}
}
