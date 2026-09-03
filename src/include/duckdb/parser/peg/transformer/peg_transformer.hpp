#pragma once

#include "duckdb/parser/peg/ast/unpivot_name_values.hpp"
#include "duckdb/parser/peg/transformer/parse_result.hpp"
#include "duckdb/parser/peg/transformer/transform_result.hpp"
#include "duckdb/parser/peg/ast/add_column_entry.hpp"
#include "duckdb/parser/peg/ast/column_constraint_entry.hpp"
#include "duckdb/parser/peg/ast/analyze_target.hpp"
#include "duckdb/parser/peg/ast/column_elements.hpp"
#include "duckdb/parser/peg/ast/create_table_column_element.hpp"
#include "duckdb/parser/peg/ast/create_table_definition.hpp"
#include "duckdb/parser/peg/ast/partition_sorted_options.hpp"
#include "duckdb/parser/peg/ast/distinct_clause.hpp"
#include "duckdb/parser/peg/ast/describe_target.hpp"
#include "duckdb/parser/peg/ast/extension_repository_info.hpp"
#include "duckdb/parser/peg/ast/generated_column_definition.hpp"
#include "duckdb/parser/peg/ast/generic_copy_option.hpp"
#include "duckdb/parser/peg/ast/generic_copy_option_value.hpp"
#include "duckdb/parser/peg/ast/insert_values.hpp"
#include "duckdb/parser/peg/ast/create_pivot_entry.hpp"
#include "duckdb/parser/peg/ast/join_prefix.hpp"
#include "duckdb/parser/peg/ast/join_qualifier.hpp"
#include "duckdb/parser/peg/ast/key_actions.hpp"
#include "duckdb/parser/peg/ast/limit_percent_result.hpp"
#include "duckdb/parser/peg/ast/macro_parameter.hpp"
#include "duckdb/parser/peg/ast/on_conflict_expression_target.hpp"
#include "duckdb/parser/peg/ast/sequence_option.hpp"
#include "duckdb/parser/peg/ast/setting_info.hpp"
#include "duckdb/parser/peg/ast/table_alias.hpp"
#include "duckdb/parser/peg/ast/cast_arguments.hpp"
#include "duckdb/parser/peg/ast/expression_chain.hpp"
#include "duckdb/parser/peg/ast/method_arguments.hpp"
#include "duckdb/parser/peg/ast/trim_arguments.hpp"
#include "duckdb/parser/peg/ast/trigger_event_info.hpp"
#include "duckdb/parser/peg/ast/trigger_table_referencing_info.hpp"
#include "duckdb/parser/peg/ast/window_frame.hpp"
#include "duckdb/function/macro_function.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/parser_options.hpp"
#include "duckdb/common/stack_checker.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/parsed_data/connect_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"
#include "duckdb/parser/statement/copy_database_statement.hpp"
#include "duckdb/parser/statement/set_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"

namespace duckdb {

// Forward declare
struct QualifiedName;
struct CompiledGrammar;
struct MatcherToken;
struct GroupingExpressionMap;
class Matcher;
class TokenIterator;

enum class GroupByExpressionInfoType : uint8_t { EXPRESSION, EMPTY, CUBE, ROLLUP, GROUPING_SETS };

struct GroupByExpressionInfo {
	GroupByExpressionInfoType type = GroupByExpressionInfoType::EMPTY;
	unique_ptr<ParsedExpression> expression;
	vector<unique_ptr<ParsedExpression>> expressions;
	vector<GroupByExpressionInfo> children;
};

class PEGTransformer;
class TransformStack;
class TransformProcess;
class GeneratedTransformProcess;

using transform_process_prepare_t = void (*)(PEGTransformer &transformer, GeneratedTransformProcess &process);
using transform_process_reduce_t = unique_ptr<TransformResultValue> (*)(PEGTransformer &transformer,
                                                                        GeneratedTransformProcess &process);

struct TransformProcessInfo {
	const char *name;
	transform_process_prepare_t prepare;
	transform_process_reduce_t reduce;
};

template <typename T>
unique_ptr<TypedTransformResult<T>> TryBridgeTransformResultValue(TransformResultValue &base_result);

//! Input to start a transformer execution. The rule can be supplied explicitly for transparent parse nodes.
struct TransformInput {
	TransformInput(ParseResult &parse_result_p) : parse_result(parse_result_p) {
	}
	TransformInput(const CompiledGrammarRule &rule_p, ParseResult &parse_result_p)
	    : rule(rule_p), parse_result(parse_result_p) {
	}

	optional_ptr<const CompiledGrammarRule> GetRule() const {
		return rule ? rule : parse_result.GetRule();
	}

	optional_ptr<const CompiledGrammarRule> rule;
	ParseResult &parse_result;
};

//! Essentially a std::variant<TransformInput, unique_ptr<TransformResultValue>>.
//! Produced by TransformProcess::Resume to control the next execution step.
class TransformStep {
public:
	static TransformStep Child(TransformInput input);
	static TransformStep Complete(unique_ptr<TransformResultValue> result);

	optional<TransformInput> GetChild();
	unique_ptr<TransformResultValue> TakeResult();

private:
	TransformStep(optional<TransformInput> child_p, unique_ptr<TransformResultValue> result_p)
	    : child(std::move(child_p)), result(std::move(result_p)) {
	}

private:
	optional<TransformInput> child;
	unique_ptr<TransformResultValue> result;
};

class TransformProcess {
public:
	virtual ~TransformProcess() = default;

	//! Resume transforming, optionally with the result of the previously requested child.
	virtual TransformStep Resume(unique_ptr<TransformResultValue> child_result) = 0;
};

class GeneratedTransformProcess final : public TransformProcess {
public:
	GeneratedTransformProcess(PEGTransformer &transformer, TransformInput input, const TransformProcessInfo &info);

	void ReserveChildSlots(idx_t count);
	void SetChildResult(idx_t slot, unique_ptr<TransformResultValue> result);
	void PushChild(TransformInput input, idx_t slot);
	TransformStep Resume(unique_ptr<TransformResultValue> child_result) override;

	template <class T>
	T TakeResult(idx_t slot) {
		if (slot >= child_results.size() || !child_results[slot]) {
			throw InternalException("Missing transformer result for slot %llu in rule '%s'", slot, info.name);
		}
		auto *result_value = TryGetTransformResult<T>(*child_results[slot]);
		if (!result_value) {
			auto bridged = TryBridgeTransformResultValue<T>(*child_results[slot]);
			if (bridged) {
				auto bridged_result = std::move(bridged->value);
				child_results[slot].reset();
				return bridged_result;
			}
			throw InternalException("Unexpected transformer result type for slot %llu in rule '%s'", slot, info.name);
		}
		auto result = std::move(*result_value);
		child_results[slot].reset();
		return result;
	}

	template <class T>
	T &GetResult(idx_t slot) {
		if (slot >= child_results.size() || !child_results[slot]) {
			throw InternalException("Missing transformer result for slot %llu in rule '%s'", slot, info.name);
		}
		auto *result_value = TryGetTransformResult<T>(*child_results[slot]);
		if (!result_value) {
			throw InternalException("Unexpected transformer result type for slot %llu in rule '%s'", slot, info.name);
		}
		return *result_value;
	}

	ParseResult &parse_result;
	const TransformProcessInfo &info;
	idx_t manual_state = 0;
	vector<unique_ptr<TransformResultValue>> child_results;

private:
	struct PendingChild {
		TransformInput input;
		idx_t slot;
	};

	TransformStep NextStep();

private:
	PEGTransformer &transformer;
	vector<PendingChild> pending_children;
	optional_idx child_result_slot;
	bool completed = false;
};

using transform_reduce_function_t =
    std::function<unique_ptr<TransformResultValue>(PEGTransformer &transformer, ParseResult &parse_result)>;

class ReduceTransformProcess final : public TransformProcess {
public:
	ReduceTransformProcess(PEGTransformer &transformer, ParseResult &parse_result, transform_reduce_function_t reduce);
	TransformStep Resume(unique_ptr<TransformResultValue> child_result) override;

private:
	PEGTransformer &transformer;
	ParseResult &parse_result;
	transform_reduce_function_t reduce;
	bool completed = false;
};

struct TransformStackFrame {
	explicit TransformStackFrame(TransformInput input);

	bool IsInitialized() const;

	optional_ptr<const CompiledGrammarRule> rule;
	ParseResult &parse_result;
	unique_ptr<TransformProcess> process;
	unique_ptr<TransformResultValue> child_result;
	unique_ptr<TransformResultValue> result;
};

class TransformStack {
public:
	explicit TransformStack(PEGTransformer &transformer);
	unique_ptr<TransformResultValue> Execute(TransformInput input);

	template <class T>
	T Execute(TransformInput input) {
		auto base_result = Execute(input);
		auto *result_value = TryGetTransformResult<T>(*base_result);
		if (!result_value) {
			throw InternalException("Unexpected transformer result type for root rule '%s'", input.parse_result.name);
		}
		return std::move(*result_value);
	}

	string FormatStack() const;

private:
	void PushFrame(TransformInput input);
	void InitializeFrame(TransformStackFrame &frame);
	void ExecuteFrame(TransformStackFrame &frame);
	unique_ptr<TransformResultValue> FinalizeFrame(TransformStackFrame &frame);

private:
	PEGTransformer &transformer;
	vector<unique_ptr<TransformStackFrame>> frames;
};

class PEGTransformer {
public:
	PEGTransformer(ArenaAllocator &allocator, TokenIterator &token_iterator, ParserOptions &options_p,
	               const CompiledGrammar &grammar_p)
	    : allocator(allocator), token_iterator(token_iterator), options(options_p), grammar(grammar_p) {
	}

	const CompiledGrammarRule &GetRule(const string &rule_name) const;

public:
	template <typename T>
	T Transform(ParseResult &parse_result) {
		auto base_result = TransformInternal(parse_result);

		auto *result_value = TryGetTransformResult<T>(*base_result);
		if (!result_value) {
			// allow transparent bridging between string-typed and Identifier-typed rules
			auto bridged = TryBridgeTransformResult<T>(*base_result);
			if (bridged) {
				auto bridged_result = std::move(bridged->value);
				SetResultLocation(bridged_result, parse_result.GetLocation());
				return bridged_result;
			}
			throw InternalException("Transformer for rule '" + parse_result.name + "' returned an unexpected type.");
		}

		auto result = std::move(*result_value);
		SetResultLocation(result, parse_result.GetLocation());
		return result;
	}

	//! Bridge between string-typed and Identifier-typed rule results (and their vector forms).
	//! The generic form performs no bridging; the specializations below convert transparently.
	template <typename T>
	static unique_ptr<TypedTransformResult<T>> TryBridgeTransformResult(TransformResultValue &base_result) {
		return TryBridgeTransformResultValue<T>(base_result);
	}

	template <typename T>
	T Transform(ListParseResult &parse_result, idx_t child_index) {
		auto &child_parse_result = parse_result.GetChild(child_index);
		return Transform<T>(child_parse_result);
	}

	template <typename T>
	void TransformOptional(ListParseResult &list_pr, idx_t child_idx, T &target) {
		auto &opt = list_pr.Child<OptionalParseResult>(child_idx);
		if (opt.HasResult()) {
			target = Transform<T>(opt.GetResult());
		}
	}

	// Make overloads return raw pointers, as ownership is handled by the ArenaAllocator.
	template <class T, typename... Args>
	T *Make(Args &&...args) {
		return allocator.Make<T>(std::forward<Args>(args)...);
	}

	void Clear();
	void ClearParameters();
	static void ParamTypeCheck(PreparedParamType last_type, PreparedParamType new_type);
	void SetParam(const Identifier &name, idx_t index, PreparedParamType type);
	bool GetParam(const Identifier &name, idx_t &index, PreparedParamType type);
	void SetParamCount(idx_t new_count);
	idx_t ParamCount() const;
	unique_ptr<SQLStatement> CreatePivotStatement(unique_ptr<SQLStatement> statement);
	unique_ptr<SQLStatement> GenerateCreateEnumStmt(unique_ptr<CreatePivotEntry> entry);
	void PivotEntryCheck(const string &type);
	void ExtractCTEsRecursive(CommonTableExpressionMap &cte_map);
	bool IsWindowFrameDefault(WindowBoundary start, WindowBoundary end);
	unique_ptr<WindowExpression> GetWindowClause(const Identifier &window_name);
	void SetQueryLocation(ParsedExpression &expr, QueryLocation query_location);
	void SetQueryLocation(TableRef &ref, QueryLocation query_location);

private:
	unique_ptr<TransformResultValue> TransformInternal(ParseResult &parse_result);
	unique_ptr<TransformResultValue> ExecuteRecursive(TransformInput input);
	void SetResultLocation(ParseResult &parse_result, TransformResultValue &result);

	template <typename T>
	void SetResultLocation(T &, QueryLocation) {
	}
	void SetResultLocation(unique_ptr<ParsedExpression> &expr, QueryLocation location) {
		if (!expr) {
			return;
		}
		if (location.IsValid() && !expr->HasQueryLocation()) {
			SetQueryLocation(*expr, location);
		}
	}
	void SetResultLocation(unique_ptr<TableRef> &ref, QueryLocation location) {
		if (!ref) {
			return;
		}
		if (location.IsValid() && !ref->query_location.IsValid()) {
			SetQueryLocation(*ref, location);
		}
	}

public:
	ArenaAllocator &allocator;
	TokenIterator &token_iterator;
	identifier_map_t<idx_t> named_parameter_map;
	idx_t prepared_statement_parameter_index = 0;
	PreparedParamType last_param_type = PreparedParamType::INVALID;

	identifier_map_t<unique_ptr<WindowExpression>> window_clauses;

	vector<unique_ptr<CreatePivotEntry>> pivot_entries;
	vector<reference<CommonTableExpressionMap>> stored_cte_map;

	bool in_window_definition = false;
	bool has_anonymous_parameters = false;

	friend class StackChecker<PEGTransformer>;
	idx_t stack_depth = 0;

	StackChecker<PEGTransformer> StackCheck(idx_t extra_stack = 1) {
		if (stack_depth + extra_stack >= options.max_expression_depth) {
			throw ParserException(
			    "Max expression depth limit of %lld exceeded. Use \"SET max_expression_depth TO x\" to "
			    "increase the maximum expression depth.",
			    options.max_expression_depth);
		}
		return StackChecker<PEGTransformer>(*this, extra_stack);
	}

	ParserOptions options;
	const CompiledGrammar &grammar;

private:
	friend class GeneratedTransformProcess;
	friend class ReduceTransformProcess;
	friend class TransformStack;
};

template <typename T>
inline unique_ptr<TypedTransformResult<T>> TryBridgeTransformResultValue(TransformResultValue &base_result) {
	return nullptr;
}

//! Transparent bridging between string-typed and Identifier-typed transform results.
template <>
inline unique_ptr<TypedTransformResult<string>>
TryBridgeTransformResultValue<string>(TransformResultValue &base_result) {
	if (auto *ident = TryGetTransformResult<Identifier>(base_result)) {
		return make_uniq<TypedTransformResult<string>>(ident->GetIdentifierName());
	}
	return nullptr;
}

template <>
inline unique_ptr<TypedTransformResult<Identifier>>
TryBridgeTransformResultValue<Identifier>(TransformResultValue &base_result) {
	if (auto *str = TryGetTransformResult<string>(base_result)) {
		return make_uniq<TypedTransformResult<Identifier>>(Identifier(*str));
	}
	return nullptr;
}

template <>
inline unique_ptr<TypedTransformResult<vector<string>>>
TryBridgeTransformResultValue<vector<string>>(TransformResultValue &base_result) {
	if (auto *idents = TryGetTransformResult<vector<Identifier>>(base_result)) {
		return make_uniq<TypedTransformResult<vector<string>>>(IdentifiersToStrings(*idents));
	}
	return nullptr;
}

template <>
inline unique_ptr<TypedTransformResult<vector<Identifier>>>
TryBridgeTransformResultValue<vector<Identifier>>(TransformResultValue &base_result) {
	if (auto *strs = TryGetTransformResult<vector<string>>(base_result)) {
		return make_uniq<TypedTransformResult<vector<Identifier>>>(StringsToIdentifiers(*strs));
	}
	return nullptr;
}

typedef unique_ptr<TransformResultValue> (*transform_reduce_t)(PEGTransformer &transformer, ParseResult &parse_result);

struct TransformReduceRule {
	const char *name;
	transform_reduce_t reduce;
};

class PEGTransformerFactory {
public:
	static void RegisterDefaultTransforms(ParsedGrammar &grammar);

	//! Match a single TopLevelStatement from `tokens` starting at `token_cursor` and transform it
	//! into a SQLStatement. Returns nullptr if the matched TLS was separator-only (no statement).
	//! Throws on syntax error. `token_cursor` is in/out: it's the token index where matching
	//! starts, and on return holds the token index immediately past the last consumed token.
	static unique_ptr<SQLStatement> TransformTopLevelStatement(TokenIterator &token_iterator, ParserOptions &options,
	                                                           const CompiledGrammar &grammar);
	static ParseResult &ExtractResultFromParens(ParseResult &parse_result);
	static vector<reference<ParseResult>> ExtractParseResultsFromList(ParseResult &parse_result);
	static bool ExpressionIsEmptyStar(const ParsedExpression &expr);
	static QualifiedName StringToQualifiedName(vector<string> input);
	static LogicalType GetIntervalTargetType(DatePartSpecifier date_part);
	static bool ConstructConstantFromExpression(const ParsedExpression &expr, Value &value);
	static unique_ptr<ParsedExpression> TryNegateValue(const ConstantExpression &expr);
	static unique_ptr<ParsedExpression> ConvertNumberToValue(string val);
	static void AddGroupByExpression(unique_ptr<ParsedExpression> expression, GroupingExpressionMap &map,
	                                 GroupByNode &result, vector<ProjectionIndex> &result_set);
	static vector<GroupingSet> GroupByExpressionUnfolding(GroupByExpressionInfo &group_by_expr,
	                                                      GroupingExpressionMap &map, GroupByNode &result);
	static unique_ptr<ResultModifier> VerifyLimitOffset(LimitPercentResult &limit, LimitPercentResult &offset);
	static unique_ptr<QueryNode> ToRecursiveCTE(unique_ptr<QueryNode> node, const Identifier &name,
	                                            vector<Identifier> &aliases,
	                                            vector<unique_ptr<ParsedExpression>> &key_targets);
	static void WrapRecursiveView(unique_ptr<CreateViewInfo> &info, unique_ptr<QueryNode> inner_node);
	static void ConvertToRecursiveView(unique_ptr<CreateViewInfo> &info, unique_ptr<QueryNode> &node);
	static void VerifyColumnRefs(const ParsedExpression &expr);
	static void RemoveOrderQualificationRecursive(unique_ptr<ParsedExpression> &root_expr);
	static void GetValueFromExpression(unique_ptr<ParsedExpression> &expr, vector<Value> &result);
	static bool TransformPivotInList(unique_ptr<ParsedExpression> &expr, PivotColumnEntry &entry);
	static void AddPivotEntry(PEGTransformer &transformer, string enum_name, unique_ptr<SelectNode> base,
	                          unique_ptr<ParsedExpression> column, unique_ptr<QueryNode> subquery, bool has_parameters);
	static Value GetConstantExpressionValue(unique_ptr<ParsedExpression> &expr);
	static void SplitGenericOptions(const vector<GenericCopyOption> &options_in,
	                                case_insensitive_map_t<unique_ptr<ParsedExpression>> &parsed_options,
	                                unordered_map<string, Value> &options, const char *statement_name);
	static void AddToMultiStatement(const unique_ptr<MultiStatement> &multi_statement,
	                                unique_ptr<AlterInfo> alter_info);
	static void AddUpdateToMultiStatement(const unique_ptr<MultiStatement> &multi_statement, const string &column_name,
	                                      const AlterEntryData &table_data,
	                                      const unique_ptr<ParsedExpression> &original_expression);
	static unique_ptr<MultiStatement> TransformAndMaterializeAlter(AlterEntryData &data,
	                                                               unique_ptr<AlterInfo> info_with_null_placeholder,
	                                                               const string &column_name,
	                                                               unique_ptr<ParsedExpression> expression);

	static void PreparePivotStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePivotStatementTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareUnpivotStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUnpivotStatementTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareLiteralExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLiteralExpressionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PreparePrefixExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePrefixExpressionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareOverClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOverClauseTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareSelectStatementInternalTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSelectStatementInternalTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareSimpleSelectTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSimpleSelectTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareTableRefTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableRefTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareWithClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWithClauseTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareWindowDefinitionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWindowDefinitionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);

	//===--------------------------------------------------------------------===//
	// START GENERATED PROCESS RULES
	//===--------------------------------------------------------------------===//
	static void PrepareStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceStatementTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareAlterStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAlterStatementTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareAlterOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAlterOptionsTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareAlterTableStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAlterTableStmtTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareAlterSchemaStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAlterSchemaStmtTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareAlterTableOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAlterTableOptionsTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareAddConstraintTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAddConstraintTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareAddColumnTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAddColumnTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareAddColumnEntryTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAddColumnEntryTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareDropColumnTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropColumnTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareAlterColumnTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAlterColumnTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareRenameColumnTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRenameColumnTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareNestedColumnNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNestedColumnNameTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareIdentifierDotTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIdentifierDotTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareRenameAlterTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRenameAlterTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareSetPartitionedByTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetPartitionedByTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareResetPartitionedByTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceResetPartitionedByTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareSetSortedByTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetSortedByTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareResetSortedByTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceResetSortedByTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareSetOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetOptionsTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareResetOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceResetOptionsTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareAlterColumnEntryTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAlterColumnEntryTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareAddOrDropDefaultTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAddOrDropDefaultTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareAddDefaultTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAddDefaultTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareDropDefaultTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropDefaultTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareChangeNullabilityTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceChangeNullabilityTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareDropOrSetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropOrSetTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareDropNullabilityTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropNullabilityTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareSetNullabilityTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetNullabilityTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareAlterTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAlterTypeTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareUsingExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUsingExpressionTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareAlterViewStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAlterViewStmtTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareAlterSequenceStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAlterSequenceStmtTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareQualifiedSequenceNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceQualifiedSequenceNameTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareAlterSequenceOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAlterSequenceOptionsTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareRenameAlterSequenceOptionsTransform(PEGTransformer &transformer,
	                                                       GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceRenameAlterSequenceOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareSetSequenceOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetSequenceOptionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareAlterDatabaseStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAlterDatabaseStmtTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareAnalyzeStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAnalyzeStatementTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareAnalyzeTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAnalyzeTargetTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareAnalyzeVerboseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAnalyzeVerboseTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareAttachStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAttachStatementTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareDatabasePathTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDatabasePathTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareAttachAliasTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAttachAliasTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareAttachOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAttachOptionsTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareCallStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCallStatementTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareCheckpointStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCheckpointStatementTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareCheckpointForceTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCheckpointForceTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareCommentStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommentStatementTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareCommentOnTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommentOnTypeTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareCommentTableTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommentTableTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareCommentSequenceTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommentSequenceTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareCommentFunctionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommentFunctionTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareCommentMacroTableTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommentMacroTableTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareCommentMacroTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommentMacroTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareCommentViewTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommentViewTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareCommentDatabaseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommentDatabaseTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareCommentIndexTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommentIndexTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareCommentSchemaTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommentSchemaTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareCommentTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommentTypeTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareCommentColumnTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommentColumnTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareCommentValueTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommentValueTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareStringLiteralValueTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceStringLiteralValueTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareAnalyzeKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAnalyzeKeywordTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareExpressionStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExpressionStatementTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareExpressionAliasTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExpressionAliasTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareIndexNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIndexNameTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareConstraintNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceConstraintNameTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareSequenceNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSequenceNameTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareCollationNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCollationNameTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareNumberLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNumberLiteralTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareStringLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceStringLiteralTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTypeTransform(PEGTransformer &transformer,
	                                                            GeneratedTransformProcess &process);
	static void PrepareTypeVariationsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTypeVariationsTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareSimpleTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSimpleTypeTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareCharacterSimpleTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCharacterSimpleTypeTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareQualifiedSimpleTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceQualifiedSimpleTypeTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareIntervalTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIntervalTypeTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareIntervalIntervalTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIntervalIntervalTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareIntervalWithSpecifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIntervalWithSpecifierTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareIntervalWithRangeSpecifierTransform(PEGTransformer &transformer,
	                                                       GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceIntervalWithRangeSpecifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareIntervalWithSimpleSpecifierTransform(PEGTransformer &transformer,
	                                                        GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceIntervalWithSimpleSpecifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareIntervalWithoutSpecifierTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIntervalWithoutSpecifierTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PrepareIntervalToIntervalAsTypeTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIntervalToIntervalAsTypeTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PrepareYearKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceYearKeywordTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareMonthKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMonthKeywordTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareDayKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDayKeywordTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareHourKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceHourKeywordTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareMinuteKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMinuteKeywordTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareSecondKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSecondKeywordTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareMillisecondKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMillisecondKeywordTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareMicrosecondKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMicrosecondKeywordTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareWeekKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWeekKeywordTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareQuarterKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceQuarterKeywordTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareDecadeKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDecadeKeywordTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareCenturyKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCenturyKeywordTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareMillenniumKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMillenniumKeywordTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareIntervalTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIntervalTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareIntervalToIntervalTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIntervalToIntervalTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareYearToMonthTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceYearToMonthTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareDayToHourTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDayToHourTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareDayToMinuteTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDayToMinuteTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareDayToSecondTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDayToSecondTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareHourToMinuteTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceHourToMinuteTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareHourToSecondTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceHourToSecondTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareMinuteToSecondTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMinuteToSecondTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareBitTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBitTypeTransform(PEGTransformer &transformer,
	                                                               GeneratedTransformProcess &process);
	static void PrepareGeometryTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGeometryTypeTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareVariantTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceVariantTypeTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareNumericTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNumericTypeTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareSimpleNumericTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSimpleNumericTypeTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareDecimalNumericTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDecimalNumericTypeTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareIntTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIntTypeTransform(PEGTransformer &transformer,
	                                                               GeneratedTransformProcess &process);
	static void PrepareIntegerTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIntegerTypeTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareSmallintTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSmallintTypeTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareBigintTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBigintTypeTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareRealTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRealTypeTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareBooleanTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBooleanTypeTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareDoubleTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDoubleTypeTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareFloatTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFloatTypeTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareDecimalTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDecimalTypeTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareDecTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDecTypeTransform(PEGTransformer &transformer,
	                                                               GeneratedTransformProcess &process);
	static void PrepareNumericModTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNumericModTypeTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareQualifiedTypeNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceQualifiedTypeNameTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareTypeNameAsQualifiedNameTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTypeNameAsQualifiedNameTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareCatalogReservedSchemaTypeNameTransform(PEGTransformer &transformer,
	                                                          GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceCatalogReservedSchemaTypeNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareSchemaReservedTypeNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSchemaReservedTypeNameTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareTypeModifiersTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTypeModifiersTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareRowTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRowTypeTransform(PEGTransformer &transformer,
	                                                               GeneratedTransformProcess &process);
	static void PrepareSetofTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetofTypeTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareUnionTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUnionTypeTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareColIdTypeListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColIdTypeListTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareMapTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMapTypeTransform(PEGTransformer &transformer,
	                                                               GeneratedTransformProcess &process);
	static void PrepareTupleTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTupleTypeTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareColIdTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColIdTypeTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareArrayBoundsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceArrayBoundsTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareArrayKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceArrayKeywordTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareArrayKeywordWithBoundsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceArrayKeywordWithBoundsTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareSquareBracketsArrayTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSquareBracketsArrayTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareTimeTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTimeTypeTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareTimeOrTimestampTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTimeOrTimestampTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareTimeTypeIdTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTimeTypeIdTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareTimestampTypeIdTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTimestampTypeIdTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareTimeZoneTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTimeZoneTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareWithOrWithoutTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWithOrWithoutTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareWithRuleTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWithRuleTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareWithoutRuleTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWithoutRuleTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareConnectStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceConnectStatementTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareDisconnectStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDisconnectStatementTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareSessionTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSessionTargetTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareLocalSessionTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLocalSessionTargetTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareStringSessionTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceStringSessionTargetTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareCatalogSessionTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCatalogSessionTargetTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareCopyStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyStatementTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareCopyVariationsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyVariationsTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareCopyTableTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyTableTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareFromOrToTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFromOrToTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareCopyFromTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyFromTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareCopyToTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyToTransform(PEGTransformer &transformer,
	                                                              GeneratedTransformProcess &process);
	static void PrepareCopySelectTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopySelectTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareCopyFileNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyFileNameTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareCopyFileNameExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyFileNameExpressionTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareCopyFileNameStringLiteralTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceCopyFileNameStringLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareCopyFileNameIdentifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyFileNameIdentifierTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareCopyFileNameIdentifierColIdTransform(PEGTransformer &transformer,
	                                                        GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceCopyFileNameIdentifierColIdTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareIdentifierColIdTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIdentifierColIdTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareCopyOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyOptionsTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareCopyOptionListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyOptionListTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareSpecializedOptionListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSpecializedOptionListTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareSpecializedOptionTailTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSpecializedOptionTailTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareSpecializedOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSpecializedOptionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareSingleOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSingleOptionTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareBinaryOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBinaryOptionTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareFreezeOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFreezeOptionTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareOidsOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOidsOptionTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareCsvOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCsvOptionTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareHeaderOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceHeaderOptionTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareNullAsOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNullAsOptionTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareDelimiterAsOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDelimiterAsOptionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareQuoteAsOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceQuoteAsOptionTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareEscapeAsOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceEscapeAsOptionTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareEncodingOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceEncodingOptionTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareForceQuoteOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceForceQuoteOptionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareStarSymbolColumnListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceStarSymbolColumnListTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareForceQuoteTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceForceQuoteTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PreparePartitionByOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePartitionByOptionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PreparePartitionByColumnListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePartitionByColumnListTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareStarPartitionByColumnListTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceStarPartitionByColumnListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareParenthesizedPartitionByColumnListTransform(PEGTransformer &transformer,
	                                                               GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceParenthesizedPartitionByColumnListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareSinglePartitionByColumnListTransform(PEGTransformer &transformer,
	                                                        GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceSinglePartitionByColumnListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareForceNullOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceForceNullOptionTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareForceNotNullTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceForceNotNullTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareCopyGenericOptionListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyGenericOptionListTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareCopyGenericOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyGenericOptionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareOrderByCopyOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOrderByCopyOptionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PreparePartitionedByCopyOptionTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePartitionedByCopyOptionTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareGenericCopyOptionListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGenericCopyOptionListTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareGenericCopyOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGenericCopyOptionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareGenericCopyOptionValueTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGenericCopyOptionValueTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareGenericCopyOptionOrderListTransform(PEGTransformer &transformer,
	                                                       GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceGenericCopyOptionOrderListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareGenericCopyOptionExpressionTransform(PEGTransformer &transformer,
	                                                        GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceGenericCopyOptionExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareGenericCopyOptionParenthesizedExpressionListTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceGenericCopyOptionParenthesizedExpressionListTransform(PEGTransformer &transformer,
	                                                            GeneratedTransformProcess &process);
	static void PrepareCopyFromDatabaseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyFromDatabaseTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareCopyFromDatabaseWithFlagTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyFromDatabaseWithFlagTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PrepareCopyFromDatabaseWithoutFlagTransform(PEGTransformer &transformer,
	                                                        GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceCopyFromDatabaseWithoutFlagTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareCopyDatabaseFlagTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyDatabaseFlagTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareSchemaOrDataTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSchemaOrDataTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareCopySchemaTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopySchemaTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareCopyDataTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCopyDataTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareCreateIndexStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateIndexStmtTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareWithListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWithListTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareRelOptionOrOidsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRelOptionOrOidsTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareRelOptionListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRelOptionListTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareOidsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOidsTransform(PEGTransformer &transformer,
	                                                            GeneratedTransformProcess &process);
	static void PrepareWithOrWithoutOidsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWithOrWithoutOidsTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareWithOidsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWithOidsTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareWithoutOidsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWithoutOidsTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareIndexElementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIndexElementTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareUniqueIndexTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUniqueIndexTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareIndexTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIndexTypeTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareRelOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRelOptionTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareRelOptionNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRelOptionNameTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareDottedIdentifierStringTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDottedIdentifierStringTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareRelOptionArgumentOptTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRelOptionArgumentOptTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareDefArgTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDefArgTransform(PEGTransformer &transformer,
	                                                              GeneratedTransformProcess &process);
	static void PrepareDefArgNullTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDefArgNullTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareDefArgKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDefArgKeywordTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareDefArgStringLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDefArgStringLiteralTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareNoneLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNoneLiteralTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareCreateMacroStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateMacroStmtTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareMacroOrFunctionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMacroOrFunctionTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareMacroKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMacroKeywordTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareFunctionKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFunctionKeywordTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareMacroDefinitionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMacroDefinitionTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareMacroDefinitionBodyTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMacroDefinitionBodyTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareMacroParametersTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMacroParametersTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareMacroParameterTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMacroParameterTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareSimpleParameterTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSimpleParameterTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareScalarMacroDefinitionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceScalarMacroDefinitionTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareTableMacroDefinitionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableMacroDefinitionTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareCreateSchemaStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateSchemaStmtTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareCreateSecretStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateSecretStmtTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareSecretStorageSpecifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSecretStorageSpecifierTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareSecretNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSecretNameTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareCreateSequenceStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateSequenceStmtTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareSequenceOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSequenceOptionTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareSeqSetCycleTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSeqSetCycleTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareSeqCycleTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSeqCycleTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareSeqNoCycleTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSeqNoCycleTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareSeqSetIncrementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSeqSetIncrementTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareSeqSetMinMaxTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSeqSetMinMaxTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareSeqNoMinMaxTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSeqNoMinMaxTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareSeqStartWithTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSeqStartWithTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareSeqOwnedByTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSeqOwnedByTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareSeqMinOrMaxTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSeqMinOrMaxTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareMinValueTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMinValueTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareMaxValueTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMaxValueTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareCreateStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateStatementTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareCreateStatementVariationTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateStatementVariationTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PrepareOrReplaceTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOrReplaceTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareTemporaryTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTemporaryTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PreparePersistentTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePersistentTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareTempPersistentTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTempPersistentTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareTemporaryPersistentTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTemporaryPersistentTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareCreateTableStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateTableStmtTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareCreateTableDefinitionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateTableDefinitionTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareCreateTableAsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateTableAsTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PreparePartitionSortedOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePartitionSortedOptionsTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PreparePartitionOptSortedOptionsTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReducePartitionOptSortedOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareSortedOptPartitionOptionsTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceSortedOptPartitionOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PreparePartitionOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePartitionOptionsTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareSortedOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSortedOptionsTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareWithDataTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWithDataTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareWithDataOnlyTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWithDataOnlyTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareWithNoDataTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWithNoDataTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareIdentifierListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIdentifierListTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareCreateColumnListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateColumnListTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareIfNotExistsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIfNotExistsTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareQualifiedNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceQualifiedNameTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareSchemaReservedIdentifierOrStringLiteralTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceSchemaReservedIdentifierOrStringLiteralTransform(PEGTransformer &transformer,
	                                                       GeneratedTransformProcess &process);
	static void PrepareCatalogReservedSchemaIdentifierTransform(PEGTransformer &transformer,
	                                                            GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceCatalogReservedSchemaIdentifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareIdentifierOrStringLiteralTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceIdentifierOrStringLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareReservedIdentifierOrStringLiteralTransform(PEGTransformer &transformer,
	                                                              GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceReservedIdentifierOrStringLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareCatalogQualificationTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCatalogQualificationTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareSchemaQualificationTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSchemaQualificationTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareReservedSchemaQualificationTransform(PEGTransformer &transformer,
	                                                        GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceReservedSchemaQualificationTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareTableQualificationTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableQualificationTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareReservedTableQualificationTransform(PEGTransformer &transformer,
	                                                       GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceReservedTableQualificationTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareCreateTableColumnListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateTableColumnListTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareCreateTableColumnElementTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateTableColumnElementTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PrepareCreateTableColumnDefinitionTransform(PEGTransformer &transformer,
	                                                        GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceCreateTableColumnDefinitionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareCreateTableConstraintTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateTableConstraintTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareColumnDefinitionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColumnDefinitionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareColumnConstraintTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColumnConstraintTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareNotNullConstraintTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNotNullConstraintTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareNullConstraintTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNullConstraintTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareNotNullColumnConstraintTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNotNullColumnConstraintTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareUniqueConstraintTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUniqueConstraintTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PreparePrimaryKeyConstraintTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePrimaryKeyConstraintTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareDefaultValueTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDefaultValueTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareCheckConstraintTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCheckConstraintTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareForeignKeyConstraintTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceForeignKeyConstraintTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareColumnCollationTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColumnCollationTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareColumnCompressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColumnCompressionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareKeyActionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceKeyActionsTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareUpdateActionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUpdateActionTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareDeleteActionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDeleteActionTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareKeyActionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceKeyActionTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareNoKeyActionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNoKeyActionTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareRestrictKeyActionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRestrictKeyActionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareCascadeKeyActionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCascadeKeyActionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareSetNullKeyActionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetNullKeyActionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareSetDefaultKeyActionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetDefaultKeyActionTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareTopLevelConstraintTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTopLevelConstraintTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareTopLevelConstraintListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTopLevelConstraintListTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareTopCheckConstraintTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTopCheckConstraintTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareTopPrimaryKeyConstraintTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTopPrimaryKeyConstraintTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareTopUniqueConstraintTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTopUniqueConstraintTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareTopForeignKeyConstraintTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTopForeignKeyConstraintTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareColumnIdListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColumnIdListTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareDottedIdentifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDottedIdentifierTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareDotColLabelTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDotColLabelTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareIdentifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIdentifierTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareColIdTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColIdTransform(PEGTransformer &transformer,
	                                                             GeneratedTransformProcess &process);
	static void PrepareColIdOrStringTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColIdOrStringTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareTypeFuncNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTypeFuncNameTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareTypeFuncKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTypeFuncKeywordTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareColLabelTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColLabelTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareColLabelOrStringTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColLabelOrStringTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareColLabelIdentifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColLabelIdentifierTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareStringLiteralIdentifierTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceStringLiteralIdentifierTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareGeneratedColumnTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGeneratedColumnTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareGeneratedColumnTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGeneratedColumnTypeTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareCommitActionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommitActionTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PreparePreserveOrDeleteTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePreserveOrDeleteTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PreparePreserveRowsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePreserveRowsTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareDeleteRowsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDeleteRowsTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareVirtualGeneratedColumnTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceVirtualGeneratedColumnTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareStoredGeneratedColumnTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceStoredGeneratedColumnTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareCreateTriggerStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateTriggerStmtTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareTriggerBodyTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTriggerBodyTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareTriggerNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTriggerNameTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareReferencingClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceReferencingClauseTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareReferencingItemTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceReferencingItemTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareReferencingNewTableAsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceReferencingNewTableAsTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareReferencingOldTableAsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceReferencingOldTableAsTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareTriggerTimingTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTriggerTimingTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareTriggerBeforeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTriggerBeforeTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareTriggerAfterTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTriggerAfterTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareTriggerInsteadOfTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTriggerInsteadOfTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareTriggerEventTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTriggerEventTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareTriggerEventInsertTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTriggerEventInsertTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareTriggerEventDeleteTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTriggerEventDeleteTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareTriggerEventUpdateTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTriggerEventUpdateTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareTriggerEventUpdateOfTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTriggerEventUpdateOfTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareTriggerColumnListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTriggerColumnListTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareForEachClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceForEachClauseTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareForEachRowTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceForEachRowTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareForEachStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceForEachStatementTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareCreateTypeStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateTypeStmtTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareCreateTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateTypeTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareCreateTypeFromTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateTypeFromTypeTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareEnumSelectTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceEnumSelectTypeTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareEnumStringLiteralListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceEnumStringLiteralListTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareCreateViewStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateViewStmtTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareCreateRecursiveTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateRecursiveTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareCreateSecureTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCreateSecureTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareDeallocateStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDeallocateStatementTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareDeallocatePrepareTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDeallocatePrepareTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareDeleteStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDeleteStatementTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareTruncateStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTruncateStatementTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareTargetOptAliasTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTargetOptAliasTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareDeleteUsingClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDeleteUsingClauseTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareDescribeStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDescribeStatementTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareShowDeprecatedSelectTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceShowDeprecatedSelectTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareDescribeSelectTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDescribeSelectTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareShowAllTablesTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceShowAllTablesTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareShowTablesTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceShowTablesTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareShowByNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceShowByNameTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareDescribeByNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDescribeByNameTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareDescribeOrSummarizeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDescribeOrSummarizeTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareShowTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceShowTargetTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareShowDeprecatedQualifiedTableNameTransform(PEGTransformer &transformer,
	                                                             GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceShowDeprecatedQualifiedTableNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareShowSettingNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceShowSettingNameTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareDescribeTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDescribeTargetTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareDescribeBaseTableNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDescribeBaseTableNameTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareDescribeStringLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDescribeStringLiteralTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareSummarizeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSummarizeTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareSummarizeRuleTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSummarizeRuleTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareShowOrDescribeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceShowOrDescribeTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareShowRuleTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceShowRuleTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareDescribeRuleTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDescribeRuleTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareDescribeLongRuleTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDescribeLongRuleTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareDescRuleTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDescRuleTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareDetachStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDetachStatementTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareDropStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropStatementTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareDropEntriesTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropEntriesTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareDropTriggerTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropTriggerTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareDropTableTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropTableTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareDropTableFunctionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropTableFunctionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareDropFunctionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropFunctionTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareDropSchemaTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropSchemaTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareDropIndexTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropIndexTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareQualifiedIndexNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceQualifiedIndexNameTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareQualifiedIndexNameStringTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceQualifiedIndexNameStringTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PrepareSchemaReservedIndexTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSchemaReservedIndexTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareCatalogReservedSchemaIndexTransform(PEGTransformer &transformer,
	                                                       GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceCatalogReservedSchemaIndexTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareDropSequenceTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropSequenceTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareDropCollationTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropCollationTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareDropTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropTypeTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareDropSecretTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropSecretTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareTableOrViewTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableOrViewTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareMaterializedViewEntryTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMaterializedViewEntryTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareFunctionTypeMacroTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFunctionTypeMacroTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareFunctionTypeMacroKeywordTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFunctionTypeMacroKeywordTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PrepareFunctionTypeFunctionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFunctionTypeFunctionTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareDropBehaviorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropBehaviorTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareCascadeDropBehaviorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCascadeDropBehaviorTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareRestrictDropBehaviorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRestrictDropBehaviorTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareIfExistsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIfExistsTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareDropSecretStorageTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDropSecretStorageTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareExecuteStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExecuteStatementTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareExplainStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExplainStatementTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareExplainOptionListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExplainOptionListTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareExplainOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExplainOptionTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareExplainOptionNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExplainOptionNameTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareExplainSelectStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExplainSelectStatementTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareExplainableStatementsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExplainableStatementsTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareExportStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExportStatementTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareExportSourceTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExportSourceTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareImportStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceImportStatementTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareColumnReferenceTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColumnReferenceTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareNestedSchemaTableColumnNameTransform(PEGTransformer &transformer,
	                                                        GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceNestedSchemaTableColumnNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareCatalogReservedSchemaTableColumnNameTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceCatalogReservedSchemaTableColumnNameTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static void PrepareSchemaReservedTableColumnNameTransform(PEGTransformer &transformer,
	                                                          GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceSchemaReservedTableColumnNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareTableReservedColumnNameTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableReservedColumnNameTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareFunctionExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFunctionExpressionTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareFunctionExpressionArgumentsTransform(PEGTransformer &transformer,
	                                                        GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceFunctionExpressionArgumentsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareFunctionExpressionArgumentListTransform(PEGTransformer &transformer,
	                                                           GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceFunctionExpressionArgumentListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareFunctionArgumentListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFunctionArgumentListTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareFunctionIdentifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFunctionIdentifierTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareFunctionNameAsQualifiedNameTransform(PEGTransformer &transformer,
	                                                        GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceFunctionNameAsQualifiedNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareCatalogReservedSchemaFunctionNameTransform(PEGTransformer &transformer,
	                                                              GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceCatalogReservedSchemaFunctionNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareSchemaReservedFunctionNameTransform(PEGTransformer &transformer,
	                                                       GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceSchemaReservedFunctionNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareDistinctOrAllTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDistinctOrAllTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareDistinctKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDistinctKeywordTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareAllKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAllKeywordTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareWithinGroupClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWithinGroupClauseTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareFilterClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFilterClauseTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareFilterClauseExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFilterClauseExpressionTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareFilterClauseContentsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFilterClauseContentsTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareIgnoreOrRespectNullsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIgnoreOrRespectNullsTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareIgnoreNullsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIgnoreNullsTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareRespectNullsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRespectNullsTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareParenthesisExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceParenthesisExpressionTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareConstantLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceConstantLiteralTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareNullLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNullLiteralTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareTrueLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTrueLiteralTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareFalseLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFalseLiteralTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareCastExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCastExpressionTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareCastArgumentsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCastArgumentsTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareCastOrTryCastTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCastOrTryCastTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareCastKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCastKeywordTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareTryCastKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTryCastKeywordTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareColIdDotTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColIdDotTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareStarExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceStarExpressionTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareStarQualifierListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceStarQualifierListTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareExcludeListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExcludeListTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareExcludeNamesTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExcludeNamesTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareExcludeNameListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExcludeNameListTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareExcludeNameSingleTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExcludeNameSingleTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareExcludeNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExcludeNameTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareExcludeDottedNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExcludeDottedNameTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareExcludeColumnNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExcludeColumnNameTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareReplaceListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceReplaceListTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareReplaceEntriesTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceReplaceEntriesTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareReplaceEntrySingleTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceReplaceEntrySingleTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareReplaceEntryListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceReplaceEntryListTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareReplaceEntryTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceReplaceEntryTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareRenameListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRenameListTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareRenameEntriesTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRenameEntriesTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareRenameEntryListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRenameEntryListTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareSingleRenameEntryTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSingleRenameEntryTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareRenameEntryTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRenameEntryTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareSubqueryExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSubqueryExpressionTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareSubqueryNotTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSubqueryNotTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareSubqueryExistsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSubqueryExistsTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareCaseExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCaseExpressionTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareCaseWhenThenTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCaseWhenThenTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareCaseElseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCaseElseTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareTypeLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTypeLiteralTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareIntervalLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIntervalLiteralTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareIntervalParameterTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIntervalParameterTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareIntervalStringParameterTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIntervalStringParameterTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareFrameClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFrameClauseTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareFramingTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFramingTransform(PEGTransformer &transformer,
	                                                               GeneratedTransformProcess &process);
	static void PrepareRowsFramingTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRowsFramingTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareRangeFramingTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRangeFramingTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareGroupsFramingTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGroupsFramingTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareFrameExtentTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFrameExtentTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareSingleFrameExtentTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSingleFrameExtentTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareBetweenFrameExtentTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBetweenFrameExtentTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareFrameBoundTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFrameBoundTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareFrameUnboundedTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFrameUnboundedTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareFrameExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFrameExpressionTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareFrameCurrentRowTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFrameCurrentRowTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PreparePrecedingOrFollowingTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePrecedingOrFollowingTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PreparePrecedingFrameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePrecedingFrameTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareFollowingFrameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFollowingFrameTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareWindowExcludeClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWindowExcludeClauseTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareWindowExcludeElementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWindowExcludeElementTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareExcludeCurrentRowTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExcludeCurrentRowTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareExcludeGroupTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExcludeGroupTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareExcludeTiesTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExcludeTiesTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareExcludeNoOthersTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExcludeNoOthersTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareWindowFrameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWindowFrameTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareIdentifierWindowFrameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIdentifierWindowFrameTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareParensIdentifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceParensIdentifierTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareWindowFrameDefinitionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWindowFrameDefinitionTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareWindowFrameNameContentsParensTransform(PEGTransformer &transformer,
	                                                          GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceWindowFrameNameContentsParensTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareWindowFrameNameContentsTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWindowFrameNameContentsTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareWindowFrameContentsParensTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceWindowFrameContentsParensTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareWindowFrameContentsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWindowFrameContentsTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareBaseWindowNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBaseWindowNameTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareWindowPartitionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWindowPartitionTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareListExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceListExpressionTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareArrayBoundedListExpressionTransform(PEGTransformer &transformer,
	                                                       GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceArrayBoundedListExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareArrayParensSelectTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceArrayParensSelectTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareBoundedListExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBoundedListExpressionTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareStructExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceStructExpressionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareStructFieldTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceStructFieldTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareMapExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMapExpressionTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareMapStructExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMapStructExpressionTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareMapStructFieldTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMapStructFieldTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareGroupingExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGroupingExpressionTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareGroupingOrGroupingIdTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGroupingOrGroupingIdTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareGroupingKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGroupingKeywordTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareGroupingIdKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGroupingIdKeywordTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareParameterTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceParameterTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareQuestionMarkNumberedParameterTransform(PEGTransformer &transformer,
	                                                          GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceQuestionMarkNumberedParameterTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareAnonymousParameterTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAnonymousParameterTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareNumberedParameterTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNumberedParameterTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareColLabelParameterTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColLabelParameterTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PreparePositionalExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePositionalExpressionTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareDefaultExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDefaultExpressionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareListComprehensionExpressionTransform(PEGTransformer &transformer,
	                                                        GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceListComprehensionExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareListComprehensionFilterTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceListComprehensionFilterTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareParensExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceParensExpressionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareSingleExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSingleExpressionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExpressionTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareColumnDefaultExprTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColumnDefaultExprTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareLambdaArrowExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLambdaArrowExpressionTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareSingleArrowPairTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSingleArrowPairTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareLogicalOrExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLogicalOrExpressionTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareLogicalOrExpressionTailTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLogicalOrExpressionTailTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareColDefOrExprTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColDefOrExprTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareColDefOrExpressionTailTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColDefOrExpressionTailTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareLogicalAndExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLogicalAndExpressionTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareLogicalAndExpressionTailTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLogicalAndExpressionTailTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PrepareColDefAndExprTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColDefAndExprTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareColDefAndExpressionTailTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColDefAndExpressionTailTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareLogicalNotExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLogicalNotExpressionTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareNotExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNotExpressionTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareNotKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNotKeywordTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareIsExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIsExpressionTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareIsTestTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIsTestTransform(PEGTransformer &transformer,
	                                                              GeneratedTransformProcess &process);
	static void PrepareIsLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIsLiteralTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareIsLiteralValueTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIsLiteralValueTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareUnknownLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUnknownLiteralTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareNotNullTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNotNullTransform(PEGTransformer &transformer,
	                                                               GeneratedTransformProcess &process);
	static void PrepareNotNullKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNotNullKeywordTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareNotNullOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNotNullOperatorTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareIsNullTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIsNullTransform(PEGTransformer &transformer,
	                                                              GeneratedTransformProcess &process);
	static void PrepareIsNullOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIsNullOperatorTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareIsDistinctFromExpressionTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIsDistinctFromExpressionTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PrepareIsDistinctFromTailTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIsDistinctFromTailTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareIsDistinctFromOpTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIsDistinctFromOpTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareComparisonExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceComparisonExpressionTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareComparisonExpressionTailTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceComparisonExpressionTailTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PrepareComparisonOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceComparisonOperatorTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareOperatorEqualTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOperatorEqualTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareOperatorNotEqualTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOperatorNotEqualTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareOperatorLessThanTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOperatorLessThanTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareOperatorGreaterThanTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOperatorGreaterThanTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareOperatorLessThanEqualsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOperatorLessThanEqualsTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareOperatorGreaterThanEqualsTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceOperatorGreaterThanEqualsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareBetweenInLikeExpressionTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBetweenInLikeExpressionTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareBetweenInLikeOpTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBetweenInLikeOpTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareBetweenInLikeOpExpressionTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceBetweenInLikeOpExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareLikeClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLikeClauseTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareEscapeClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceEscapeClauseTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareLikeVariationsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLikeVariationsTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareLikeTokenTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLikeTokenTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareILikeTokenTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceILikeTokenTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareGlobTokenTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGlobTokenTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareSimilarToTokenTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSimilarToTokenTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareRegexMatchTokenTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRegexMatchTokenTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareRegexInsensitiveMatchTokenTransform(PEGTransformer &transformer,
	                                                       GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceRegexInsensitiveMatchTokenTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareNotILikeOpTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNotILikeOpTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareNotLikeOpTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNotLikeOpTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareNotRegexInsensitiveMatchOpTransform(PEGTransformer &transformer,
	                                                       GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceNotRegexInsensitiveMatchOpTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareNotSimilarToOpTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNotSimilarToOpTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareInClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInClauseTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareInExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInExpressionTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareInContainsExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInContainsExpressionTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareInExpressionListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInExpressionListTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareInSelectStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInSelectStatementTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareBetweenClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBetweenClauseTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareOtherOperatorExpressionTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOtherOperatorExpressionTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareOtherOperatorTailTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOtherOperatorTailTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareOtherOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOtherOperatorTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareAnyAllParsedOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAnyAllParsedOperatorTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareNamedOtherOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNamedOtherOperatorTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareOperatorLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOperatorLiteralTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareAnyAllOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAnyAllOperatorTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareAnyOrAllTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAnyOrAllTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareSubqueryAnyTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSubqueryAnyTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareSubqueryAllTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSubqueryAllTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareInetOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInetOperatorTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareJsonOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceJsonOperatorTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareListOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceListOperatorTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareStringOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceStringOperatorTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareQualifiedOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceQualifiedOperatorTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareQualifiedOperatorContentsTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceQualifiedOperatorContentsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareAnyOpTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAnyOpTransform(PEGTransformer &transformer,
	                                                             GeneratedTransformProcess &process);
	static void PrepareBitwiseExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBitwiseExpressionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareBitwiseExpressionTailTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBitwiseExpressionTailTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareBitOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBitOperatorTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareAdditiveExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAdditiveExpressionTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareAdditiveExpressionTailTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAdditiveExpressionTailTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareTermTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTermTransform(PEGTransformer &transformer,
	                                                            GeneratedTransformProcess &process);
	static void PrepareMultiplicativeExpressionTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMultiplicativeExpressionTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PrepareMultiplicativeExpressionTailTransform(PEGTransformer &transformer,
	                                                         GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceMultiplicativeExpressionTailTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareFactorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFactorTransform(PEGTransformer &transformer,
	                                                              GeneratedTransformProcess &process);
	static void PrepareExponentiationExpressionTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExponentiationExpressionTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PrepareExponentiationExpressionTailTransform(PEGTransformer &transformer,
	                                                         GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceExponentiationExpressionTailTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareExponentOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExponentOperatorTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareCollateExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCollateExpressionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareCollateExpressionTailTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCollateExpressionTailTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareAtTimeZoneExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAtTimeZoneExpressionTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareAtTimeZoneExpressionTailTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAtTimeZoneExpressionTailTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PreparePrefixOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePrefixOperatorTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareMinusPrefixOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMinusPrefixOperatorTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PreparePlusPrefixOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePlusPrefixOperatorTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareTildePrefixOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTildePrefixOperatorTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareBaseExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBaseExpressionTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareIndirectionListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIndirectionListTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareIndirectionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIndirectionTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareCastOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCastOperatorTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareDotOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDotOperatorTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareDotMethodOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDotMethodOperatorTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareDotColumnOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDotColumnOperatorTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareMethodExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMethodExpressionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareMethodExpressionArgumentsTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceMethodExpressionArgumentsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareMethodExpressionArgumentListTransform(PEGTransformer &transformer,
	                                                         GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceMethodExpressionArgumentListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareMethodFunctionArgumentsTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMethodFunctionArgumentsTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareSliceExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSliceExpressionTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareSliceBoundTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSliceBoundTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareEndSliceBoundTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceEndSliceBoundTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareEndSliceValueTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceEndSliceValueTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareEndSliceMinusTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceEndSliceMinusTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareStepSliceBoundTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceStepSliceBoundTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PreparePostfixOperatorTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePostfixOperatorTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareSpecialFunctionExpressionTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceSpecialFunctionExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareCoalesceExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCoalesceExpressionTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareUnpackExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUnpackExpressionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareTryExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTryExpressionTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareColumnsExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColumnsExpressionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareExtractExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExtractExpressionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareExtractArgumentsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExtractArgumentsTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareLambdaExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLambdaExpressionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareNullIfExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNullIfExpressionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareNullIfArgumentsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNullIfArgumentsTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PreparePositionExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePositionExpressionTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PreparePositionArgumentsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePositionArgumentsTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareRowExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRowExpressionTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareSubstringExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSubstringExpressionTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareSubstringArgumentsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSubstringArgumentsTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareSubstringExpressionListTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSubstringExpressionListTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareSubstringParametersTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSubstringParametersTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareSubstringFromForTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSubstringFromForTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareSubstringFromOptionalForTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSubstringFromOptionalForTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PrepareSubstringForTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSubstringForTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareTrimExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTrimExpressionTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareTrimArgumentsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTrimArgumentsTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareTrimDirectionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTrimDirectionTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareTrimBothTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTrimBothTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareTrimLeadingTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTrimLeadingTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareTrimTrailingTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTrimTrailingTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareTrimSourceTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTrimSourceTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareOverlayExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOverlayExpressionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareOverlayArgumentsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOverlayArgumentsTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareOverlayParametersTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOverlayParametersTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareFromExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFromExpressionTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareForExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceForExpressionTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareOverlayExpressionListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOverlayExpressionListTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareExtractArgumentTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExtractArgumentTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareExtractDatePartArgumentTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExtractDatePartArgumentTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareExtractIdentifierArgumentTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceExtractIdentifierArgumentTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareExtractStringArgumentTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExtractStringArgumentTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareExtractDatePartTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExtractDatePartTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareExternalResourceStatementTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceExternalResourceStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareCreateExternalResourceStmtTransform(PEGTransformer &transformer,
	                                                       GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceCreateExternalResourceStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareRegisterExternalResourceStmtTransform(PEGTransformer &transformer,
	                                                         GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceRegisterExternalResourceStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareDestroyExternalResourceStmtTransform(PEGTransformer &transformer,
	                                                        GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceDestroyExternalResourceStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareShowExternalResourcesStmtTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceShowExternalResourcesStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareShowAllModifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceShowAllModifierTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareExternalResourceCreationOptionsTransform(PEGTransformer &transformer,
	                                                            GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceExternalResourceCreationOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareInsertStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertStatementTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareOrActionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOrActionTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareInsertOrReplaceTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertOrReplaceTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareInsertOrIgnoreTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertOrIgnoreTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareByNameOrPositionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceByNameOrPositionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareInsertByNameOrderTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertByNameOrderTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareInsertByPositionOrderTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertByPositionOrderTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareInsertByNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertByNameTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareInsertByPositionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertByPositionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareInsertTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertTargetTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareInsertAliasTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertAliasTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareColumnListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColumnListTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareInsertColumnListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertColumnListTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareInsertValuesTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertValuesTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareSelectInsertValuesTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSelectInsertValuesTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareDefaultValuesTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDefaultValuesTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareOnConflictClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOnConflictClauseTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareOnConflictTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOnConflictTargetTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareOnConflictExpressionTargetTransform(PEGTransformer &transformer,
	                                                       GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceOnConflictExpressionTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareOnConflictIndexTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOnConflictIndexTargetTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareOnConflictActionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOnConflictActionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareOnConflictUpdateTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOnConflictUpdateTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareOnConflictNothingTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOnConflictNothingTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareReturningClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceReturningClauseTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareLoadStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLoadStatementTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareExtensionAliasTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExtensionAliasTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareInstallStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInstallStatementTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareInstallAndLoadTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInstallAndLoadTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareUpdateExtensionsStatementTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceUpdateExtensionsStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareFromSourceTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFromSourceTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareFromSourceIdentifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFromSourceIdentifierTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareFromSourceStringTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFromSourceStringTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareVersionNumberTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceVersionNumberTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareExtensionRepositoryStatementTransform(PEGTransformer &transformer,
	                                                         GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceExtensionRepositoryStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareCreateExtensionRepositoryStmtTransform(PEGTransformer &transformer,
	                                                          GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceCreateExtensionRepositoryStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareRepositoryPrefixTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRepositoryPrefixTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareRepositoryPublicKeyTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRepositoryPublicKeyTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareDropExtensionRepositoryStmtTransform(PEGTransformer &transformer,
	                                                        GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceDropExtensionRepositoryStmtTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareMergeIntoStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMergeIntoStatementTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareMergeIntoUsingClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMergeIntoUsingClauseTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareMergeMatchTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMergeMatchTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareMatchedClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMatchedClauseTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareMatchedClauseActionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMatchedClauseActionTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareUpdateMatchClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUpdateMatchClauseTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareUpdateMatchInfoTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUpdateMatchInfoTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareUpdateMatchSetActionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUpdateMatchSetActionTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareUpdateByNameOrPositionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUpdateByNameOrPositionTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareDeleteMatchClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDeleteMatchClauseTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareInsertMatchClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertMatchClauseTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareInsertMatchInfoTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertMatchInfoTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareInsertDefaultValuesTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertDefaultValuesTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareInsertByNameOrPositionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertByNameOrPositionTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareInsertValuesListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInsertValuesListTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareDoNothingMatchClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDoNothingMatchClauseTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareErrorMatchClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceErrorMatchClauseTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareUpdateMatchSetClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUpdateMatchSetClauseTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareUpdateMatchSetInfoTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUpdateMatchSetInfoTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareAndExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAndExpressionTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareNotMatchedClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNotMatchedClauseTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareBySourceOrTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBySourceOrTargetTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareBySourceTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBySourceTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareByTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceByTargetTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PreparePivotOnTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePivotOnTransform(PEGTransformer &transformer,
	                                                               GeneratedTransformProcess &process);
	static void PreparePivotUsingTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePivotUsingTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PreparePivotColumnListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePivotColumnListTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PreparePivotColumnEntryTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePivotColumnEntryTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PreparePivotColumnExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePivotColumnExpressionTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PreparePivotColumnSubqueryTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePivotColumnSubqueryTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareIntoNameValuesTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIntoNameValuesTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareIncludeOrExcludeNullsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIncludeOrExcludeNullsTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareIncludeNullsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIncludeNullsTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareExcludeNullsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExcludeNullsTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareUnpivotHeaderTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUnpivotHeaderTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareUnpivotHeaderSingleTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUnpivotHeaderSingleTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareUnpivotHeaderListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUnpivotHeaderListTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PreparePragmaStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePragmaStatementTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PreparePragmaAssignOrFunctionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePragmaAssignOrFunctionTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PreparePragmaAssignTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePragmaAssignTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PreparePragmaFunctionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePragmaFunctionTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PreparePragmaParametersTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePragmaParametersTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PreparePrepareStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePrepareStatementTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareTypeListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTypeListTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareSelectStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSelectStatementTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareSelectSetOpChainTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSelectSetOpChainTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareSelectSetOpChainTailTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSelectSetOpChainTailTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareIntersectChainTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIntersectChainTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareIntersectChainTailTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceIntersectChainTailTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareSetIntersectClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetIntersectClauseTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareSelectAtomTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSelectAtomTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareSelectParensTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSelectParensTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareSetopClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetopClauseTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareSetopTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetopTypeTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareSetopUnionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetopUnionTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareSetopExceptTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetopExceptTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareSelectStatementTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSelectStatementTypeTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareResultModifiersTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceResultModifiersTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareLimitOffsetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLimitOffsetTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareLimitOffsetClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLimitOffsetClauseTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareOffsetLimitClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOffsetLimitClauseTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareOffsetFetchClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOffsetFetchClauseTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareFetchOnlyClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFetchOnlyClauseTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareTableStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableStatementTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareOptionalParensSimpleSelectTransform(PEGTransformer &transformer,
	                                                       GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceOptionalParensSimpleSelectTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareSimpleSelectParensTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSimpleSelectParensTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareSelectFromTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSelectFromTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareSelectFromClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSelectFromClauseTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareFromSelectClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFromSelectClauseTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareWithStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWithStatementTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareCTEBodyTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCTEBodyTransform(PEGTransformer &transformer,
	                                                               GeneratedTransformProcess &process);
	static void PrepareCTESelectBodyTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCTESelectBodyTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareCTEDMLBodyTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCTEDMLBodyTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareUsingKeyTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUsingKeyTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareMaterializedTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceMaterializedTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareSelectClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSelectClauseTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareTargetListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTargetListTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareColumnAliasesTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColumnAliasesTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareDistinctClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDistinctClauseTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareDistinctAllTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDistinctAllTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareDistinctOnTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDistinctOnTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareDistinctOnTargetsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDistinctOnTargetsTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareInnerTableRefTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInnerTableRefTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareTableSubqueryTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableSubqueryTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareBaseTableRefTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBaseTableRefTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareTableAliasColonTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableAliasColonTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareValuesRefTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceValuesRefTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareParensTableRefTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceParensTableRefTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareJoinOrPivotTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceJoinOrPivotTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareTablePivotClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTablePivotClauseTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareTablePivotClauseBodyTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTablePivotClauseBodyTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PreparePivotGroupByListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePivotGroupByListTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareTableUnpivotClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableUnpivotClauseTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareTableUnpivotClauseBodyTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableUnpivotClauseBodyTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PreparePivotHeaderTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePivotHeaderTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PreparePivotValueListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePivotValueListTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PreparePivotValueTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePivotValueTargetTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PreparePivotEnumTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePivotEnumTargetTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PreparePivotListTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePivotListTargetTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareUnpivotValueListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUnpivotValueListTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PreparePivotTargetListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePivotTargetListTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareUnpivotTargetListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUnpivotTargetListTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareLateralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLateralTransform(PEGTransformer &transformer,
	                                                               GeneratedTransformProcess &process);
	static void PrepareBaseTableNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBaseTableNameTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareUnqualifiedBaseTableNameTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUnqualifiedBaseTableNameTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PrepareQualifiedTableNameTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceQualifiedTableNameTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareSchemaReservedTableTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSchemaReservedTableTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareCatalogReservedSchemaTableTransform(PEGTransformer &transformer,
	                                                       GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceCatalogReservedSchemaTableTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareTableFunctionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableFunctionTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareTableFunctionLateralOptTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableFunctionLateralOptTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareTableFunctionAliasColonTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableFunctionAliasColonTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareWithOrdinalityTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWithOrdinalityTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareQualifiedTableFunctionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceQualifiedTableFunctionTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareTableFunctionArgumentsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableFunctionArgumentsTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareFunctionArgumentTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFunctionArgumentTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareNamedFunctionArgumentTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNamedFunctionArgumentTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PreparePositionalFunctionArgumentTransform(PEGTransformer &transformer,
	                                                       GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReducePositionalFunctionArgumentTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareNamedParameterTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNamedParameterTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareTableAliasTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableAliasTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareTableAliasAsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableAliasAsTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareTableAliasWithoutAsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTableAliasWithoutAsTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareAtClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAtClauseTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareAtSpecifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAtSpecifierTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareAtUnitTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAtUnitTransform(PEGTransformer &transformer,
	                                                              GeneratedTransformProcess &process);
	static void PrepareVersionAtUnitTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceVersionAtUnitTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareTimestampAtUnitTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTimestampAtUnitTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareJoinClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceJoinClauseTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareNearestJoinClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNearestJoinClauseTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareNearestJoinAliasedTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNearestJoinAliasedTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareNearestJoinBareTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNearestJoinBareTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareNearestBareTableRefTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNearestBareTableRefTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareNearestValuesRefTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNearestValuesRefTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareNearestTableFunctionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNearestTableFunctionTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareNearestTableSubqueryTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNearestTableSubqueryTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareNearestBaseTableRefTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNearestBaseTableRefTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareNearestParensTableRefTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNearestParensTableRefTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareApproxOrExactTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceApproxOrExactTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareNearestApproxTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNearestApproxTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareNearestExactTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNearestExactTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareDistanceOrSimilarityTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDistanceOrSimilarityTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareNearestDistanceTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNearestDistanceTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareNearestSimilarityTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNearestSimilarityTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareRegularJoinClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRegularJoinClauseTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareJoinByClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceJoinByClauseTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareAsofTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAsofTransform(PEGTransformer &transformer,
	                                                            GeneratedTransformProcess &process);
	static void PrepareJoinWithoutOnClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceJoinWithoutOnClauseTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareJoinQualifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceJoinQualifierTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareOnClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOnClauseTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareUsingClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUsingClauseTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareJoinTypeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceJoinTypeTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareJoinPrefixTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceJoinPrefixTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareCrossJoinPrefixTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCrossJoinPrefixTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareNaturalJoinPrefixTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNaturalJoinPrefixTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PreparePositionalJoinPrefixTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReducePositionalJoinPrefixTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareFullJoinTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFullJoinTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareLeftJoinTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLeftJoinTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareRightJoinTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRightJoinTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareSemiJoinTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSemiJoinTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareAntiJoinTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAntiJoinTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareInnerJoinTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceInnerJoinTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareFromClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFromClauseTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareWhereClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWhereClauseTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareGroupByClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGroupByClauseTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareHavingClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceHavingClauseTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareQualifyClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceQualifyClauseTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareSampleClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSampleClauseTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareWindowClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceWindowClauseTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareSampleEntryTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSampleEntryTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareSampleEntryCountTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSampleEntryCountTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareSampleEntryFunctionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSampleEntryFunctionTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareSampleFunctionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSampleFunctionTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareSamplePropertiesTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSamplePropertiesTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareRepeatableSampleTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRepeatableSampleTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareSampleSeedTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSampleSeedTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareSampleCountTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSampleCountTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareSampleValueTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSampleValueTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareSampleUnitTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSampleUnitTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareSamplePercentageTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSamplePercentageTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareSampleRowsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSampleRowsTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareGroupByExpressionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGroupByExpressionsTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareGroupByAllTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGroupByAllTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareGroupByListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGroupByListTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareGroupByExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGroupByExpressionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareGroupByBaseExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGroupByBaseExpressionTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareEmptyGroupingItemTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceEmptyGroupingItemTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareCubeOrRollupClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCubeOrRollupClauseTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareCubeOrRollupTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCubeOrRollupTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareCubeKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCubeKeywordTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareRollupKeywordTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRollupKeywordTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareGroupingSetsClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGroupingSetsClauseTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareSubqueryReferenceTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSubqueryReferenceTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareOrderByExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOrderByExpressionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareDescOrAscTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDescOrAscTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareDescendingOrderTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDescendingOrderTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareAscendingOrderTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAscendingOrderTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareNullsFirstOrLastTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNullsFirstOrLastTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareNullsFirstTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNullsFirstTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareNullsLastTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNullsLastTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareOrderByClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOrderByClauseTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareOrderByExpressionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOrderByExpressionsTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareOrderByExpressionListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOrderByExpressionListTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareOrderByAllTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOrderByAllTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareLimitClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLimitClauseTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareOffsetClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOffsetClauseTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareOffsetValueTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOffsetValueTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareLimitValueTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLimitValueTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareLimitAllTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLimitAllTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareLimitLiteralPercentTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLimitLiteralPercentTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareLimitExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLimitExpressionTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareFetchClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFetchClauseTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareFetchValueTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceFetchValueTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareAliasedExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceAliasedExpressionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareColIdExpressionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceColIdExpressionTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareExpressionAsCollabelTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExpressionAsCollabelTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareExpressionOptIdentifierTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceExpressionOptIdentifierTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareValuesClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceValuesClauseTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareValuesExpressionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceValuesExpressionsTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareSetStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetStatementTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareSetAssignmentOrTimeZoneTransform(PEGTransformer &transformer,
	                                                    GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetAssignmentOrTimeZoneTransform(PEGTransformer &transformer,
	                                                                               GeneratedTransformProcess &process);
	static void PrepareResetStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceResetStatementTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareSetSchemaTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetSchemaTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareStandardAssignmentTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceStandardAssignmentTransform(PEGTransformer &transformer,
	                                                                          GeneratedTransformProcess &process);
	static void PrepareSetVariableOrSettingTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetVariableOrSettingTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareSetTimeZoneTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetTimeZoneTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareZoneValueTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceZoneValueTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareZoneLocalTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceZoneLocalTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareZoneDefaultTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceZoneDefaultTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareZoneStringLiteralTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceZoneStringLiteralTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareZoneIdentifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceZoneIdentifierTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareZoneIntervalWithIntervalTransform(PEGTransformer &transformer,
	                                                     GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceZoneIntervalWithIntervalTransform(PEGTransformer &transformer,
	                                                                                GeneratedTransformProcess &process);
	static void PrepareZoneIntervalWithPrecisionTransform(PEGTransformer &transformer,
	                                                      GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue>
	ReduceZoneIntervalWithPrecisionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static void PrepareSetSettingTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetSettingTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareSetVariableTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetVariableTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareVariableScopeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceVariableScopeTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareSettingScopeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSettingScopeTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareLocalScopeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceLocalScopeTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareSessionScopeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSessionScopeTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareGlobalScopeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceGlobalScopeTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareSetAssignmentTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSetAssignmentTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareVariableListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceVariableListTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareTransactionStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceTransactionStatementTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareBeginTransactionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBeginTransactionTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareRollbackTransactionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceRollbackTransactionTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareCommitTransactionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCommitTransactionTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareReadOrWriteTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceReadOrWriteTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareReadOnlyOrReadWriteTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceReadOnlyOrReadWriteTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareReadOnlyTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceReadOnlyTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	static void PrepareReadWriteTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceReadWriteTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareUpdateStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUpdateStatementTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareUpdateTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUpdateTargetTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareBaseTableSetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBaseTableSetTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareBaseTableAliasSetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceBaseTableAliasSetTransform(PEGTransformer &transformer,
	                                                                         GeneratedTransformProcess &process);
	static void PrepareUpdateAliasTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUpdateAliasTransform(PEGTransformer &transformer,
	                                                                   GeneratedTransformProcess &process);
	static void PrepareUpdateSetClauseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUpdateSetClauseTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareUpdateSetTupleTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUpdateSetTupleTransform(PEGTransformer &transformer,
	                                                                      GeneratedTransformProcess &process);
	static void PrepareUpdateSetElementListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUpdateSetElementListTransform(PEGTransformer &transformer,
	                                                                            GeneratedTransformProcess &process);
	static void PrepareUpdateSetElementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUpdateSetElementTransform(PEGTransformer &transformer,
	                                                                        GeneratedTransformProcess &process);
	static void PrepareUpdateSetColumnTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUpdateSetColumnTargetTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareUseStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUseStatementTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareUseTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUseTargetTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareSchemaNameAsUseTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceSchemaNameAsUseTargetTransform(PEGTransformer &transformer,
	                                                                             GeneratedTransformProcess &process);
	static void PrepareCatalogNameAsUseTargetTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceCatalogNameAsUseTargetTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareUseTargetCatalogSchemaTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceUseTargetCatalogSchemaTransform(PEGTransformer &transformer,
	                                                                              GeneratedTransformProcess &process);
	static void PrepareDotIdentifierTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceDotIdentifierTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareVacuumStatementTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceVacuumStatementTransform(PEGTransformer &transformer,
	                                                                       GeneratedTransformProcess &process);
	static void PrepareVacuumOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceVacuumOptionsTransform(PEGTransformer &transformer,
	                                                                     GeneratedTransformProcess &process);
	static void PrepareVacuumParensOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceVacuumParensOptionsTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareVacuumLegacyOptionsTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceVacuumLegacyOptionsTransform(PEGTransformer &transformer,
	                                                                           GeneratedTransformProcess &process);
	static void PrepareVacuumOptionTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceVacuumOptionTransform(PEGTransformer &transformer,
	                                                                    GeneratedTransformProcess &process);
	static void PrepareOptAnalyzeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOptAnalyzeTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareOptFullTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOptFullTransform(PEGTransformer &transformer,
	                                                               GeneratedTransformProcess &process);
	static void PrepareOptFreezeTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOptFreezeTransform(PEGTransformer &transformer,
	                                                                 GeneratedTransformProcess &process);
	static void PrepareOptVerboseTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceOptVerboseTransform(PEGTransformer &transformer,
	                                                                  GeneratedTransformProcess &process);
	static void PrepareNameListTransform(PEGTransformer &transformer, GeneratedTransformProcess &process);
	static unique_ptr<TransformResultValue> ReduceNameListTransform(PEGTransformer &transformer,
	                                                                GeneratedTransformProcess &process);
	//===--------------------------------------------------------------------===//
	// END GENERATED PROCESS RULES
	//===--------------------------------------------------------------------===//

	// Registration methods
	void RegisterCommon();
	void RegisterCreateTable();
	void RegisterExpression();
	void RegisterPivot();
	void RegisterSelect();
	void RegisterKeywordsAndIdentifiers();
	void RegisterGenerated();
	template <class FUNC>
	void Register(const string &rule_name, FUNC function) {
		auto &rule = grammar.GetMutableRule(rule_name);
		if (rule.transform_process) {
			throw InternalException("Rule %s already exists", rule_name);
		}
		grammar.SetTransformProcess(
		    rule_name,
		    [function](PEGTransformer &transformer, ParseResult &parse_result) -> unique_ptr<TransformProcess> {
			    transform_reduce_function_t reduce =
			        [function](PEGTransformer &transformer,
			                   ParseResult &parse_result) -> unique_ptr<TransformResultValue> {
				    auto result_value = function(transformer, parse_result);
				    return make_uniq<TypedTransformResult<decltype(result_value)>>(std::move(result_value));
			    };
			    return make_uniq<ReduceTransformProcess>(transformer, parse_result, std::move(reduce));
		    });
	}

	PEGTransformerFactory(const PEGTransformerFactory &) = delete;

	static unique_ptr<SQLStatement> TransformStatement(PEGTransformer &, ParseResult &list);
	static const case_insensitive_map_t<const TransformProcessInfo *> &GeneratedTransformProcessInfo();

	// common.gram
	static unique_ptr<ParsedExpression> TransformNumberLiteral(PEGTransformer &transformer, ParseResult &parse_result);
	static string TransformStringLiteral(PEGTransformer &transformer, ParseResult &parse_result);
	static DatePartSpecifier TransformIntervalToIntervalAsType(PEGTransformer &transformer, ParseResult &parse_result);

	static string ExtractFormat(const string &file_path);

	// create_table.gram
	static string TransformIdentifier(PEGTransformer &transformer, ParseResult &parse_result);

	// expression.gram
	static unique_ptr<ParsedExpression> TransformExpression(PEGTransformer &transformer, ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformPrefixExpression(PEGTransformer &transformer,
	                                                              ParseResult &parse_result);
	static unique_ptr<WindowExpression> TransformOverClause(PEGTransformer &transformer, ParseResult &parse_result);

	// pivot.gram
	static unique_ptr<SelectStatement> TransformPivotStatement(PEGTransformer &transformer, ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformUnpivotStatement(PEGTransformer &transformer,
	                                                             ParseResult &parse_result);

	// select.gram
	static unique_ptr<SelectStatement> TransformSelectStatementInternalRule(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformSimpleSelect(PEGTransformer &transformer, ParseResult &parse_result);

	static unique_ptr<TableRef> TransformTableRef(PEGTransformer &transformer, ParseResult &parse_result);

	static CommonTableExpressionMap TransformWithClause(PEGTransformer &transformer, ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformWindowDefinition(PEGTransformer &transformer,
	                                                              ParseResult &parse_result);
	static string TransformIdentifierOrKeyword(PEGTransformer &transformer, ParseResult &parse_result);

	//===--------------------------------------------------------------------===//
	// START GENERATED RULES
	//===--------------------------------------------------------------------===//
	static unique_ptr<TransformResultValue> TransformAlterStatementInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformAlterStatement(PEGTransformer &transformer,
	                                                        unique_ptr<AlterInfo> alter_options);
	static unique_ptr<TransformResultValue> TransformAlterOptionsInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformAlterTableStmtInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<AlterInfo> TransformAlterTableStmt(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                     unique_ptr<BaseTableRef> base_table_name,
	                                                     vector<unique_ptr<AlterTableInfo>> alter_table_options);
	static unique_ptr<TransformResultValue> TransformAlterSchemaStmtInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<AlterInfo> TransformAlterSchemaStmt(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                      const QualifiedName &qualified_name,
	                                                      unique_ptr<AlterTableInfo> rename_alter);
	static unique_ptr<TransformResultValue> TransformAlterTableOptionsInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformAddConstraintInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformAddConstraint(PEGTransformer &transformer,
	                                                         unique_ptr<Constraint> top_level_constraint);
	static unique_ptr<TransformResultValue> TransformAddColumnInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformAddColumn(PEGTransformer &transformer, const bool &has_result,
	                                                     const optional<bool> &if_not_exists,
	                                                     AddColumnEntry add_column_entry);
	static unique_ptr<TransformResultValue> TransformAddColumnEntryInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static AddColumnEntry TransformAddColumnEntry(PEGTransformer &transformer, const vector<string> &dotted_identifier,
	                                              const optional<LogicalType> &type,
	                                              optional<GeneratedColumnDefinition> generated_column,
	                                              optional<vector<ColumnConstraintEntry>> column_constraint);
	static unique_ptr<TransformResultValue> TransformDropColumnInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformDropColumn(PEGTransformer &transformer, const bool &has_result,
	                                                      const optional<bool> &if_exists,
	                                                      unique_ptr<ColumnRefExpression> nested_column_name,
	                                                      const optional<bool> &drop_behavior);
	static unique_ptr<TransformResultValue> TransformAlterColumnInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformAlterColumn(PEGTransformer &transformer, const bool &has_result,
	                                                       unique_ptr<ColumnRefExpression> nested_column_name,
	                                                       unique_ptr<AlterTableInfo> alter_column_entry);
	static unique_ptr<TransformResultValue> TransformRenameColumnInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformRenameColumn(PEGTransformer &transformer, const bool &has_result,
	                                                        unique_ptr<ColumnRefExpression> nested_column_name,
	                                                        const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformNestedColumnNameInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ColumnRefExpression> TransformNestedColumnName(PEGTransformer &transformer,
	                                                                 const optional<vector<Identifier>> &identifier_dot,
	                                                                 const Identifier &column_name);
	static unique_ptr<TransformResultValue> TransformIdentifierDotInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static Identifier TransformIdentifierDot(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformRenameAlterInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformRenameAlter(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformSetPartitionedByInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformSetPartitionedBy(PEGTransformer &transformer,
	                                                            vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformResetPartitionedByInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformResetPartitionedBy(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSetSortedByInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformSetSortedBy(PEGTransformer &transformer,
	                                                       vector<OrderByNode> order_by_expressions);
	static unique_ptr<TransformResultValue> TransformResetSortedByInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformResetSortedBy(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSetOptionsInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<AlterTableInfo>
	TransformSetOptions(PEGTransformer &transformer,
	                    case_insensitive_map_t<unique_ptr<ParsedExpression>> rel_option_list);
	static unique_ptr<TransformResultValue> TransformResetOptionsInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<AlterTableInfo>
	TransformResetOptions(PEGTransformer &transformer,
	                      case_insensitive_map_t<unique_ptr<ParsedExpression>> rel_option_list);
	static unique_ptr<TransformResultValue> TransformAlterColumnEntryInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformAddOrDropDefaultInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformAddDefaultInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformAddDefault(PEGTransformer &transformer,
	                                                      unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformDropDefaultInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformDropDefault(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformChangeNullabilityInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformChangeNullability(PEGTransformer &transformer,
	                                                             const string &drop_or_set);
	static unique_ptr<TransformResultValue> TransformDropOrSetInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDropNullabilityInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static string TransformDropNullability(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSetNullabilityInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static string TransformSetNullability(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformAlterTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformAlterType(PEGTransformer &transformer, const bool &has_result,
	                                                     const optional<LogicalType> &type,
	                                                     optional<unique_ptr<ParsedExpression>> using_expression);
	static unique_ptr<TransformResultValue> TransformUsingExpressionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformUsingExpression(PEGTransformer &transformer,
	                                                             unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformAlterViewStmtInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<AlterInfo> TransformAlterViewStmt(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                    unique_ptr<BaseTableRef> base_table_name,
	                                                    unique_ptr<AlterTableInfo> rename_alter);
	static unique_ptr<TransformResultValue> TransformAlterSequenceStmtInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<AlterInfo> TransformAlterSequenceStmt(PEGTransformer &transformer,
	                                                        const optional<bool> &if_exists,
	                                                        const QualifiedName &qualified_sequence_name,
	                                                        unique_ptr<AlterInfo> alter_sequence_options);
	static unique_ptr<TransformResultValue> TransformQualifiedSequenceNameInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static QualifiedName TransformQualifiedSequenceName(PEGTransformer &transformer,
	                                                    const optional<Identifier> &catalog_qualification,
	                                                    const optional<Identifier> &schema_qualification,
	                                                    const Identifier &sequence_name);
	static unique_ptr<TransformResultValue> TransformAlterSequenceOptionsInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformRenameAlterSequenceOptionsInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static unique_ptr<AlterInfo> TransformRenameAlterSequenceOptions(PEGTransformer &transformer,
	                                                                 unique_ptr<AlterTableInfo> rename_alter);
	static unique_ptr<TransformResultValue> TransformSetSequenceOptionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<AlterInfo>
	TransformSetSequenceOption(PEGTransformer &transformer,
	                           vector<pair<string, unique_ptr<SequenceOption>>> sequence_option);
	static unique_ptr<TransformResultValue> TransformAlterDatabaseStmtInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<AlterInfo> TransformAlterDatabaseStmt(PEGTransformer &transformer,
	                                                        const optional<bool> &if_exists,
	                                                        const Identifier &identifier,
	                                                        const Identifier &identifier_1);
	static unique_ptr<TransformResultValue> TransformAnalyzeStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformAnalyzeStatement(PEGTransformer &transformer,
	                                                          const Identifier &analyze_keyword,
	                                                          const optional<bool> &analyze_verbose,
	                                                          optional<AnalyzeTarget> analyze_target);
	static unique_ptr<TransformResultValue> TransformAnalyzeTargetInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static AnalyzeTarget TransformAnalyzeTarget(PEGTransformer &transformer, unique_ptr<BaseTableRef> base_table_name,
	                                            const optional<vector<string>> &name_list);
	static unique_ptr<TransformResultValue> TransformAnalyzeVerboseInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformAnalyzeVerbose(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformAttachStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformAttachStatement(PEGTransformer &transformer, const optional<bool> &or_replace,
	                         const optional<bool> &if_not_exists, const bool &has_result,
	                         unique_ptr<ParsedExpression> database_path, const optional<Identifier> &attach_alias,
	                         const optional<vector<GenericCopyOption>> &attach_options);
	static unique_ptr<TransformResultValue> TransformDatabasePathInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDatabasePath(PEGTransformer &transformer,
	                                                          unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformAttachAliasInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static Identifier TransformAttachAlias(PEGTransformer &transformer, const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformAttachOptionsInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static vector<GenericCopyOption> TransformAttachOptions(PEGTransformer &transformer,
	                                                        const vector<GenericCopyOption> &generic_copy_option_list);
	static unique_ptr<TransformResultValue> TransformCallStatementInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCallStatement(PEGTransformer &transformer,
	                                                       const QualifiedName &qualified_table_function,
	                                                       vector<FunctionArgument> table_function_arguments);
	static unique_ptr<TransformResultValue> TransformCheckpointStatementInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCheckpointStatement(PEGTransformer &transformer,
	                                                             const optional<bool> &checkpoint_force,
	                                                             const optional<Identifier> &catalog_name);
	static unique_ptr<TransformResultValue> TransformCheckpointForceInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static bool TransformCheckpointForce(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCommentStatement(PEGTransformer &transformer,
	                                                          const CatalogType &comment_on_type,
	                                                          const vector<string> &dotted_identifier,
	                                                          const Value &comment_value);
	static unique_ptr<TransformResultValue> TransformCommentOnTypeInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCommentTableInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static CatalogType TransformCommentTable(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentSequenceInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static CatalogType TransformCommentSequence(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentFunctionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static CatalogType TransformCommentFunction(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentMacroTableInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static CatalogType TransformCommentMacroTable(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentMacroInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static CatalogType TransformCommentMacro(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentViewInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static CatalogType TransformCommentView(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentDatabaseInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static CatalogType TransformCommentDatabase(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentIndexInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static CatalogType TransformCommentIndex(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentSchemaInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static CatalogType TransformCommentSchema(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentTypeInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static CatalogType TransformCommentType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentColumnInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static CatalogType TransformCommentColumn(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentValueInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformStringLiteralValueInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static Value TransformStringLiteralValue(PEGTransformer &transformer, const string &string_literal);
	static unique_ptr<TransformResultValue> TransformAnalyzeKeywordInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static Identifier TransformAnalyzeKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformExpressionStatementInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformExpressionStatement(PEGTransformer &transformer,
	                                                             vector<unique_ptr<ParsedExpression>> expression_alias);
	static unique_ptr<TransformResultValue> TransformExpressionAliasInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformConstraintNameInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static Identifier TransformConstraintName(PEGTransformer &transformer, const Identifier &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformCollationNameInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static Identifier TransformCollationName(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformTypeInternal(PEGTransformer &transformer,
	                                                              ParseResult &parse_result);
	static LogicalType TransformType(PEGTransformer &transformer, unique_ptr<ParsedExpression> type_variations,
	                                 const optional<vector<int64_t>> &array_bounds);
	static unique_ptr<TransformResultValue> TransformTypeVariationsInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSimpleTypeInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCharacterSimpleTypeInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformCharacterSimpleType(PEGTransformer &transformer,
	                             optional<vector<unique_ptr<ParsedExpression>>> type_modifiers);
	static unique_ptr<TransformResultValue> TransformQualifiedSimpleTypeInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformQualifiedSimpleType(PEGTransformer &transformer, const QualifiedName &qualified_type_name,
	                             optional<vector<unique_ptr<ParsedExpression>>> type_modifiers);
	static unique_ptr<TransformResultValue> TransformIntervalTypeInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIntervalIntervalInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIntervalWithSpecifierInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIntervalWithRangeSpecifierInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformIntervalWithRangeSpecifier(PEGTransformer &transformer,
	                                    const DatePartSpecifier &interval_to_interval_as_type);
	static unique_ptr<TransformResultValue> TransformIntervalWithSimpleSpecifierInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIntervalWithSimpleSpecifier(PEGTransformer &transformer,
	                                                                         const DatePartSpecifier &interval);
	static unique_ptr<TransformResultValue> TransformIntervalWithoutSpecifierInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIntervalWithoutSpecifier(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformYearKeywordInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static DatePartSpecifier TransformYearKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformMonthKeywordInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static DatePartSpecifier TransformMonthKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDayKeywordInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static DatePartSpecifier TransformDayKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformHourKeywordInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static DatePartSpecifier TransformHourKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformMinuteKeywordInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static DatePartSpecifier TransformMinuteKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSecondKeywordInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static DatePartSpecifier TransformSecondKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformMillisecondKeywordInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static DatePartSpecifier TransformMillisecondKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformMicrosecondKeywordInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static DatePartSpecifier TransformMicrosecondKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformWeekKeywordInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static DatePartSpecifier TransformWeekKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformQuarterKeywordInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static DatePartSpecifier TransformQuarterKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDecadeKeywordInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static DatePartSpecifier TransformDecadeKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCenturyKeywordInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static DatePartSpecifier TransformCenturyKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformMillenniumKeywordInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static DatePartSpecifier TransformMillenniumKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIntervalInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIntervalToIntervalInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformYearToMonthInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static DatePartSpecifier TransformYearToMonth(PEGTransformer &transformer, const DatePartSpecifier &year_keyword,
	                                              const DatePartSpecifier &month_keyword);
	static unique_ptr<TransformResultValue> TransformDayToHourInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static DatePartSpecifier TransformDayToHour(PEGTransformer &transformer, const DatePartSpecifier &day_keyword,
	                                            const DatePartSpecifier &hour_keyword);
	static unique_ptr<TransformResultValue> TransformDayToMinuteInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static DatePartSpecifier TransformDayToMinute(PEGTransformer &transformer, const DatePartSpecifier &day_keyword,
	                                              const DatePartSpecifier &minute_keyword);
	static unique_ptr<TransformResultValue> TransformDayToSecondInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static DatePartSpecifier TransformDayToSecond(PEGTransformer &transformer, const DatePartSpecifier &day_keyword,
	                                              const DatePartSpecifier &second_keyword);
	static unique_ptr<TransformResultValue> TransformHourToMinuteInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static DatePartSpecifier TransformHourToMinute(PEGTransformer &transformer, const DatePartSpecifier &hour_keyword,
	                                               const DatePartSpecifier &minute_keyword);
	static unique_ptr<TransformResultValue> TransformHourToSecondInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static DatePartSpecifier TransformHourToSecond(PEGTransformer &transformer, const DatePartSpecifier &hour_keyword,
	                                               const DatePartSpecifier &second_keyword);
	static unique_ptr<TransformResultValue> TransformMinuteToSecondInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static DatePartSpecifier TransformMinuteToSecond(PEGTransformer &transformer,
	                                                 const DatePartSpecifier &minute_keyword,
	                                                 const DatePartSpecifier &second_keyword);
	static unique_ptr<TransformResultValue> TransformBitTypeInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformBitType(PEGTransformer &transformer, const bool &has_result,
	                                                     optional<vector<unique_ptr<ParsedExpression>>> expression);
	static unique_ptr<TransformResultValue> TransformGeometryTypeInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformGeometryType(PEGTransformer &transformer,
	                                                          optional<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformVariantTypeInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformVariantType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNumericTypeInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSimpleNumericTypeInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformSimpleNumericType(PEGTransformer &transformer, const string &child);
	static unique_ptr<TransformResultValue> TransformDecimalNumericTypeInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIntTypeInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static string TransformIntType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIntegerTypeInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static string TransformIntegerType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSmallintTypeInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static string TransformSmallintType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformBigintTypeInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static string TransformBigintType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRealTypeInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static string TransformRealType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformBooleanTypeInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static string TransformBooleanType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDoubleTypeInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static string TransformDoubleType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFloatTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformFloatType(PEGTransformer &transformer,
	                                                       optional<unique_ptr<ParsedExpression>> number_literal);
	static unique_ptr<TransformResultValue> TransformDecimalTypeInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformDecimalType(PEGTransformer &transformer, optional<vector<unique_ptr<ParsedExpression>>> type_modifiers);
	static unique_ptr<TransformResultValue> TransformDecTypeInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDecType(PEGTransformer &transformer,
	                                                     optional<vector<unique_ptr<ParsedExpression>>> type_modifiers);
	static unique_ptr<TransformResultValue> TransformNumericModTypeInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformNumericModType(PEGTransformer &transformer, optional<vector<unique_ptr<ParsedExpression>>> type_modifiers);
	static unique_ptr<TransformResultValue> TransformQualifiedTypeNameInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTypeNameAsQualifiedNameInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static QualifiedName TransformTypeNameAsQualifiedName(PEGTransformer &transformer, const Identifier &type_name);
	static unique_ptr<TransformResultValue> TransformCatalogReservedSchemaTypeNameInternal(PEGTransformer &transformer,
	                                                                                       ParseResult &parse_result);
	static QualifiedName TransformCatalogReservedSchemaTypeName(PEGTransformer &transformer,
	                                                            const Identifier &catalog_qualification,
	                                                            const vector<Identifier> &reserved_schema_qualification,
	                                                            const Identifier &reserved_type_name);
	static unique_ptr<TransformResultValue> TransformSchemaReservedTypeNameInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static QualifiedName TransformSchemaReservedTypeName(PEGTransformer &transformer,
	                                                     const Identifier &schema_qualification,
	                                                     const Identifier &reserved_type_name);
	static unique_ptr<TransformResultValue> TransformTypeModifiersInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformTypeModifiers(PEGTransformer &transformer, optional<vector<unique_ptr<ParsedExpression>>> expression);
	static unique_ptr<TransformResultValue> TransformRowTypeInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformRowType(PEGTransformer &transformer,
	                                                     const optional<child_list_t<LogicalType>> &col_id_type_list);
	static unique_ptr<TransformResultValue> TransformSetofTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformSetofType(PEGTransformer &transformer, const LogicalType &type);
	static unique_ptr<TransformResultValue> TransformUnionTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformUnionType(PEGTransformer &transformer,
	                                                       const child_list_t<LogicalType> &col_id_type_list);
	static unique_ptr<TransformResultValue> TransformColIdTypeListInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static child_list_t<LogicalType> TransformColIdTypeList(PEGTransformer &transformer,
	                                                        const vector<pair<Identifier, LogicalType>> &col_id_type);
	static unique_ptr<TransformResultValue> TransformMapTypeInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformMapType(PEGTransformer &transformer,
	                                                     const optional<vector<LogicalType>> &type);
	static unique_ptr<TransformResultValue> TransformTupleTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformTupleType(PEGTransformer &transformer,
	                                                       const vector<LogicalType> &type);
	static unique_ptr<TransformResultValue> TransformColIdTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static pair<Identifier, LogicalType> TransformColIdType(PEGTransformer &transformer, const Identifier &col_id,
	                                                        const LogicalType &type);
	static unique_ptr<TransformResultValue> TransformArrayBoundsInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformArrayKeywordInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static int64_t TransformArrayKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformArrayKeywordWithBoundsInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static int64_t TransformArrayKeywordWithBounds(PEGTransformer &transformer, const int64_t &square_brackets_array);
	static unique_ptr<TransformResultValue> TransformSquareBracketsArrayInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static int64_t TransformSquareBracketsArray(PEGTransformer &transformer,
	                                            optional<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformTimeTypeInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformTimeType(PEGTransformer &transformer,
	                                                      const LogicalTypeId &time_or_timestamp,
	                                                      optional<vector<unique_ptr<ParsedExpression>>> type_modifiers,
	                                                      const optional<bool> &time_zone);
	static unique_ptr<TransformResultValue> TransformTimeOrTimestampInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTimeTypeIdInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static LogicalTypeId TransformTimeTypeId(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTimestampTypeIdInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static LogicalTypeId TransformTimestampTypeId(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTimeZoneInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static bool TransformTimeZone(PEGTransformer &transformer, const bool &with_or_without);
	static unique_ptr<TransformResultValue> TransformWithOrWithoutInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformWithRuleInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static bool TransformWithRule(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformWithoutRuleInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformWithoutRule(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformConnectStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformConnectStatement(PEGTransformer &transformer,
	                                                          optional<unique_ptr<ConnectInfo>> session_target);
	static unique_ptr<TransformResultValue> TransformDisconnectStatementInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformDisconnectStatement(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSessionTargetInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLocalSessionTargetInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ConnectInfo> TransformLocalSessionTarget(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformStringSessionTargetInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<ConnectInfo>
	TransformStringSessionTarget(PEGTransformer &transformer, const string &string_literal,
	                             const optional<vector<GenericCopyOption>> &generic_copy_option_list);
	static unique_ptr<TransformResultValue> TransformCatalogSessionTargetInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ConnectInfo> TransformCatalogSessionTarget(PEGTransformer &transformer,
	                                                             const Identifier &catalog_name);
	static unique_ptr<TransformResultValue> TransformCopyStatementInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCopyStatement(PEGTransformer &transformer,
	                                                       unique_ptr<SQLStatement> copy_variations);
	static unique_ptr<TransformResultValue> TransformCopyVariationsInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCopyTableInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCopyTable(PEGTransformer &transformer,
	                                                   unique_ptr<BaseTableRef> base_table_name,
	                                                   const optional<vector<string>> &insert_column_list,
	                                                   const bool &from_or_to,
	                                                   unique_ptr<ParsedExpression> copy_file_name,
	                                                   const optional<vector<GenericCopyOption>> &copy_options);
	static unique_ptr<TransformResultValue> TransformFromOrToInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCopyFromInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static bool TransformCopyFrom(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCopyToInternal(PEGTransformer &transformer,
	                                                                ParseResult &parse_result);
	static bool TransformCopyTo(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCopySelectInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCopySelect(PEGTransformer &transformer,
	                                                    unique_ptr<SelectStatement> select_statement_internal,
	                                                    unique_ptr<ParsedExpression> copy_file_name,
	                                                    const optional<vector<GenericCopyOption>> &copy_options);
	static unique_ptr<TransformResultValue> TransformCopyFileNameInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCopyFileNameExpressionInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCopyFileNameStringLiteralInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformCopyFileNameStringLiteral(PEGTransformer &transformer,
	                                                                       const string &string_literal);
	static unique_ptr<TransformResultValue> TransformCopyFileNameIdentifierInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformCopyFileNameIdentifier(PEGTransformer &transformer,
	                                                                    const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformCopyFileNameIdentifierColIdInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformCopyFileNameIdentifierColId(PEGTransformer &transformer,
	                                                                         const Identifier &identifier_col_id);
	static unique_ptr<TransformResultValue> TransformIdentifierColIdInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static Identifier TransformIdentifierColId(PEGTransformer &transformer, const Identifier &identifier,
	                                           const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformCopyOptionsInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static vector<GenericCopyOption> TransformCopyOptions(PEGTransformer &transformer, const bool &has_result,
	                                                      const vector<GenericCopyOption> &copy_option_list);
	static unique_ptr<TransformResultValue> TransformCopyOptionListInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSpecializedOptionListInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static vector<GenericCopyOption>
	TransformSpecializedOptionList(PEGTransformer &transformer, const GenericCopyOption &specialized_option,
	                               const optional<vector<GenericCopyOption>> &specialized_option_tail);
	static unique_ptr<TransformResultValue> TransformSpecializedOptionTailInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static GenericCopyOption TransformSpecializedOptionTail(PEGTransformer &transformer, const bool &has_result,
	                                                        const GenericCopyOption &specialized_option);
	static unique_ptr<TransformResultValue> TransformSpecializedOptionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSingleOptionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformBinaryOptionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static GenericCopyOption TransformBinaryOption(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFreezeOptionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static GenericCopyOption TransformFreezeOption(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOidsOptionInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static GenericCopyOption TransformOidsOption(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCsvOptionInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static GenericCopyOption TransformCsvOption(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformHeaderOptionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static GenericCopyOption TransformHeaderOption(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNullAsOptionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static GenericCopyOption TransformNullAsOption(PEGTransformer &transformer, const bool &has_result,
	                                               const string &string_literal);
	static unique_ptr<TransformResultValue> TransformDelimiterAsOptionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static GenericCopyOption TransformDelimiterAsOption(PEGTransformer &transformer, const bool &has_result,
	                                                    const string &string_literal);
	static unique_ptr<TransformResultValue> TransformQuoteAsOptionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static GenericCopyOption TransformQuoteAsOption(PEGTransformer &transformer, const bool &has_result,
	                                                const string &string_literal);
	static unique_ptr<TransformResultValue> TransformEscapeAsOptionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static GenericCopyOption TransformEscapeAsOption(PEGTransformer &transformer, const bool &has_result,
	                                                 const string &string_literal);
	static unique_ptr<TransformResultValue> TransformEncodingOptionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static GenericCopyOption TransformEncodingOption(PEGTransformer &transformer, const string &string_literal);
	static unique_ptr<TransformResultValue> TransformForceQuoteOptionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static GenericCopyOption TransformForceQuoteOption(PEGTransformer &transformer, const optional<bool> &force_quote,
	                                                   const vector<string> &star_symbol_column_list);
	static unique_ptr<TransformResultValue> TransformStarSymbolColumnListInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformForceQuoteInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static bool TransformForceQuote(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformPartitionByOptionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static GenericCopyOption TransformPartitionByOption(PEGTransformer &transformer,
	                                                    const vector<string> &partition_by_column_list);
	static unique_ptr<TransformResultValue> TransformPartitionByColumnListInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformStarPartitionByColumnListInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static vector<string> TransformStarPartitionByColumnList(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue>
	TransformParenthesizedPartitionByColumnListInternal(PEGTransformer &transformer, ParseResult &parse_result);
	static vector<string> TransformParenthesizedPartitionByColumnList(PEGTransformer &transformer,
	                                                                  const vector<string> &column_list);
	static unique_ptr<TransformResultValue> TransformSinglePartitionByColumnListInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static vector<string> TransformSinglePartitionByColumnList(PEGTransformer &transformer, const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformForceNullOptionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static GenericCopyOption TransformForceNullOption(PEGTransformer &transformer, const optional<bool> &force_not_null,
	                                                  const vector<string> &column_list);
	static unique_ptr<TransformResultValue> TransformForceNotNullInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformForceNotNull(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCopyGenericOptionListInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static vector<GenericCopyOption>
	TransformCopyGenericOptionList(PEGTransformer &transformer, const vector<GenericCopyOption> &copy_generic_option);
	static unique_ptr<TransformResultValue> TransformCopyGenericOptionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOrderByCopyOptionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static GenericCopyOption TransformOrderByCopyOption(PEGTransformer &transformer,
	                                                    optional<GenericCopyOptionValue> generic_copy_option_value);
	static unique_ptr<TransformResultValue> TransformPartitionedByCopyOptionInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static GenericCopyOption
	TransformPartitionedByCopyOption(PEGTransformer &transformer,
	                                 optional<GenericCopyOptionValue> generic_copy_option_value);
	static unique_ptr<TransformResultValue> TransformGenericCopyOptionListInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static vector<GenericCopyOption>
	TransformGenericCopyOptionList(PEGTransformer &transformer, const vector<GenericCopyOption> &generic_copy_option);
	static unique_ptr<TransformResultValue> TransformGenericCopyOptionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static GenericCopyOption TransformGenericCopyOption(PEGTransformer &transformer, const Identifier &copy_option_name,
	                                                    optional<GenericCopyOptionValue> generic_copy_option_value);
	static unique_ptr<TransformResultValue> TransformGenericCopyOptionValueInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformGenericCopyOptionOrderListInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static GenericCopyOptionValue
	TransformGenericCopyOptionOrderList(PEGTransformer &transformer,
	                                    vector<OrderByNode> generic_copy_option_parenthesized_expression_list);
	static unique_ptr<TransformResultValue> TransformGenericCopyOptionExpressionInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static GenericCopyOptionValue TransformGenericCopyOptionExpression(PEGTransformer &transformer,
	                                                                   unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue>
	TransformGenericCopyOptionParenthesizedExpressionListInternal(PEGTransformer &transformer,
	                                                              ParseResult &parse_result);
	static vector<OrderByNode>
	TransformGenericCopyOptionParenthesizedExpressionList(PEGTransformer &transformer,
	                                                      vector<OrderByNode> order_by_expression_list);
	static unique_ptr<TransformResultValue> TransformCopyFromDatabaseInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCopyFromDatabaseWithFlagInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCopyFromDatabaseWithFlag(PEGTransformer &transformer,
	                                                                  const Identifier &col_id,
	                                                                  const Identifier &col_id_1,
	                                                                  const CopyDatabaseType &copy_database_flag);
	static unique_ptr<TransformResultValue> TransformCopyFromDatabaseWithoutFlagInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCopyFromDatabaseWithoutFlag(PEGTransformer &transformer,
	                                                                     const Identifier &col_id,
	                                                                     const Identifier &col_id_1);
	static unique_ptr<TransformResultValue> TransformCopyDatabaseFlagInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static CopyDatabaseType TransformCopyDatabaseFlag(PEGTransformer &transformer,
	                                                  const CopyDatabaseType &schema_or_data);
	static unique_ptr<TransformResultValue> TransformSchemaOrDataInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCopySchemaInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static CopyDatabaseType TransformCopySchema(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCopyDataInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static CopyDatabaseType TransformCopyData(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCreateIndexStmtInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<CreateStatement>
	TransformCreateIndexStmt(PEGTransformer &transformer, const optional<bool> &unique_index,
	                         const optional<bool> &if_not_exists, const optional<Identifier> &index_name,
	                         unique_ptr<BaseTableRef> base_table_name,
	                         const optional<vector<string>> &insert_column_list, const optional<Identifier> &index_type,
	                         optional<vector<unique_ptr<ParsedExpression>>> index_element,
	                         optional<case_insensitive_map_t<unique_ptr<ParsedExpression>>> with_list,
	                         optional<unique_ptr<ParsedExpression>> where_clause);
	static unique_ptr<TransformResultValue> TransformWithListInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static case_insensitive_map_t<unique_ptr<ParsedExpression>>
	TransformWithList(PEGTransformer &transformer,
	                  case_insensitive_map_t<unique_ptr<ParsedExpression>> rel_option_or_oids);
	static unique_ptr<TransformResultValue> TransformRelOptionOrOidsInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformRelOptionListInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static case_insensitive_map_t<unique_ptr<ParsedExpression>>
	TransformRelOptionList(PEGTransformer &transformer,
	                       vector<pair<Identifier, unique_ptr<ParsedExpression>>> rel_option);
	static unique_ptr<TransformResultValue> TransformOidsInternal(PEGTransformer &transformer,
	                                                              ParseResult &parse_result);
	static case_insensitive_map_t<unique_ptr<ParsedExpression>> TransformOids(PEGTransformer &transformer,
	                                                                          const bool &with_or_without_oids);
	static unique_ptr<TransformResultValue> TransformWithOrWithoutOidsInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformWithOidsInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static bool TransformWithOids(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformWithoutOidsInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformWithoutOids(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIndexElementInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIndexElement(PEGTransformer &transformer,
	                                                          unique_ptr<ParsedExpression> expression,
	                                                          const optional<OrderType> &desc_or_asc,
	                                                          const optional<OrderByNullType> &nulls_first_or_last);
	static unique_ptr<TransformResultValue> TransformUniqueIndexInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformUniqueIndex(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIndexTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static Identifier TransformIndexType(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformRelOptionInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static pair<Identifier, unique_ptr<ParsedExpression>>
	TransformRelOption(PEGTransformer &transformer, const Identifier &rel_option_name,
	                   optional<unique_ptr<ParsedExpression>> rel_option_argument_opt);
	static unique_ptr<TransformResultValue> TransformRelOptionNameInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static Identifier TransformRelOptionName(PEGTransformer &transformer, const string &child);
	static unique_ptr<TransformResultValue> TransformDottedIdentifierStringInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static string TransformDottedIdentifierString(PEGTransformer &transformer, const vector<string> &dotted_identifier);
	static unique_ptr<TransformResultValue> TransformRelOptionArgumentOptInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformRelOptionArgumentOpt(PEGTransformer &transformer,
	                                                                  unique_ptr<ParsedExpression> def_arg);
	static unique_ptr<TransformResultValue> TransformDefArgInternal(PEGTransformer &transformer,
	                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDefArgNullInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDefArgNull(PEGTransformer &transformer, const Value &null_literal);
	static unique_ptr<TransformResultValue> TransformDefArgKeywordInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDefArgKeyword(PEGTransformer &transformer,
	                                                           const string &reserved_keyword);
	static unique_ptr<TransformResultValue> TransformDefArgStringLiteralInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDefArgStringLiteral(PEGTransformer &transformer,
	                                                                 const string &string_literal);
	static unique_ptr<TransformResultValue> TransformNoneLiteralInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformNoneLiteral(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCreateMacroStmtInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<CreateStatement> TransformCreateMacroStmt(PEGTransformer &transformer,
	                                                            const bool &macro_or_function,
	                                                            const optional<bool> &if_not_exists,
	                                                            const QualifiedName &qualified_name,
	                                                            vector<unique_ptr<MacroFunction>> macro_definition);
	static unique_ptr<TransformResultValue> TransformMacroOrFunctionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformMacroKeywordInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformMacroKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFunctionKeywordInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static bool TransformFunctionKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformMacroDefinitionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<MacroFunction> TransformMacroDefinition(PEGTransformer &transformer,
	                                                          optional<vector<MacroParameter>> macro_parameters,
	                                                          unique_ptr<MacroFunction> macro_definition_body);
	static unique_ptr<TransformResultValue> TransformMacroDefinitionBodyInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformMacroParametersInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static vector<MacroParameter> TransformMacroParameters(PEGTransformer &transformer,
	                                                       vector<MacroParameter> macro_parameter);
	static unique_ptr<TransformResultValue> TransformMacroParameterInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSimpleParameterInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static MacroParameter TransformSimpleParameter(PEGTransformer &transformer, const Identifier &type_func_name,
	                                               const optional<LogicalType> &type);
	static unique_ptr<TransformResultValue> TransformScalarMacroDefinitionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<MacroFunction> TransformScalarMacroDefinition(PEGTransformer &transformer,
	                                                                unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformTableMacroDefinitionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<MacroFunction>
	TransformTableMacroDefinition(PEGTransformer &transformer, unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformCreateSchemaStmtInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<CreateStatement> TransformCreateSchemaStmt(PEGTransformer &transformer,
	                                                             const optional<bool> &if_not_exists,
	                                                             const QualifiedName &qualified_name);
	static unique_ptr<TransformResultValue> TransformCreateSecretStmtInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<CreateStatement>
	TransformCreateSecretStmt(PEGTransformer &transformer, const optional<bool> &if_not_exists,
	                          const optional<Identifier> &secret_name,
	                          const optional<Identifier> &secret_storage_specifier,
	                          const vector<GenericCopyOption> &generic_copy_option_list);
	static unique_ptr<TransformResultValue> TransformSecretStorageSpecifierInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static Identifier TransformSecretStorageSpecifier(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformSecretNameInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static Identifier TransformSecretName(PEGTransformer &transformer, const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformCreateSequenceStmtInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<CreateStatement>
	TransformCreateSequenceStmt(PEGTransformer &transformer, const optional<bool> &if_not_exists,
	                            const QualifiedName &qualified_name,
	                            optional<vector<pair<string, unique_ptr<SequenceOption>>>> sequence_option);
	static unique_ptr<TransformResultValue> TransformSequenceOptionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSeqSetCycleInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSeqCycleInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqCycle(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSeqNoCycleInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqNoCycle(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSeqSetIncrementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqSetIncrement(PEGTransformer &transformer,
	                                                                         const bool &has_result,
	                                                                         unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformSeqSetMinMaxInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqSetMinMax(PEGTransformer &transformer,
	                                                                      const string &seq_min_or_max,
	                                                                      unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformSeqNoMinMaxInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqNoMinMax(PEGTransformer &transformer,
	                                                                     const string &seq_min_or_max);
	static unique_ptr<TransformResultValue> TransformSeqStartWithInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static pair<string, unique_ptr<SequenceOption>>
	TransformSeqStartWith(PEGTransformer &transformer, const bool &has_result, unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformSeqOwnedByInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqOwnedBy(PEGTransformer &transformer,
	                                                                    const QualifiedName &qualified_name);
	static unique_ptr<TransformResultValue> TransformSeqMinOrMaxInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformMinValueInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static string TransformMinValue(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformMaxValueInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static string TransformMaxValue(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCreateStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCreateStatement(PEGTransformer &transformer,
	                                                         const optional<bool> &or_replace,
	                                                         const optional<SecretPersistType> &temporary,
	                                                         unique_ptr<CreateStatement> create_statement_variation);
	static unique_ptr<TransformResultValue> TransformCreateStatementVariationInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOrReplaceInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static bool TransformOrReplace(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTemporaryInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPersistentInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static SecretPersistType TransformPersistent(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTempPersistentInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static SecretPersistType TransformTempPersistent(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTemporaryPersistentInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static SecretPersistType TransformTemporaryPersistent(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCreateTableStmtInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<CreateStatement> TransformCreateTableStmt(PEGTransformer &transformer,
	                                                            const optional<bool> &if_not_exists,
	                                                            const QualifiedName &qualified_name,
	                                                            CreateTableDefinition create_table_definition,
	                                                            const optional<bool> &commit_action);
	static unique_ptr<TransformResultValue> TransformCreateTableDefinitionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCreateTableAsInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static CreateTableDefinition
	TransformCreateTableAs(PEGTransformer &transformer, optional<ColumnList> identifier_list,
	                       optional<PartitionSortedOptions> partition_sorted_options,
	                       optional<case_insensitive_map_t<unique_ptr<ParsedExpression>>> with_list,
	                       unique_ptr<SQLStatement> statement, const optional<bool> &with_data);
	static unique_ptr<TransformResultValue> TransformPartitionSortedOptionsInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPartitionOptSortedOptionsInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static PartitionSortedOptions
	TransformPartitionOptSortedOptions(PEGTransformer &transformer,
	                                   vector<unique_ptr<ParsedExpression>> partition_options,
	                                   optional<vector<unique_ptr<ParsedExpression>>> sorted_options);
	static unique_ptr<TransformResultValue> TransformSortedOptPartitionOptionsInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static PartitionSortedOptions
	TransformSortedOptPartitionOptions(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> sorted_options,
	                                   optional<vector<unique_ptr<ParsedExpression>>> partition_options);
	static unique_ptr<TransformResultValue> TransformPartitionOptionsInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformPartitionOptions(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformSortedOptionsInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformSortedOptions(PEGTransformer &transformer,
	                                                                   vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformWithDataInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformWithDataOnlyInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformWithDataOnly(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformWithNoDataInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static bool TransformWithNoData(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIdentifierListInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static ColumnList TransformIdentifierList(PEGTransformer &transformer, const vector<Identifier> &identifier);
	static unique_ptr<TransformResultValue> TransformCreateColumnListInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static CreateTableDefinition
	TransformCreateColumnList(PEGTransformer &transformer, optional<ColumnElements> create_table_column_list,
	                          optional<PartitionSortedOptions> partition_sorted_options,
	                          optional<case_insensitive_map_t<unique_ptr<ParsedExpression>>> with_list);
	static unique_ptr<TransformResultValue> TransformIfNotExistsInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformIfNotExists(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformQualifiedNameInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue>
	TransformSchemaReservedIdentifierOrStringLiteralInternal(PEGTransformer &transformer, ParseResult &parse_result);
	static QualifiedName
	TransformSchemaReservedIdentifierOrStringLiteral(PEGTransformer &transformer,
	                                                 const Identifier &schema_qualification,
	                                                 const Identifier &reserved_identifier_or_string_literal);
	static unique_ptr<TransformResultValue>
	TransformCatalogReservedSchemaIdentifierInternal(PEGTransformer &transformer, ParseResult &parse_result);
	static QualifiedName
	TransformCatalogReservedSchemaIdentifier(PEGTransformer &transformer, const Identifier &catalog_qualification,
	                                         const vector<Identifier> &reserved_schema_qualification,
	                                         const Identifier &reserved_identifier_or_string_literal);
	static unique_ptr<TransformResultValue> TransformIdentifierOrStringLiteralInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static QualifiedName TransformIdentifierOrStringLiteral(PEGTransformer &transformer, const string &child);
	static unique_ptr<TransformResultValue>
	TransformReservedIdentifierOrStringLiteralInternal(PEGTransformer &transformer, ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCatalogQualificationInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static Identifier TransformCatalogQualification(PEGTransformer &transformer, const Identifier &catalog_name);
	static unique_ptr<TransformResultValue> TransformSchemaQualificationInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static Identifier TransformSchemaQualification(PEGTransformer &transformer, const Identifier &schema_name);
	static unique_ptr<TransformResultValue> TransformReservedSchemaQualificationInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static Identifier TransformReservedSchemaQualification(PEGTransformer &transformer,
	                                                       const Identifier &reserved_schema_name);
	static unique_ptr<TransformResultValue> TransformTableQualificationInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static Identifier TransformTableQualification(PEGTransformer &transformer, const Identifier &table_name);
	static unique_ptr<TransformResultValue> TransformReservedTableQualificationInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static Identifier TransformReservedTableQualification(PEGTransformer &transformer,
	                                                      const Identifier &reserved_table_name);
	static unique_ptr<TransformResultValue> TransformCreateTableColumnListInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static ColumnElements TransformCreateTableColumnList(PEGTransformer &transformer,
	                                                     vector<CreateTableColumnElement> create_table_column_element);
	static unique_ptr<TransformResultValue> TransformCreateTableColumnElementInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCreateTableColumnDefinitionInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static CreateTableColumnElement TransformCreateTableColumnDefinition(PEGTransformer &transformer,
	                                                                     ConstraintColumnDefinition column_definition);
	static unique_ptr<TransformResultValue> TransformCreateTableConstraintInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static CreateTableColumnElement TransformCreateTableConstraint(PEGTransformer &transformer,
	                                                               unique_ptr<Constraint> top_level_constraint);
	static unique_ptr<TransformResultValue> TransformColumnDefinitionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static ConstraintColumnDefinition
	TransformColumnDefinition(PEGTransformer &transformer, const vector<string> &dotted_identifier,
	                          const optional<LogicalType> &type, optional<GeneratedColumnDefinition> generated_column,
	                          const bool &has_result, optional<vector<ColumnConstraintEntry>> column_constraint);
	static unique_ptr<TransformResultValue> TransformColumnConstraintInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNotNullConstraintInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static ColumnConstraintEntry TransformNotNullConstraint(PEGTransformer &transformer, const bool &child);
	static unique_ptr<TransformResultValue> TransformNullConstraintInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformNullConstraint(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNotNullColumnConstraintInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static bool TransformNotNullColumnConstraint(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformUniqueConstraintInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static ColumnConstraintEntry TransformUniqueConstraint(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformPrimaryKeyConstraintInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static ColumnConstraintEntry TransformPrimaryKeyConstraint(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDefaultValueInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static ColumnConstraintEntry TransformDefaultValue(PEGTransformer &transformer,
	                                                   unique_ptr<ParsedExpression> column_default_expr);
	static unique_ptr<TransformResultValue> TransformCheckConstraintInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static ColumnConstraintEntry TransformCheckConstraint(PEGTransformer &transformer,
	                                                      unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformForeignKeyConstraintInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static ColumnConstraintEntry TransformForeignKeyConstraint(PEGTransformer &transformer,
	                                                           unique_ptr<BaseTableRef> base_table_name,
	                                                           const optional<vector<string>> &column_list,
	                                                           const KeyActions &key_actions);
	static unique_ptr<TransformResultValue> TransformColumnCollationInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static ColumnConstraintEntry TransformColumnCollation(PEGTransformer &transformer,
	                                                      const vector<string> &dotted_identifier);
	static unique_ptr<TransformResultValue> TransformColumnCompressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static ColumnConstraintEntry TransformColumnCompression(PEGTransformer &transformer,
	                                                        const Identifier &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformKeyActionsInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static KeyActions TransformKeyActions(PEGTransformer &transformer, const optional<string> &update_action,
	                                      const optional<string> &delete_action);
	static unique_ptr<TransformResultValue> TransformUpdateActionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static string TransformUpdateAction(PEGTransformer &transformer, const string &key_action);
	static unique_ptr<TransformResultValue> TransformDeleteActionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static string TransformDeleteAction(PEGTransformer &transformer, const string &key_action);
	static unique_ptr<TransformResultValue> TransformKeyActionInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNoKeyActionInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static string TransformNoKeyAction(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRestrictKeyActionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static string TransformRestrictKeyAction(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCascadeKeyActionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static string TransformCascadeKeyAction(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSetNullKeyActionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static string TransformSetNullKeyAction(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSetDefaultKeyActionInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static string TransformSetDefaultKeyAction(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTopLevelConstraintInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<Constraint> TransformTopLevelConstraint(PEGTransformer &transformer, const bool &has_result,
	                                                          unique_ptr<Constraint> top_level_constraint_list);
	static unique_ptr<TransformResultValue> TransformTopLevelConstraintListInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTopCheckConstraintInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<Constraint> TransformTopCheckConstraint(PEGTransformer &transformer,
	                                                          ColumnConstraintEntry check_constraint);
	static unique_ptr<TransformResultValue> TransformTopPrimaryKeyConstraintInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<Constraint> TransformTopPrimaryKeyConstraint(PEGTransformer &transformer,
	                                                               const vector<string> &column_id_list);
	static unique_ptr<TransformResultValue> TransformTopUniqueConstraintInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<Constraint> TransformTopUniqueConstraint(PEGTransformer &transformer,
	                                                           const vector<string> &column_id_list);
	static unique_ptr<TransformResultValue> TransformTopForeignKeyConstraintInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<Constraint> TransformTopForeignKeyConstraint(PEGTransformer &transformer,
	                                                               const vector<string> &column_id_list,
	                                                               ColumnConstraintEntry foreign_key_constraint);
	static unique_ptr<TransformResultValue> TransformColumnIdListInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static vector<string> TransformColumnIdList(PEGTransformer &transformer, const vector<Identifier> &col_id);
	static unique_ptr<TransformResultValue> TransformDottedIdentifierInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static vector<string> TransformDottedIdentifier(PEGTransformer &transformer, const Identifier &identifier,
	                                                const optional<vector<string>> &dot_col_label);
	static unique_ptr<TransformResultValue> TransformDotColLabelInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static string TransformDotColLabel(PEGTransformer &transformer, const string &col_label);
	static unique_ptr<TransformResultValue> TransformColIdInternal(PEGTransformer &transformer,
	                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformColIdOrStringInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTypeFuncNameInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTypeFuncKeywordInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformColLabelInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformColLabelOrStringInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformColLabelIdentifierInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static Identifier TransformColLabelIdentifier(PEGTransformer &transformer, const string &col_label);
	static unique_ptr<TransformResultValue> TransformStringLiteralIdentifierInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static Identifier TransformStringLiteralIdentifier(PEGTransformer &transformer, const string &string_literal);
	static unique_ptr<TransformResultValue> TransformGeneratedColumnInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static GeneratedColumnDefinition TransformGeneratedColumn(PEGTransformer &transformer, const bool &has_result,
	                                                          unique_ptr<ParsedExpression> expression,
	                                                          const optional<bool> &generated_column_type);
	static unique_ptr<TransformResultValue> TransformGeneratedColumnTypeInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCommitActionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformCommitAction(PEGTransformer &transformer, const bool &preserve_or_delete);
	static unique_ptr<TransformResultValue> TransformPreserveOrDeleteInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPreserveRowsInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformPreserveRows(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDeleteRowsInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static bool TransformDeleteRows(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformVirtualGeneratedColumnInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static bool TransformVirtualGeneratedColumn(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformStoredGeneratedColumnInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static bool TransformStoredGeneratedColumn(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCreateTriggerStmtInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<CreateStatement>
	TransformCreateTriggerStmt(PEGTransformer &transformer, const optional<bool> &if_not_exists,
	                           const Identifier &trigger_name, const TriggerTiming &trigger_timing,
	                           const TriggerEventInfo &trigger_event, unique_ptr<BaseTableRef> base_table_name,
	                           const optional<TriggerTableReferencingInfo> &referencing_clause,
	                           const optional<TriggerForEach> &for_each_clause, unique_ptr<SQLStatement> trigger_body);
	static unique_ptr<TransformResultValue> TransformTriggerBodyInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTriggerNameInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static Identifier TransformTriggerName(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformReferencingClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static TriggerTableReferencingInfo
	TransformReferencingClause(PEGTransformer &transformer, const TriggerTableReferencingInfo &referencing_item,
	                           const optional<TriggerTableReferencingInfo> &referencing_item_1);
	static unique_ptr<TransformResultValue> TransformReferencingItemInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformReferencingNewTableAsInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static TriggerTableReferencingInfo TransformReferencingNewTableAs(PEGTransformer &transformer,
	                                                                  const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformReferencingOldTableAsInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static TriggerTableReferencingInfo TransformReferencingOldTableAs(PEGTransformer &transformer,
	                                                                  const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformTriggerTimingInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTriggerBeforeInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static TriggerTiming TransformTriggerBefore(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTriggerAfterInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static TriggerTiming TransformTriggerAfter(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTriggerInsteadOfInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static TriggerTiming TransformTriggerInsteadOf(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTriggerEventInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTriggerEventInsertInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static TriggerEventInfo TransformTriggerEventInsert(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTriggerEventDeleteInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static TriggerEventInfo TransformTriggerEventDelete(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTriggerEventUpdateInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static TriggerEventInfo TransformTriggerEventUpdate(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTriggerEventUpdateOfInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static TriggerEventInfo TransformTriggerEventUpdateOf(PEGTransformer &transformer,
	                                                      const vector<string> &trigger_column_list);
	static unique_ptr<TransformResultValue> TransformTriggerColumnListInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<string> TransformTriggerColumnList(PEGTransformer &transformer, const vector<Identifier> &col_id);
	static unique_ptr<TransformResultValue> TransformForEachClauseInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformForEachRowInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static TriggerForEach TransformForEachRow(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformForEachStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static TriggerForEach TransformForEachStatement(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCreateTypeStmtInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<CreateStatement> TransformCreateTypeStmt(PEGTransformer &transformer,
	                                                           const optional<bool> &if_not_exists,
	                                                           const QualifiedName &qualified_name,
	                                                           unique_ptr<CreateTypeInfo> create_type);
	static unique_ptr<TransformResultValue> TransformCreateTypeInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCreateTypeFromTypeInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<CreateTypeInfo> TransformCreateTypeFromType(PEGTransformer &transformer, const LogicalType &type);
	static unique_ptr<TransformResultValue> TransformEnumSelectTypeInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<CreateTypeInfo> TransformEnumSelectType(PEGTransformer &transformer,
	                                                          unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformEnumStringLiteralListInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<CreateTypeInfo> TransformEnumStringLiteralList(PEGTransformer &transformer,
	                                                                 const optional<vector<string>> &string_literal);
	static unique_ptr<TransformResultValue> TransformCreateViewStmtInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<CreateStatement>
	TransformCreateViewStmt(PEGTransformer &transformer, const optional<bool> &create_secure,
	                        const optional<bool> &create_recursive, const optional<bool> &if_not_exists,
	                        const QualifiedName &qualified_name, const optional<vector<string>> &insert_column_list,
	                        optional<case_insensitive_map_t<unique_ptr<ParsedExpression>>> with_list,
	                        unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformCreateRecursiveInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static bool TransformCreateRecursive(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCreateSecureInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformCreateSecure(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDeallocateStatementInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformDeallocateStatement(PEGTransformer &transformer,
	                                                             const optional<bool> &deallocate_prepare,
	                                                             const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformDeallocatePrepareInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static bool TransformDeallocatePrepare(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDeleteStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformDeleteStatement(PEGTransformer &transformer, optional<CommonTableExpressionMap> with_clause,
	                         unique_ptr<BaseTableRef> target_opt_alias,
	                         optional<vector<unique_ptr<TableRef>>> delete_using_clause,
	                         optional<unique_ptr<ParsedExpression>> where_clause,
	                         optional<vector<unique_ptr<ParsedExpression>>> returning_clause);
	static unique_ptr<TransformResultValue> TransformTruncateStatementInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformTruncateStatement(PEGTransformer &transformer, const bool &has_result,
	                                                           unique_ptr<BaseTableRef> base_table_name);
	static unique_ptr<TransformResultValue> TransformTargetOptAliasInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<BaseTableRef> TransformTargetOptAlias(PEGTransformer &transformer,
	                                                        unique_ptr<BaseTableRef> base_table_name,
	                                                        const bool &has_result, const optional<Identifier> &col_id);
	static unique_ptr<TransformResultValue> TransformDeleteUsingClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<unique_ptr<TableRef>> TransformDeleteUsingClause(PEGTransformer &transformer,
	                                                               vector<unique_ptr<TableRef>> table_ref);
	static unique_ptr<TransformResultValue> TransformDescribeStatementInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformDescribeStatement(PEGTransformer &transformer,
	                                                              unique_ptr<QueryNode> child);
	static unique_ptr<TransformResultValue> TransformShowDeprecatedSelectInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<QueryNode> TransformShowDeprecatedSelect(PEGTransformer &transformer, const ShowType &show_rule,
	                                                           unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformDescribeSelectInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<QueryNode> TransformDescribeSelect(PEGTransformer &transformer,
	                                                     const ShowType &describe_or_summarize,
	                                                     unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformShowAllTablesInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<QueryNode> TransformShowAllTables(PEGTransformer &transformer, const ShowType &show_or_describe,
	                                                    const bool &has_result);
	static unique_ptr<TransformResultValue> TransformShowTablesInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<QueryNode> TransformShowTables(PEGTransformer &transformer, const ShowType &show_or_describe,
	                                                 const QualifiedName &qualified_name);
	static unique_ptr<TransformResultValue> TransformShowByNameInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<QueryNode> TransformShowByName(PEGTransformer &transformer, const ShowType &show_rule,
	                                                 optional<DescribeTarget> show_target);
	static unique_ptr<TransformResultValue> TransformDescribeByNameInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<QueryNode> TransformDescribeByName(PEGTransformer &transformer,
	                                                     const ShowType &describe_or_summarize,
	                                                     optional<DescribeTarget> describe_target);
	static unique_ptr<TransformResultValue> TransformDescribeOrSummarizeInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformShowTargetInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue>
	TransformShowDeprecatedQualifiedTableNameInternal(PEGTransformer &transformer, ParseResult &parse_result);
	static DescribeTarget TransformShowDeprecatedQualifiedTableName(PEGTransformer &transformer,
	                                                                unique_ptr<BaseTableRef> qualified_table_name);
	static unique_ptr<TransformResultValue> TransformShowSettingNameInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static DescribeTarget TransformShowSettingName(PEGTransformer &transformer, const Identifier &setting_name);
	static unique_ptr<TransformResultValue> TransformDescribeTargetInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDescribeBaseTableNameInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static DescribeTarget TransformDescribeBaseTableName(PEGTransformer &transformer,
	                                                     unique_ptr<BaseTableRef> base_table_name);
	static unique_ptr<TransformResultValue> TransformDescribeStringLiteralInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static DescribeTarget TransformDescribeStringLiteral(PEGTransformer &transformer, const string &string_literal);
	static unique_ptr<TransformResultValue> TransformSummarizeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static ShowType TransformSummarize(PEGTransformer &transformer, const ShowType &summarize_rule);
	static unique_ptr<TransformResultValue> TransformSummarizeRuleInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static ShowType TransformSummarizeRule(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformShowOrDescribeInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformShowRuleInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static ShowType TransformShowRule(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDescribeRuleInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDescribeLongRuleInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static ShowType TransformDescribeLongRule(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDescRuleInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static ShowType TransformDescRule(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDetachStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformDetachStatement(PEGTransformer &transformer, const bool &has_result,
	                                                         const optional<bool> &if_exists,
	                                                         const Identifier &catalog_name);
	static unique_ptr<TransformResultValue> TransformDropStatementInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformDropStatement(PEGTransformer &transformer,
	                                                       unique_ptr<DropStatement> drop_entries,
	                                                       const optional<bool> &drop_behavior);
	static unique_ptr<TransformResultValue> TransformDropEntriesInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDropTriggerInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropTrigger(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                      const Identifier &trigger_name,
	                                                      unique_ptr<BaseTableRef> base_table_name);
	static unique_ptr<TransformResultValue> TransformDropTableInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropTable(PEGTransformer &transformer, const CatalogType &table_or_view,
	                                                    const optional<bool> &if_exists,
	                                                    vector<unique_ptr<BaseTableRef>> base_table_name);
	static unique_ptr<TransformResultValue> TransformDropTableFunctionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropTableFunction(PEGTransformer &transformer,
	                                                            const CatalogType &comment_macro_table,
	                                                            const optional<bool> &if_exists,
	                                                            const vector<Identifier> &table_function_name);
	static unique_ptr<TransformResultValue> TransformDropFunctionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropFunction(PEGTransformer &transformer, const bool &function_type_macro,
	                                                       const optional<bool> &if_exists,
	                                                       const vector<QualifiedName> &function_identifier);
	static unique_ptr<TransformResultValue> TransformDropSchemaInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropSchema(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                     const vector<QualifiedName> &qualified_name);
	static unique_ptr<TransformResultValue> TransformDropIndexInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropIndex(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                    const vector<QualifiedName> &qualified_index_name);
	static unique_ptr<TransformResultValue> TransformQualifiedIndexNameInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformQualifiedIndexNameStringInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static QualifiedName TransformQualifiedIndexNameString(PEGTransformer &transformer, const Identifier &index_name);
	static unique_ptr<TransformResultValue> TransformSchemaReservedIndexInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static QualifiedName TransformSchemaReservedIndex(PEGTransformer &transformer,
	                                                  const Identifier &schema_qualification,
	                                                  const Identifier &reserved_index_name);
	static unique_ptr<TransformResultValue> TransformCatalogReservedSchemaIndexInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static QualifiedName TransformCatalogReservedSchemaIndex(PEGTransformer &transformer,
	                                                         const Identifier &catalog_qualification,
	                                                         const Identifier &reserved_schema_qualification,
	                                                         const Identifier &reserved_index_name);
	static unique_ptr<TransformResultValue> TransformDropSequenceInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropSequence(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                       const vector<QualifiedName> &qualified_sequence_name);
	static unique_ptr<TransformResultValue> TransformDropCollationInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropCollation(PEGTransformer &transformer,
	                                                        const optional<bool> &if_exists,
	                                                        const vector<Identifier> &collation_name);
	static unique_ptr<TransformResultValue> TransformDropTypeInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropType(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                   const vector<QualifiedName> &qualified_type_name);
	static unique_ptr<TransformResultValue> TransformDropSecretInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropSecret(PEGTransformer &transformer,
	                                                     const optional<SecretPersistType> &temporary,
	                                                     const optional<bool> &if_exists, const Identifier &secret_name,
	                                                     const optional<Identifier> &drop_secret_storage);
	static unique_ptr<TransformResultValue> TransformTableOrViewInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformMaterializedViewEntryInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static CatalogType TransformMaterializedViewEntry(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFunctionTypeMacroInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformFunctionTypeMacroKeywordInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static bool TransformFunctionTypeMacroKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFunctionTypeFunctionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static bool TransformFunctionTypeFunction(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDropBehaviorInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCascadeDropBehaviorInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static bool TransformCascadeDropBehavior(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRestrictDropBehaviorInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static bool TransformRestrictDropBehavior(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIfExistsInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static bool TransformIfExists(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDropSecretStorageInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static Identifier TransformDropSecretStorage(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformExecuteStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformExecuteStatement(PEGTransformer &transformer, const Identifier &identifier,
	                          optional<vector<FunctionArgument>> table_function_arguments);
	static unique_ptr<TransformResultValue> TransformExplainStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformExplainStatement(PEGTransformer &transformer, const optional<Identifier> &analyze_keyword,
	                          const optional<vector<GenericCopyOption>> &explain_option_list,
	                          unique_ptr<SQLStatement> explainable_statements);
	static unique_ptr<TransformResultValue> TransformExplainOptionListInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<GenericCopyOption> TransformExplainOptionList(PEGTransformer &transformer,
	                                                            const vector<GenericCopyOption> &explain_option);
	static unique_ptr<TransformResultValue> TransformExplainOptionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static GenericCopyOption TransformExplainOption(PEGTransformer &transformer, const Identifier &explain_option_name,
	                                                optional<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformExplainOptionNameInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static Identifier TransformExplainOptionName(PEGTransformer &transformer, ParseResult &choice_result);
	static unique_ptr<TransformResultValue> TransformExplainSelectStatementInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformExplainSelectStatement(PEGTransformer &transformer, unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformExplainableStatementsInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformExportStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformExportStatement(PEGTransformer &transformer, const optional<string> &export_source,
	                         const string &string_literal,
	                         const optional<vector<GenericCopyOption>> &generic_copy_option_list);
	static unique_ptr<TransformResultValue> TransformExportSourceInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static string TransformExportSource(PEGTransformer &transformer, const Identifier &catalog_name);
	static unique_ptr<TransformResultValue> TransformImportStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformImportStatement(PEGTransformer &transformer, const string &string_literal);
	static unique_ptr<TransformResultValue> TransformColumnReferenceInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformColumnReference(PEGTransformer &transformer,
	                                                             unique_ptr<ColumnRefExpression> child);
	static unique_ptr<TransformResultValue> TransformNestedSchemaTableColumnNameInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static unique_ptr<ColumnRefExpression> TransformNestedSchemaTableColumnName(
	    PEGTransformer &transformer, const Identifier &catalog_qualification,
	    const Identifier &reserved_schema_qualification, const Identifier &reserved_schema_qualification_1,
	    const vector<Identifier> &reserved_schema_qualification_2, const Identifier &reserved_column_name);
	static unique_ptr<TransformResultValue>
	TransformCatalogReservedSchemaTableColumnNameInternal(PEGTransformer &transformer, ParseResult &parse_result);
	static unique_ptr<ColumnRefExpression>
	TransformCatalogReservedSchemaTableColumnName(PEGTransformer &transformer, const Identifier &catalog_qualification,
	                                              const Identifier &reserved_schema_qualification,
	                                              const Identifier &reserved_table_qualification,
	                                              const Identifier &reserved_column_name);
	static unique_ptr<TransformResultValue> TransformSchemaReservedTableColumnNameInternal(PEGTransformer &transformer,
	                                                                                       ParseResult &parse_result);
	static unique_ptr<ColumnRefExpression>
	TransformSchemaReservedTableColumnName(PEGTransformer &transformer, const Identifier &schema_qualification,
	                                       const Identifier &reserved_table_qualification,
	                                       const Identifier &reserved_column_name);
	static unique_ptr<TransformResultValue> TransformTableReservedColumnNameInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ColumnRefExpression> TransformTableReservedColumnName(PEGTransformer &transformer,
	                                                                        const Identifier &table_qualification,
	                                                                        const Identifier &reserved_column_name);
	static unique_ptr<TransformResultValue> TransformFunctionExpressionInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformFunctionExpression(PEGTransformer &transformer, const QualifiedName &function_identifier,
	                            MethodArguments function_expression_arguments,
	                            optional<vector<OrderByNode>> within_group_clause,
	                            optional<unique_ptr<ParsedExpression>> filter_clause, const bool &has_result,
	                            optional<unique_ptr<WindowExpression>> over_clause);
	static unique_ptr<TransformResultValue> TransformFunctionExpressionArgumentsInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static MethodArguments TransformFunctionExpressionArguments(PEGTransformer &transformer,
	                                                            MethodArguments function_expression_argument_list);
	static unique_ptr<TransformResultValue> TransformFunctionExpressionArgumentListInternal(PEGTransformer &transformer,
	                                                                                        ParseResult &parse_result);
	static MethodArguments
	TransformFunctionExpressionArgumentList(PEGTransformer &transformer, const optional<bool> &distinct_or_all,
	                                        optional<vector<FunctionArgument>> function_argument_list,
	                                        optional<vector<OrderByNode>> order_by_clause,
	                                        const optional<bool> &ignore_or_respect_nulls);
	static unique_ptr<TransformResultValue> TransformFunctionArgumentListInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static vector<FunctionArgument> TransformFunctionArgumentList(PEGTransformer &transformer,
	                                                              vector<FunctionArgument> function_argument);
	static unique_ptr<TransformResultValue> TransformFunctionIdentifierInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformFunctionNameAsQualifiedNameInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static QualifiedName TransformFunctionNameAsQualifiedName(PEGTransformer &transformer,
	                                                          const Identifier &function_name);
	static unique_ptr<TransformResultValue>
	TransformCatalogReservedSchemaFunctionNameInternal(PEGTransformer &transformer, ParseResult &parse_result);
	static QualifiedName
	TransformCatalogReservedSchemaFunctionName(PEGTransformer &transformer, const Identifier &catalog_qualification,
	                                           const optional<vector<Identifier>> &reserved_schema_qualification,
	                                           const Identifier &reserved_function_name);
	static unique_ptr<TransformResultValue> TransformSchemaReservedFunctionNameInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static QualifiedName TransformSchemaReservedFunctionName(PEGTransformer &transformer,
	                                                         const Identifier &schema_qualification,
	                                                         const Identifier &reserved_function_name);
	static unique_ptr<TransformResultValue> TransformDistinctOrAllInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDistinctKeywordInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static bool TransformDistinctKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformAllKeywordInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static bool TransformAllKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformWithinGroupClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<OrderByNode> TransformWithinGroupClause(PEGTransformer &transformer,
	                                                      vector<OrderByNode> order_by_clause);
	static unique_ptr<TransformResultValue> TransformFilterClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformFilterClause(PEGTransformer &transformer,
	                                                          unique_ptr<ParsedExpression> filter_clause_expression);
	static unique_ptr<TransformResultValue> TransformFilterClauseExpressionInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformFilterClauseExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> filter_clause_contents);
	static unique_ptr<TransformResultValue> TransformFilterClauseContentsInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformFilterClauseContents(PEGTransformer &transformer,
	                                                                  const bool &has_result,
	                                                                  unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformIgnoreOrRespectNullsInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIgnoreNullsInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformIgnoreNulls(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRespectNullsInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformRespectNulls(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformParenthesisExpressionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformParenthesisExpression(PEGTransformer &transformer,
	                               optional<vector<unique_ptr<ParsedExpression>>> expression);
	static unique_ptr<TransformResultValue> TransformLiteralExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformLiteralExpression(PEGTransformer &transformer,
	                                                               ParseResult &choice_result);
	static unique_ptr<TransformResultValue> TransformConstantLiteralInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformConstantLiteral(PEGTransformer &transformer, const Value &child);
	static unique_ptr<TransformResultValue> TransformNullLiteralInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static Value TransformNullLiteral(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTrueLiteralInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static Value TransformTrueLiteral(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFalseLiteralInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static Value TransformFalseLiteral(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCastExpressionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformCastExpression(PEGTransformer &transformer, const bool &cast_or_try_cast, CastArguments cast_arguments);
	static unique_ptr<TransformResultValue> TransformCastArgumentsInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static CastArguments TransformCastArguments(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                                            const LogicalType &type);
	static unique_ptr<TransformResultValue> TransformCastOrTryCastInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCastKeywordInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformCastKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTryCastKeywordInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformTryCastKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformColIdDotInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static string TransformColIdDot(PEGTransformer &transformer, const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformStarExpressionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformStarExpression(PEGTransformer &transformer, const optional<vector<string>> &star_qualifier_list,
	                        const optional<qualified_column_set_t> &exclude_list,
	                        optional<case_insensitive_map_t<unique_ptr<ParsedExpression>>> replace_list,
	                        const optional<qualified_column_map_t<string>> &rename_list);
	static unique_ptr<TransformResultValue> TransformStarQualifierListInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<string> TransformStarQualifierList(PEGTransformer &transformer, const vector<string> &col_id_dot);
	static unique_ptr<TransformResultValue> TransformExcludeListInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static qualified_column_set_t TransformExcludeList(PEGTransformer &transformer,
	                                                   const qualified_column_set_t &exclude_names);
	static unique_ptr<TransformResultValue> TransformExcludeNamesInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformExcludeNameListInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static qualified_column_set_t TransformExcludeNameList(PEGTransformer &transformer,
	                                                       const vector<QualifiedColumnName> &exclude_name);
	static unique_ptr<TransformResultValue> TransformExcludeNameSingleInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static qualified_column_set_t TransformExcludeNameSingle(PEGTransformer &transformer,
	                                                         const QualifiedColumnName &exclude_name);
	static unique_ptr<TransformResultValue> TransformExcludeNameInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformExcludeDottedNameInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static QualifiedColumnName TransformExcludeDottedName(PEGTransformer &transformer,
	                                                      const vector<string> &dotted_identifier);
	static unique_ptr<TransformResultValue> TransformExcludeColumnNameInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static QualifiedColumnName TransformExcludeColumnName(PEGTransformer &transformer,
	                                                      const Identifier &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformReplaceListInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static case_insensitive_map_t<unique_ptr<ParsedExpression>>
	TransformReplaceList(PEGTransformer &transformer,
	                     case_insensitive_map_t<unique_ptr<ParsedExpression>> replace_entries);
	static unique_ptr<TransformResultValue> TransformReplaceEntriesInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformReplaceEntrySingleInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static case_insensitive_map_t<unique_ptr<ParsedExpression>>
	TransformReplaceEntrySingle(PEGTransformer &transformer, pair<string, unique_ptr<ParsedExpression>> replace_entry);
	static unique_ptr<TransformResultValue> TransformReplaceEntryListInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static case_insensitive_map_t<unique_ptr<ParsedExpression>>
	TransformReplaceEntryList(PEGTransformer &transformer,
	                          vector<pair<string, unique_ptr<ParsedExpression>>> replace_entry);
	static unique_ptr<TransformResultValue> TransformReplaceEntryInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static pair<string, unique_ptr<ParsedExpression>>
	TransformReplaceEntry(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                      unique_ptr<ParsedExpression> column_reference);
	static unique_ptr<TransformResultValue> TransformRenameListInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static qualified_column_map_t<string> TransformRenameList(PEGTransformer &transformer,
	                                                          const qualified_column_map_t<string> &rename_entries);
	static unique_ptr<TransformResultValue> TransformRenameEntriesInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformRenameEntryListInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static qualified_column_map_t<string>
	TransformRenameEntryList(PEGTransformer &transformer,
	                         const vector<pair<QualifiedColumnName, string>> &rename_entry);
	static unique_ptr<TransformResultValue> TransformSingleRenameEntryInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static qualified_column_map_t<string>
	TransformSingleRenameEntry(PEGTransformer &transformer, const pair<QualifiedColumnName, string> &rename_entry);
	static unique_ptr<TransformResultValue> TransformRenameEntryInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static pair<QualifiedColumnName, string> TransformRenameEntry(PEGTransformer &transformer,
	                                                              const QualifiedColumnName &exclude_name,
	                                                              const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformSubqueryExpressionInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformSubqueryExpression(PEGTransformer &transformer,
	                                                                const optional<bool> &subquery_not,
	                                                                const optional<bool> &subquery_exists,
	                                                                unique_ptr<TableRef> subquery_reference);
	static unique_ptr<TransformResultValue> TransformSubqueryNotInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformSubqueryNot(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSubqueryExistsInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformSubqueryExists(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCaseExpressionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformCaseExpression(PEGTransformer &transformer,
	                                                            optional<unique_ptr<ParsedExpression>> expression,
	                                                            vector<CaseCheck> case_when_then,
	                                                            optional<unique_ptr<ParsedExpression>> case_else);
	static unique_ptr<TransformResultValue> TransformCaseWhenThenInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static CaseCheck TransformCaseWhenThen(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                                       unique_ptr<ParsedExpression> expression_1);
	static unique_ptr<TransformResultValue> TransformCaseElseInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformCaseElse(PEGTransformer &transformer,
	                                                      unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformTypeLiteralInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformTypeLiteral(PEGTransformer &transformer, const LogicalType &type,
	                                                         const string &string_literal);
	static unique_ptr<TransformResultValue> TransformIntervalLiteralInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIntervalLiteral(PEGTransformer &transformer,
	                                                             unique_ptr<ParsedExpression> interval_parameter,
	                                                             const optional<DatePartSpecifier> &interval);
	static unique_ptr<TransformResultValue> TransformIntervalParameterInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIntervalStringParameterInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIntervalStringParameter(PEGTransformer &transformer,
	                                                                     const string &string_literal);
	static unique_ptr<TransformResultValue> TransformFrameClauseInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static WindowFrame TransformFrameClause(PEGTransformer &transformer, const string &framing,
	                                        vector<WindowBoundaryExpression> frame_extent,
	                                        const optional<WindowExcludeMode> &window_exclude_clause);
	static unique_ptr<TransformResultValue> TransformFramingInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformRowsFramingInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static string TransformRowsFraming(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRangeFramingInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static string TransformRangeFraming(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformGroupsFramingInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static string TransformGroupsFraming(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFrameExtentInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSingleFrameExtentInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<WindowBoundaryExpression> TransformSingleFrameExtent(PEGTransformer &transformer,
	                                                                   WindowBoundaryExpression frame_bound);
	static unique_ptr<TransformResultValue> TransformBetweenFrameExtentInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static vector<WindowBoundaryExpression> TransformBetweenFrameExtent(PEGTransformer &transformer,
	                                                                    WindowBoundaryExpression frame_bound,
	                                                                    WindowBoundaryExpression frame_bound_1);
	static unique_ptr<TransformResultValue> TransformFrameBoundInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformFrameUnboundedInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static WindowBoundaryExpression TransformFrameUnbounded(PEGTransformer &transformer,
	                                                        const bool &preceding_or_following);
	static unique_ptr<TransformResultValue> TransformFrameExpressionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static WindowBoundaryExpression TransformFrameExpression(PEGTransformer &transformer,
	                                                         unique_ptr<ParsedExpression> expression,
	                                                         const bool &preceding_or_following);
	static unique_ptr<TransformResultValue> TransformFrameCurrentRowInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static WindowBoundaryExpression TransformFrameCurrentRow(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformPrecedingOrFollowingInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPrecedingFrameInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformPrecedingFrame(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFollowingFrameInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformFollowingFrame(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformWindowExcludeClauseInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static WindowExcludeMode TransformWindowExcludeClause(PEGTransformer &transformer,
	                                                      const WindowExcludeMode &window_exclude_element);
	static unique_ptr<TransformResultValue> TransformWindowExcludeElementInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformExcludeCurrentRowInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static WindowExcludeMode TransformExcludeCurrentRow(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformExcludeGroupInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static WindowExcludeMode TransformExcludeGroup(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformExcludeTiesInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static WindowExcludeMode TransformExcludeTies(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformExcludeNoOthersInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static WindowExcludeMode TransformExcludeNoOthers(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformWindowFrameInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIdentifierWindowFrameInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<WindowExpression> TransformIdentifierWindowFrame(PEGTransformer &transformer,
	                                                                   const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformParensIdentifierInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<WindowExpression> TransformParensIdentifier(PEGTransformer &transformer,
	                                                              const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformWindowFrameDefinitionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformWindowFrameNameContentsParensInternal(PEGTransformer &transformer,
	                                                                                       ParseResult &parse_result);
	static unique_ptr<WindowExpression>
	TransformWindowFrameNameContentsParens(PEGTransformer &transformer,
	                                       unique_ptr<WindowExpression> window_frame_name_contents);
	static unique_ptr<TransformResultValue> TransformWindowFrameNameContentsInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<WindowExpression>
	TransformWindowFrameNameContents(PEGTransformer &transformer, const optional<Identifier> &base_window_name,
	                                 unique_ptr<WindowExpression> window_frame_contents);
	static unique_ptr<TransformResultValue> TransformWindowFrameContentsParensInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<WindowExpression>
	TransformWindowFrameContentsParens(PEGTransformer &transformer, unique_ptr<WindowExpression> window_frame_contents);
	static unique_ptr<TransformResultValue> TransformWindowFrameContentsInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<WindowExpression>
	TransformWindowFrameContents(PEGTransformer &transformer,
	                             optional<vector<unique_ptr<ParsedExpression>>> window_partition,
	                             optional<vector<OrderByNode>> order_by_clause, optional<WindowFrame> frame_clause);
	static unique_ptr<TransformResultValue> TransformBaseWindowNameInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static Identifier TransformBaseWindowName(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformWindowPartitionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformWindowPartition(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformListExpressionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformArrayBoundedListExpressionInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformArrayBoundedListExpression(PEGTransformer &transformer, const bool &has_result,
	                                    vector<unique_ptr<ParsedExpression>> bounded_list_expression);
	static unique_ptr<TransformResultValue> TransformArrayParensSelectInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformArrayParensSelect(PEGTransformer &transformer, unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformBoundedListExpressionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformBoundedListExpression(PEGTransformer &transformer,
	                               optional<vector<unique_ptr<ParsedExpression>>> expression);
	static unique_ptr<TransformResultValue> TransformStructExpressionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformStructExpression(PEGTransformer &transformer,
	                                                              optional<vector<FunctionArgument>> struct_field);
	static unique_ptr<TransformResultValue> TransformStructFieldInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static FunctionArgument TransformStructField(PEGTransformer &transformer, const Identifier &col_id_or_string,
	                                             unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformMapExpressionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformMapExpression(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> map_struct_expression);
	static unique_ptr<TransformResultValue> TransformMapStructExpressionInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformMapStructExpression(PEGTransformer &transformer,
	                             optional<vector<vector<unique_ptr<ParsedExpression>>>> map_struct_field);
	static unique_ptr<TransformResultValue> TransformMapStructFieldInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformMapStructField(PEGTransformer &transformer,
	                                                                    unique_ptr<ParsedExpression> expression,
	                                                                    unique_ptr<ParsedExpression> expression_1);
	static unique_ptr<TransformResultValue> TransformGroupingExpressionInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformGroupingExpression(PEGTransformer &transformer, const bool &grouping_or_grouping_id,
	                            optional<vector<unique_ptr<ParsedExpression>>> expression);
	static unique_ptr<TransformResultValue> TransformGroupingOrGroupingIdInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformGroupingKeywordInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static bool TransformGroupingKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformGroupingIdKeywordInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static bool TransformGroupingIdKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformParameterInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformQuestionMarkNumberedParameterInternal(PEGTransformer &transformer,
	                                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformQuestionMarkNumberedParameter(PEGTransformer &transformer, unique_ptr<ParsedExpression> number_literal);
	static unique_ptr<TransformResultValue> TransformAnonymousParameterInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformAnonymousParameter(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNumberedParameterInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformNumberedParameter(PEGTransformer &transformer,
	                                                               unique_ptr<ParsedExpression> number_literal);
	static unique_ptr<TransformResultValue> TransformColLabelParameterInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformColLabelParameter(PEGTransformer &transformer,
	                                                               const string &col_label);
	static unique_ptr<TransformResultValue> TransformPositionalExpressionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformPositionalExpression(PEGTransformer &transformer,
	                                                                  unique_ptr<ParsedExpression> number_literal);
	static unique_ptr<TransformResultValue> TransformDefaultExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDefaultExpression(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformListComprehensionExpressionInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformListComprehensionExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                                     const vector<Identifier> &col_id_or_string,
	                                     unique_ptr<ParsedExpression> expression_1,
	                                     optional<unique_ptr<ParsedExpression>> list_comprehension_filter);
	static unique_ptr<TransformResultValue> TransformListComprehensionFilterInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformListComprehensionFilter(PEGTransformer &transformer,
	                                                                     unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformParensExpressionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformParensExpression(PEGTransformer &transformer,
	                                                              unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformSingleExpressionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformExpressionInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformExpression(PEGTransformer &transformer,
	                                                        unique_ptr<ParsedExpression> lambda_arrow_expression);
	static unique_ptr<TransformResultValue> TransformColumnDefaultExprInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformColumnDefaultExpr(PEGTransformer &transformer,
	                                                               unique_ptr<ParsedExpression> col_def_or_expr);
	static unique_ptr<TransformResultValue> TransformLambdaArrowExpressionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformLambdaArrowExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> logical_or_expression,
	                               optional<vector<unique_ptr<ParsedExpression>>> single_arrow_pair);
	static unique_ptr<TransformResultValue> TransformSingleArrowPairInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformSingleArrowPair(PEGTransformer &transformer,
	                                                             unique_ptr<ParsedExpression> logical_or_expression);
	static unique_ptr<TransformResultValue> TransformLogicalOrExpressionInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformLogicalOrExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> logical_and_expression,
	                             optional<vector<unique_ptr<ParsedExpression>>> logical_or_expression_tail);
	static unique_ptr<TransformResultValue> TransformLogicalOrExpressionTailInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformLogicalOrExpressionTail(PEGTransformer &transformer, unique_ptr<ParsedExpression> logical_and_expression);
	static unique_ptr<TransformResultValue> TransformColDefOrExprInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformColDefOrExpr(PEGTransformer &transformer, unique_ptr<ParsedExpression> col_def_and_expr,
	                      optional<vector<unique_ptr<ParsedExpression>>> col_def_or_expression_tail);
	static unique_ptr<TransformResultValue> TransformColDefOrExpressionTailInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformColDefOrExpressionTail(PEGTransformer &transformer,
	                                                                    unique_ptr<ParsedExpression> col_def_and_expr);
	static unique_ptr<TransformResultValue> TransformLogicalAndExpressionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformLogicalAndExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> logical_not_expression,
	                              optional<vector<unique_ptr<ParsedExpression>>> logical_and_expression_tail);
	static unique_ptr<TransformResultValue> TransformLogicalAndExpressionTailInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformLogicalAndExpressionTail(PEGTransformer &transformer, unique_ptr<ParsedExpression> logical_not_expression);
	static unique_ptr<TransformResultValue> TransformColDefAndExprInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformColDefAndExpr(PEGTransformer &transformer, unique_ptr<ParsedExpression> is_distinct_from_expression,
	                       optional<vector<unique_ptr<ParsedExpression>>> col_def_and_expression_tail);
	static unique_ptr<TransformResultValue> TransformColDefAndExpressionTailInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformColDefAndExpressionTail(PEGTransformer &transformer,
	                                 unique_ptr<ParsedExpression> is_distinct_from_expression);
	static unique_ptr<TransformResultValue> TransformLogicalNotExpressionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformLogicalNotExpression(PEGTransformer &transformer,
	                                                                  optional<vector<bool>> not_expression,
	                                                                  unique_ptr<ParsedExpression> is_expression);
	static unique_ptr<TransformResultValue> TransformNotExpressionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static vector<bool> TransformNotExpression(PEGTransformer &transformer, const vector<bool> &not_keyword);
	static unique_ptr<TransformResultValue> TransformNotKeywordInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static bool TransformNotKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIsExpressionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIsExpression(PEGTransformer &transformer,
	                                                          unique_ptr<ParsedExpression> is_distinct_from_expression,
	                                                          optional<vector<unique_ptr<ParsedExpression>>> is_test);
	static unique_ptr<TransformResultValue> TransformIsTestInternal(PEGTransformer &transformer,
	                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIsLiteralInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIsLiteral(PEGTransformer &transformer, const bool &has_result,
	                                                       const Value &is_literal_value);
	static unique_ptr<TransformResultValue> TransformIsLiteralValueInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformUnknownLiteralInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static Value TransformUnknownLiteral(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNotNullInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNotNullKeywordInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformNotNullKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNotNullOperatorInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformNotNullOperator(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIsNullInternal(PEGTransformer &transformer,
	                                                                ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIsNull(PEGTransformer &transformer,
	                                                    unique_ptr<ParsedExpression> is_null_operator);
	static unique_ptr<TransformResultValue> TransformIsNullOperatorInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIsNullOperator(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIsDistinctFromExpressionInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformIsDistinctFromExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> comparison_expression,
	                                  optional<vector<IsDistinctFromTail>> is_distinct_from_tail);
	static unique_ptr<TransformResultValue> TransformIsDistinctFromTailInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static IsDistinctFromTail TransformIsDistinctFromTail(PEGTransformer &transformer,
	                                                      const ExpressionType &is_distinct_from_op,
	                                                      unique_ptr<ParsedExpression> comparison_expression);
	static unique_ptr<TransformResultValue> TransformIsDistinctFromOpInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static ExpressionType TransformIsDistinctFromOp(PEGTransformer &transformer, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformComparisonExpressionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformComparisonExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> between_in_like_expression,
	                              optional<vector<ComparisonExpressionTail>> comparison_expression_tail);
	static unique_ptr<TransformResultValue> TransformComparisonExpressionTailInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static ComparisonExpressionTail
	TransformComparisonExpressionTail(PEGTransformer &transformer, const ExpressionType &comparison_operator,
	                                  optional<vector<bool>> not_expression,
	                                  unique_ptr<ParsedExpression> between_in_like_expression);
	static unique_ptr<TransformResultValue> TransformComparisonOperatorInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOperatorEqualInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static ExpressionType TransformOperatorEqual(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOperatorNotEqualInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static ExpressionType TransformOperatorNotEqual(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOperatorLessThanInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static ExpressionType TransformOperatorLessThan(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOperatorGreaterThanInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static ExpressionType TransformOperatorGreaterThan(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOperatorLessThanEqualsInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static ExpressionType TransformOperatorLessThanEquals(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOperatorGreaterThanEqualsInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static ExpressionType TransformOperatorGreaterThanEquals(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformBetweenInLikeExpressionInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformBetweenInLikeExpression(PEGTransformer &transformer,
	                                 unique_ptr<ParsedExpression> other_operator_expression,
	                                 optional<BetweenInLikeOperator> between_in_like_op);
	static unique_ptr<TransformResultValue> TransformBetweenInLikeOpInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static BetweenInLikeOperator TransformBetweenInLikeOp(PEGTransformer &transformer, const bool &has_result,
	                                                      unique_ptr<ParsedExpression> between_in_like_op_expression);
	static unique_ptr<TransformResultValue> TransformBetweenInLikeOpExpressionInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLikeClauseInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformLikeClause(PEGTransformer &transformer, const string &like_variations,
	                                                        unique_ptr<ParsedExpression> other_operator_expression,
	                                                        optional<unique_ptr<ParsedExpression>> escape_clause);
	static unique_ptr<TransformResultValue> TransformEscapeClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformEscapeClause(PEGTransformer &transformer,
	                                                          unique_ptr<ParsedExpression> comparison_expression);
	static unique_ptr<TransformResultValue> TransformLikeVariationsInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLikeTokenInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static string TransformLikeToken(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformILikeTokenInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static string TransformILikeToken(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformGlobTokenInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static string TransformGlobToken(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSimilarToTokenInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static string TransformSimilarToToken(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRegexMatchTokenInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static string TransformRegexMatchToken(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRegexInsensitiveMatchTokenInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static string TransformRegexInsensitiveMatchToken(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNotILikeOpInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static string TransformNotILikeOp(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNotLikeOpInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static string TransformNotLikeOp(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNotRegexInsensitiveMatchOpInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static string TransformNotRegexInsensitiveMatchOp(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNotSimilarToOpInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static string TransformNotSimilarToOp(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInClauseInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformInClause(PEGTransformer &transformer,
	                                                      unique_ptr<ParsedExpression> in_expression);
	static unique_ptr<TransformResultValue> TransformInExpressionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformInContainsExpressionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformInContainsExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> other_operator_expression);
	static unique_ptr<TransformResultValue> TransformInExpressionListInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformInExpressionList(PEGTransformer &transformer,
	                                                              vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformInSelectStatementInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformInSelectStatement(PEGTransformer &transformer, unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformBetweenClauseInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformBetweenClause(PEGTransformer &transformer, unique_ptr<ParsedExpression> other_operator_expression,
	                       unique_ptr<ParsedExpression> other_operator_expression_1);
	static unique_ptr<TransformResultValue> TransformOtherOperatorExpressionInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformOtherOperatorExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> bitwise_expression,
	                                 optional<vector<OtherOperatorTail>> other_operator_tail);
	static unique_ptr<TransformResultValue> TransformOtherOperatorTailInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static OtherOperatorTail TransformOtherOperatorTail(PEGTransformer &transformer, ParsedOperator other_operator,
	                                                    unique_ptr<ParsedExpression> bitwise_expression);
	static unique_ptr<TransformResultValue> TransformOtherOperatorInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformAnyAllParsedOperatorInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static ParsedOperator TransformAnyAllParsedOperator(PEGTransformer &transformer,
	                                                    const pair<string, bool> &any_all_operator);
	static unique_ptr<TransformResultValue> TransformNamedOtherOperatorInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static ParsedOperator TransformNamedOtherOperator(PEGTransformer &transformer, const string &child);
	static unique_ptr<TransformResultValue> TransformAnyAllOperatorInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static pair<string, bool> TransformAnyAllOperator(PEGTransformer &transformer, const string &any_op,
	                                                  const bool &any_or_all);
	static unique_ptr<TransformResultValue> TransformAnyOrAllInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSubqueryAnyInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformSubqueryAny(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSubqueryAllInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformSubqueryAll(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInetOperatorInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformJsonOperatorInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformListOperatorInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformStringOperatorInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformQualifiedOperatorInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static string TransformQualifiedOperator(PEGTransformer &transformer, const string &qualified_operator_contents);
	static unique_ptr<TransformResultValue> TransformQualifiedOperatorContentsInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static string TransformQualifiedOperatorContents(PEGTransformer &transformer,
	                                                 const optional<vector<string>> &col_id_dot, const string &any_op);
	static unique_ptr<TransformResultValue> TransformAnyOpInternal(PEGTransformer &transformer,
	                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformBitwiseExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformBitwiseExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> additive_expression,
	                           optional<vector<BinaryExpressionTail>> bitwise_expression_tail);
	static unique_ptr<TransformResultValue> TransformBitwiseExpressionTailInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static BinaryExpressionTail TransformBitwiseExpressionTail(PEGTransformer &transformer, const string &bit_operator,
	                                                           unique_ptr<ParsedExpression> additive_expression);
	static unique_ptr<TransformResultValue> TransformBitOperatorInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformAdditiveExpressionInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformAdditiveExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> multiplicative_expression,
	                            optional<vector<BinaryExpressionTail>> additive_expression_tail);
	static unique_ptr<TransformResultValue> TransformAdditiveExpressionTailInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static BinaryExpressionTail TransformAdditiveExpressionTail(PEGTransformer &transformer, const string &term,
	                                                            unique_ptr<ParsedExpression> multiplicative_expression,
	                                                            optional_idx query_location);
	static unique_ptr<TransformResultValue> TransformTermInternal(PEGTransformer &transformer,
	                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformMultiplicativeExpressionInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformMultiplicativeExpression(PEGTransformer &transformer,
	                                  unique_ptr<ParsedExpression> exponentiation_expression,
	                                  optional<vector<BinaryExpressionTail>> multiplicative_expression_tail);
	static unique_ptr<TransformResultValue> TransformMultiplicativeExpressionTailInternal(PEGTransformer &transformer,
	                                                                                      ParseResult &parse_result);
	static BinaryExpressionTail
	TransformMultiplicativeExpressionTail(PEGTransformer &transformer, const string &factor,
	                                      unique_ptr<ParsedExpression> exponentiation_expression);
	static unique_ptr<TransformResultValue> TransformFactorInternal(PEGTransformer &transformer,
	                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformExponentiationExpressionInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformExponentiationExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> collate_expression,
	                                  optional<vector<BinaryExpressionTail>> exponentiation_expression_tail);
	static unique_ptr<TransformResultValue> TransformExponentiationExpressionTailInternal(PEGTransformer &transformer,
	                                                                                      ParseResult &parse_result);
	static BinaryExpressionTail TransformExponentiationExpressionTail(PEGTransformer &transformer,
	                                                                  const string &exponent_operator,
	                                                                  unique_ptr<ParsedExpression> collate_expression);
	static unique_ptr<TransformResultValue> TransformExponentOperatorInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCollateExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformCollateExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> at_time_zone_expression,
	                           optional<vector<unique_ptr<ParsedExpression>>> collate_expression_tail);
	static unique_ptr<TransformResultValue> TransformCollateExpressionTailInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformCollateExpressionTail(PEGTransformer &transformer, unique_ptr<ParsedExpression> at_time_zone_expression);
	static unique_ptr<TransformResultValue> TransformAtTimeZoneExpressionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformAtTimeZoneExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> prefix_expression,
	                              optional<vector<unique_ptr<ParsedExpression>>> at_time_zone_expression_tail);
	static unique_ptr<TransformResultValue> TransformAtTimeZoneExpressionTailInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformAtTimeZoneExpressionTail(PEGTransformer &transformer, unique_ptr<ParsedExpression> prefix_expression);
	static unique_ptr<TransformResultValue> TransformPrefixOperatorInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformMinusPrefixOperatorInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPlusPrefixOperatorInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTildePrefixOperatorInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformBaseExpressionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformBaseExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> single_expression,
	                        optional<vector<unique_ptr<ParsedExpression>>> indirection_list);
	static unique_ptr<TransformResultValue> TransformIndirectionListInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformIndirectionList(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> indirection);
	static unique_ptr<TransformResultValue> TransformIndirectionInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCastOperatorInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformCastOperator(PEGTransformer &transformer, const LogicalType &type);
	static unique_ptr<TransformResultValue> TransformDotOperatorInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDotMethodOperatorInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDotMethodOperator(PEGTransformer &transformer,
	                                                               unique_ptr<ParsedExpression> method_expression);
	static unique_ptr<TransformResultValue> TransformDotColumnOperatorInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDotColumnOperator(PEGTransformer &transformer,
	                                                               const string &col_label);
	static unique_ptr<TransformResultValue> TransformMethodExpressionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformMethodExpression(PEGTransformer &transformer, const string &col_label,
	                                                              MethodArguments method_expression_arguments);
	static unique_ptr<TransformResultValue> TransformMethodExpressionArgumentsInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static MethodArguments TransformMethodExpressionArguments(PEGTransformer &transformer,
	                                                          MethodArguments method_expression_argument_list);
	static unique_ptr<TransformResultValue> TransformMethodExpressionArgumentListInternal(PEGTransformer &transformer,
	                                                                                      ParseResult &parse_result);
	static MethodArguments
	TransformMethodExpressionArgumentList(PEGTransformer &transformer, const optional<bool> &distinct_or_all,
	                                      optional<vector<FunctionArgument>> method_function_arguments,
	                                      optional<vector<OrderByNode>> order_by_clause,
	                                      const optional<bool> &ignore_or_respect_nulls);
	static unique_ptr<TransformResultValue> TransformMethodFunctionArgumentsInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static vector<FunctionArgument> TransformMethodFunctionArguments(PEGTransformer &transformer,
	                                                                 vector<FunctionArgument> function_argument);
	static unique_ptr<TransformResultValue> TransformSliceExpressionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformSliceExpression(PEGTransformer &transformer,
	                                                             vector<unique_ptr<ParsedExpression>> slice_bound);
	static unique_ptr<TransformResultValue> TransformSliceBoundInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformSliceBound(PEGTransformer &transformer, optional<unique_ptr<ParsedExpression>> expression,
	                    optional<unique_ptr<ParsedExpression>> end_slice_bound,
	                    optional<unique_ptr<ParsedExpression>> step_slice_bound);
	static unique_ptr<TransformResultValue> TransformEndSliceBoundInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformEndSliceBound(PEGTransformer &transformer,
	                                                           optional<unique_ptr<ParsedExpression>> end_slice_value);
	static unique_ptr<TransformResultValue> TransformEndSliceValueInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformEndSliceMinusInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformEndSliceMinus(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformStepSliceBoundInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformStepSliceBound(PEGTransformer &transformer,
	                                                            optional<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformPostfixOperatorInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformPostfixOperator(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSpecialFunctionExpressionInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCoalesceExpressionInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformCoalesceExpression(PEGTransformer &transformer,
	                                                                vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformUnpackExpressionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformUnpackExpression(PEGTransformer &transformer,
	                                                              unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformTryExpressionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformTryExpression(PEGTransformer &transformer,
	                                                           unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformColumnsExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformColumnsExpression(PEGTransformer &transformer, const bool &has_result,
	                                                               unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformExtractExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformExtractExpression(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> extract_arguments);
	static unique_ptr<TransformResultValue> TransformExtractArgumentsInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformExtractArguments(PEGTransformer &transformer,
	                                                                      unique_ptr<ParsedExpression> extract_argument,
	                                                                      unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformLambdaExpressionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformLambdaExpression(PEGTransformer &transformer,
	                                                              const vector<Identifier> &col_id_or_string,
	                                                              unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformNullIfExpressionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformNullIfExpression(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> null_if_arguments);
	static unique_ptr<TransformResultValue> TransformNullIfArgumentsInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformNullIfArguments(PEGTransformer &transformer,
	                                                                     unique_ptr<ParsedExpression> expression,
	                                                                     unique_ptr<ParsedExpression> expression_1);
	static unique_ptr<TransformResultValue> TransformPositionExpressionInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformPositionExpression(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> position_arguments);
	static unique_ptr<TransformResultValue> TransformPositionArgumentsInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformPositionArguments(PEGTransformer &transformer, unique_ptr<ParsedExpression> other_operator_expression,
	                           unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformRowExpressionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformRowExpression(PEGTransformer &transformer, optional<vector<unique_ptr<ParsedExpression>>> expression);
	static unique_ptr<TransformResultValue> TransformSubstringExpressionInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformSubstringExpression(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> substring_arguments);
	static unique_ptr<TransformResultValue> TransformSubstringArgumentsInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSubstringExpressionListInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformSubstringExpressionList(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformSubstringParametersInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformSubstringParameters(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                             vector<unique_ptr<ParsedExpression>> substring_from_for);
	static unique_ptr<TransformResultValue> TransformSubstringFromForInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSubstringFromOptionalForInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformSubstringFromOptionalFor(PEGTransformer &transformer, unique_ptr<ParsedExpression> from_expression,
	                                  optional<unique_ptr<ParsedExpression>> for_expression);
	static unique_ptr<TransformResultValue> TransformSubstringForInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformSubstringFor(PEGTransformer &transformer,
	                                                                  unique_ptr<ParsedExpression> for_expression);
	static unique_ptr<TransformResultValue> TransformTrimExpressionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformTrimExpression(PEGTransformer &transformer,
	                                                            TrimArguments trim_arguments);
	static unique_ptr<TransformResultValue> TransformTrimArgumentsInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static TrimArguments TransformTrimArguments(PEGTransformer &transformer, const optional<string> &trim_direction,
	                                            optional<unique_ptr<ParsedExpression>> trim_source,
	                                            vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformTrimDirectionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTrimBothInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static string TransformTrimBoth(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTrimLeadingInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static string TransformTrimLeading(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTrimTrailingInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static string TransformTrimTrailing(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTrimSourceInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformTrimSource(PEGTransformer &transformer,
	                                                        optional<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformOverlayExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformOverlayExpression(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> overlay_arguments);
	static unique_ptr<TransformResultValue> TransformOverlayArgumentsInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOverlayParametersInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformOverlayParameters(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                           unique_ptr<ParsedExpression> expression_1, unique_ptr<ParsedExpression> from_expression,
	                           optional<unique_ptr<ParsedExpression>> for_expression);
	static unique_ptr<TransformResultValue> TransformFromExpressionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformFromExpression(PEGTransformer &transformer,
	                                                            unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformForExpressionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformForExpression(PEGTransformer &transformer,
	                                                           unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformOverlayExpressionListInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformOverlayExpressionList(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformExtractArgumentInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformExtractDatePartArgumentInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformExtractDatePartArgument(PEGTransformer &transformer,
	                                                                     const DatePartSpecifier &extract_date_part);
	static unique_ptr<TransformResultValue> TransformExtractIdentifierArgumentInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformExtractIdentifierArgument(PEGTransformer &transformer,
	                                                                       const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformExtractStringArgumentInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformExtractStringArgument(PEGTransformer &transformer,
	                                                                   const string &string_literal);
	static unique_ptr<TransformResultValue> TransformExtractDatePartInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformExternalResourceStatementInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCreateExternalResourceStmtInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformCreateExternalResourceStmt(PEGTransformer &transformer, const string &string_literal,
	                                    const optional<Identifier> &attach_alias,
	                                    const optional<vector<GenericCopyOption>> &external_resource_creation_options);
	static unique_ptr<TransformResultValue> TransformRegisterExternalResourceStmtInternal(PEGTransformer &transformer,
	                                                                                      ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformRegisterExternalResourceStmt(PEGTransformer &transformer,
	                                                                      const string &string_literal,
	                                                                      const optional<Identifier> &attach_alias,
	                                                                      unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformDestroyExternalResourceStmtInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformDestroyExternalResourceStmt(PEGTransformer &transformer,
	                                                                     const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformShowExternalResourcesStmtInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformShowExternalResourcesStmt(PEGTransformer &transformer,
	                                                                   const optional<bool> &show_all_modifier);
	static unique_ptr<TransformResultValue> TransformShowAllModifierInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static bool TransformShowAllModifier(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue>
	TransformExternalResourceCreationOptionsInternal(PEGTransformer &transformer, ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformInsertStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformInsertStatement(PEGTransformer &transformer, optional<CommonTableExpressionMap> with_clause,
	                         const optional<OnConflictAction> &or_action, unique_ptr<BaseTableRef> insert_target,
	                         const optional<InsertColumnOrder> &by_name_or_position,
	                         const optional<vector<string>> &insert_column_list, InsertValues insert_values,
	                         optional<unique_ptr<OnConflictInfo>> on_conflict_clause,
	                         optional<vector<unique_ptr<ParsedExpression>>> returning_clause);
	static unique_ptr<TransformResultValue> TransformOrActionInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformInsertOrReplaceInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static OnConflictAction TransformInsertOrReplace(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInsertOrIgnoreInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static OnConflictAction TransformInsertOrIgnore(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformByNameOrPositionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformInsertByNameOrderInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformInsertByPositionOrderInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformInsertByNameInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static InsertColumnOrder TransformInsertByName(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInsertByPositionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static InsertColumnOrder TransformInsertByPosition(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInsertTargetInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<BaseTableRef> TransformInsertTarget(PEGTransformer &transformer,
	                                                      unique_ptr<BaseTableRef> base_table_name,
	                                                      const optional<Identifier> &insert_alias);
	static unique_ptr<TransformResultValue> TransformInsertAliasInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static Identifier TransformInsertAlias(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformColumnListInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static vector<string> TransformColumnList(PEGTransformer &transformer, const vector<Identifier> &col_id);
	static unique_ptr<TransformResultValue> TransformInsertColumnListInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static vector<string> TransformInsertColumnList(PEGTransformer &transformer, const vector<string> &column_list);
	static unique_ptr<TransformResultValue> TransformInsertValuesInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSelectInsertValuesInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static InsertValues TransformSelectInsertValues(PEGTransformer &transformer,
	                                                unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformDefaultValuesInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static InsertValues TransformDefaultValues(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOnConflictClauseInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<OnConflictInfo> TransformOnConflictClause(PEGTransformer &transformer,
	                                                            optional<OnConflictExpressionTarget> on_conflict_target,
	                                                            unique_ptr<OnConflictInfo> on_conflict_action);
	static unique_ptr<TransformResultValue> TransformOnConflictTargetInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOnConflictExpressionTargetInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static OnConflictExpressionTarget
	TransformOnConflictExpressionTarget(PEGTransformer &transformer, const vector<string> &column_id_list,
	                                    optional<unique_ptr<ParsedExpression>> where_clause);
	static unique_ptr<TransformResultValue> TransformOnConflictIndexTargetInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static OnConflictExpressionTarget TransformOnConflictIndexTarget(PEGTransformer &transformer,
	                                                                 const Identifier &constraint_name);
	static unique_ptr<TransformResultValue> TransformOnConflictActionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOnConflictUpdateInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<OnConflictInfo> TransformOnConflictUpdate(PEGTransformer &transformer,
	                                                            unique_ptr<UpdateSetInfo> update_set_clause,
	                                                            optional<unique_ptr<ParsedExpression>> where_clause);
	static unique_ptr<TransformResultValue> TransformOnConflictNothingInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<OnConflictInfo> TransformOnConflictNothing(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformReturningClauseInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformReturningClause(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> target_list);
	static unique_ptr<TransformResultValue> TransformLoadStatementInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformLoadStatement(PEGTransformer &transformer,
	                                                       const Identifier &col_id_or_string,
	                                                       const optional<ExtensionRepositoryInfo> &from_source,
	                                                       const optional<Identifier> &extension_alias);
	static unique_ptr<TransformResultValue> TransformExtensionAliasInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static Identifier TransformExtensionAlias(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformInstallStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformInstallStatement(PEGTransformer &transformer, const bool &has_result,
	                                                          const optional<bool> &install_and_load,
	                                                          const QualifiedName &identifier_or_string_literal,
	                                                          const optional<ExtensionRepositoryInfo> &from_source,
	                                                          const optional<string> &version_number);
	static unique_ptr<TransformResultValue> TransformInstallAndLoadInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformInstallAndLoad(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformUpdateExtensionsStatementInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformUpdateExtensionsStatement(PEGTransformer &transformer,
	                                                                   const optional<vector<Identifier>> &identifier);
	static unique_ptr<TransformResultValue> TransformFromSourceInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformFromSourceIdentifierInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static ExtensionRepositoryInfo TransformFromSourceIdentifier(PEGTransformer &transformer,
	                                                             const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformFromSourceStringInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static ExtensionRepositoryInfo TransformFromSourceString(PEGTransformer &transformer, const string &string_literal);
	static unique_ptr<TransformResultValue> TransformVersionNumberInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static string TransformVersionNumber(PEGTransformer &transformer,
	                                     const QualifiedName &identifier_or_string_literal);
	static unique_ptr<TransformResultValue> TransformExtensionRepositoryStatementInternal(PEGTransformer &transformer,
	                                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCreateExtensionRepositoryStmtInternal(PEGTransformer &transformer,
	                                                                                       ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformCreateExtensionRepositoryStmt(PEGTransformer &transformer, const optional<bool> &or_replace,
	                                       const optional<bool> &if_not_exists, const Identifier &col_id_or_string,
	                                       const string &repository_prefix,
	                                       const optional<vector<string>> &repository_public_key);
	static unique_ptr<TransformResultValue> TransformRepositoryPrefixInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static string TransformRepositoryPrefix(PEGTransformer &transformer, const bool &has_result,
	                                        const string &string_literal);
	static unique_ptr<TransformResultValue> TransformRepositoryPublicKeyInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static vector<string> TransformRepositoryPublicKey(PEGTransformer &transformer, const bool &has_result,
	                                                   const vector<string> &string_literal);
	static unique_ptr<TransformResultValue> TransformDropExtensionRepositoryStmtInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformDropExtensionRepositoryStmt(PEGTransformer &transformer,
	                                                                     const optional<bool> &if_exists,
	                                                                     const Identifier &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformMergeIntoStatementInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformMergeIntoStatement(PEGTransformer &transformer, optional<CommonTableExpressionMap> with_clause,
	                            unique_ptr<BaseTableRef> target_opt_alias, unique_ptr<TableRef> merge_into_using_clause,
	                            JoinQualifier join_qualifier,
	                            vector<pair<MergeActionCondition, unique_ptr<MergeIntoAction>>> merge_match,
	                            optional<vector<unique_ptr<ParsedExpression>>> returning_clause);
	static unique_ptr<TransformResultValue> TransformMergeIntoUsingClauseInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TableRef> TransformMergeIntoUsingClause(PEGTransformer &transformer,
	                                                          unique_ptr<TableRef> table_ref);
	static unique_ptr<TransformResultValue> TransformMergeMatchInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformMatchedClauseInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static pair<MergeActionCondition, unique_ptr<MergeIntoAction>>
	TransformMatchedClause(PEGTransformer &transformer, optional<unique_ptr<ParsedExpression>> and_expression,
	                       unique_ptr<MergeIntoAction> matched_clause_action);
	static unique_ptr<TransformResultValue> TransformMatchedClauseActionInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformUpdateMatchClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<MergeIntoAction>
	TransformUpdateMatchClause(PEGTransformer &transformer, optional<unique_ptr<MergeIntoAction>> update_match_info);
	static unique_ptr<TransformResultValue> TransformUpdateMatchInfoInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformUpdateMatchSetActionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<MergeIntoAction> TransformUpdateMatchSetAction(PEGTransformer &transformer,
	                                                                 unique_ptr<UpdateSetInfo> update_match_set_clause);
	static unique_ptr<TransformResultValue> TransformUpdateByNameOrPositionInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<MergeIntoAction> TransformUpdateByNameOrPosition(PEGTransformer &transformer,
	                                                                   const InsertColumnOrder &by_name_or_position);
	static unique_ptr<TransformResultValue> TransformDeleteMatchClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<MergeIntoAction> TransformDeleteMatchClause(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInsertMatchClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<MergeIntoAction>
	TransformInsertMatchClause(PEGTransformer &transformer, optional<unique_ptr<MergeIntoAction>> insert_match_info);
	static unique_ptr<TransformResultValue> TransformInsertMatchInfoInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformInsertDefaultValuesInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<MergeIntoAction> TransformInsertDefaultValues(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInsertByNameOrPositionInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<MergeIntoAction>
	TransformInsertByNameOrPosition(PEGTransformer &transformer, const optional<InsertColumnOrder> &by_name_or_position,
	                                const bool &has_result);
	static unique_ptr<TransformResultValue> TransformInsertValuesListInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<MergeIntoAction> TransformInsertValuesList(PEGTransformer &transformer,
	                                                             const optional<vector<string>> &insert_column_list,
	                                                             vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformDoNothingMatchClauseInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<MergeIntoAction> TransformDoNothingMatchClause(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformErrorMatchClauseInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<MergeIntoAction> TransformErrorMatchClause(PEGTransformer &transformer,
	                                                             optional<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformUpdateMatchSetClauseInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformUpdateMatchSetInfoInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformAndExpressionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformAndExpression(PEGTransformer &transformer,
	                                                           unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformNotMatchedClauseInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static pair<MergeActionCondition, unique_ptr<MergeIntoAction>>
	TransformNotMatchedClause(PEGTransformer &transformer, const optional<MergeActionCondition> &by_source_or_target,
	                          optional<unique_ptr<ParsedExpression>> and_expression,
	                          unique_ptr<MergeIntoAction> matched_clause_action);
	static unique_ptr<TransformResultValue> TransformBySourceOrTargetInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformBySourceInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static MergeActionCondition TransformBySource(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformByTargetInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static MergeActionCondition TransformByTarget(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformPivotOnInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static vector<PivotColumn> TransformPivotOn(PEGTransformer &transformer, vector<PivotColumn> pivot_column_list);
	static unique_ptr<TransformResultValue> TransformPivotUsingInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformPivotUsing(PEGTransformer &transformer,
	                                                                vector<unique_ptr<ParsedExpression>> target_list);
	static unique_ptr<TransformResultValue> TransformPivotColumnListInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static vector<PivotColumn> TransformPivotColumnList(PEGTransformer &transformer,
	                                                    vector<PivotColumn> pivot_column_entry);
	static unique_ptr<TransformResultValue> TransformPivotColumnEntryInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPivotColumnExpressionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static PivotColumn TransformPivotColumnExpression(PEGTransformer &transformer,
	                                                  unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformPivotColumnSubqueryInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static PivotColumn TransformPivotColumnSubquery(PEGTransformer &transformer,
	                                                unique_ptr<ParsedExpression> base_expression,
	                                                unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformIntoNameValuesInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static UnpivotNameValues TransformIntoNameValues(PEGTransformer &transformer, const Identifier &col_id_or_string,
	                                                 const vector<Identifier> &identifier);
	static unique_ptr<TransformResultValue> TransformIncludeOrExcludeNullsInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIncludeNullsInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformIncludeNulls(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformExcludeNullsInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformExcludeNulls(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformUnpivotHeaderInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformUnpivotHeaderSingleInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static vector<string> TransformUnpivotHeaderSingle(PEGTransformer &transformer, const Identifier &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformUnpivotHeaderListInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<string> TransformUnpivotHeaderList(PEGTransformer &transformer,
	                                                 const vector<Identifier> &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformPragmaStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformPragmaStatement(PEGTransformer &transformer,
	                                                         unique_ptr<SQLStatement> pragma_assign_or_function);
	static unique_ptr<TransformResultValue> TransformPragmaAssignOrFunctionInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPragmaAssignInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformPragmaAssign(PEGTransformer &transformer, const Identifier &setting_name,
	                                                      vector<unique_ptr<ParsedExpression>> variable_list);
	static unique_ptr<TransformResultValue> TransformPragmaFunctionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformPragmaFunction(PEGTransformer &transformer, const Identifier &pragma_name,
	                        optional<vector<unique_ptr<ParsedExpression>>> pragma_parameters);
	static unique_ptr<TransformResultValue> TransformPragmaParametersInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformPragmaParameters(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformPrepareStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformPrepareStatement(PEGTransformer &transformer, const Identifier &identifier,
	                                                          const optional<vector<LogicalType>> &type_list,
	                                                          unique_ptr<SQLStatement> statement);
	static unique_ptr<TransformResultValue> TransformTypeListInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static vector<LogicalType> TransformTypeList(PEGTransformer &transformer, const vector<LogicalType> &type);
	static unique_ptr<TransformResultValue> TransformSelectStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformSelectStatement(PEGTransformer &transformer,
	                                                         unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformSelectSetOpChainInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformSelectSetOpChain(
	    PEGTransformer &transformer, unique_ptr<SelectStatement> intersect_chain,
	    optional<vector<pair<unique_ptr<SetOperationNode>, unique_ptr<SelectStatement>>>> select_set_op_chain_tail);
	static unique_ptr<TransformResultValue> TransformSelectSetOpChainTailInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static pair<unique_ptr<SetOperationNode>, unique_ptr<SelectStatement>>
	TransformSelectSetOpChainTail(PEGTransformer &transformer, unique_ptr<SetOperationNode> setop_clause,
	                              unique_ptr<SelectStatement> intersect_chain);
	static unique_ptr<TransformResultValue> TransformIntersectChainInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformIntersectChain(
	    PEGTransformer &transformer, unique_ptr<SelectStatement> select_atom,
	    optional<vector<pair<unique_ptr<SetOperationNode>, unique_ptr<SelectStatement>>>> intersect_chain_tail);
	static unique_ptr<TransformResultValue> TransformIntersectChainTailInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static pair<unique_ptr<SetOperationNode>, unique_ptr<SelectStatement>>
	TransformIntersectChainTail(PEGTransformer &transformer, unique_ptr<SetOperationNode> set_intersect_clause,
	                            unique_ptr<SelectStatement> select_atom);
	static unique_ptr<TransformResultValue> TransformSetIntersectClauseInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<SetOperationNode> TransformSetIntersectClause(PEGTransformer &transformer,
	                                                                const optional<bool> &distinct_or_all);
	static unique_ptr<TransformResultValue> TransformSelectAtomInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSelectParensInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformSelectParens(PEGTransformer &transformer,
	                                                         unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformSetopClauseInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<SetOperationNode> TransformSetopClause(PEGTransformer &transformer,
	                                                         const SetOperationType &setop_type,
	                                                         const optional<bool> &distinct_or_all,
	                                                         const bool &has_result);
	static unique_ptr<TransformResultValue> TransformSetopTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSetopUnionInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static SetOperationType TransformSetopUnion(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSetopExceptInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static SetOperationType TransformSetopExcept(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSelectStatementTypeInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformResultModifiersInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static vector<unique_ptr<ResultModifier>>
	TransformResultModifiers(PEGTransformer &transformer, optional<vector<OrderByNode>> order_by_clause,
	                         optional<unique_ptr<ResultModifier>> limit_offset);
	static unique_ptr<TransformResultValue> TransformLimitOffsetInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLimitOffsetClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ResultModifier> TransformLimitOffsetClause(PEGTransformer &transformer,
	                                                             LimitPercentResult limit_clause,
	                                                             optional<LimitPercentResult> offset_clause);
	static unique_ptr<TransformResultValue> TransformOffsetLimitClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ResultModifier> TransformOffsetLimitClause(PEGTransformer &transformer,
	                                                             LimitPercentResult offset_clause,
	                                                             optional<LimitPercentResult> limit_clause);
	static unique_ptr<TransformResultValue> TransformOffsetFetchClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ResultModifier> TransformOffsetFetchClause(PEGTransformer &transformer,
	                                                             LimitPercentResult offset_clause,
	                                                             LimitPercentResult fetch_clause);
	static unique_ptr<TransformResultValue> TransformFetchOnlyClauseInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ResultModifier> TransformFetchOnlyClause(PEGTransformer &transformer,
	                                                           LimitPercentResult fetch_clause);
	static unique_ptr<TransformResultValue> TransformTableStatementInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformTableStatement(PEGTransformer &transformer,
	                                                           unique_ptr<BaseTableRef> base_table_name);
	static unique_ptr<TransformResultValue> TransformOptionalParensSimpleSelectInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSimpleSelectParensInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformSimpleSelectParens(PEGTransformer &transformer,
	                                                               unique_ptr<SelectStatement> simple_select);
	static unique_ptr<TransformResultValue> TransformSelectFromInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSelectFromClauseInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SelectNode> TransformSelectFromClause(PEGTransformer &transformer,
	                                                        unique_ptr<SelectNode> select_clause,
	                                                        optional<unique_ptr<TableRef>> from_clause);
	static unique_ptr<TransformResultValue> TransformFromSelectClauseInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SelectNode> TransformFromSelectClause(PEGTransformer &transformer,
	                                                        unique_ptr<TableRef> from_clause,
	                                                        optional<unique_ptr<SelectNode>> select_clause);
	static unique_ptr<TransformResultValue> TransformWithStatementInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static pair<Identifier, unique_ptr<CommonTableExpressionInfo>>
	TransformWithStatement(PEGTransformer &transformer, const Identifier &col_id_or_string,
	                       const optional<vector<string>> &insert_column_list,
	                       optional<vector<unique_ptr<ParsedExpression>>> using_key, const optional<bool> &materialized,
	                       unique_ptr<TableRef> cte_body);
	static unique_ptr<TransformResultValue> TransformCTEBodyInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCTESelectBodyInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TableRef> TransformCTESelectBody(PEGTransformer &transformer,
	                                                   unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformCTEDMLBodyInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TableRef> TransformCTEDMLBody(PEGTransformer &transformer, unique_ptr<SQLStatement> statement);
	static unique_ptr<TransformResultValue> TransformUsingKeyInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformUsingKey(PEGTransformer &transformer,
	                                                              vector<unique_ptr<ParsedExpression>> target_list);
	static unique_ptr<TransformResultValue> TransformMaterializedInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformMaterialized(PEGTransformer &transformer, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformSelectClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<SelectNode> TransformSelectClause(PEGTransformer &transformer,
	                                                    optional<DistinctClause> distinct_clause,
	                                                    optional<vector<unique_ptr<ParsedExpression>>> target_list);
	static unique_ptr<TransformResultValue> TransformTargetListInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformTargetList(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> aliased_expression);
	static unique_ptr<TransformResultValue> TransformColumnAliasesInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static vector<string> TransformColumnAliases(PEGTransformer &transformer,
	                                             const vector<Identifier> &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformDistinctClauseInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDistinctAllInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static DistinctClause TransformDistinctAll(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDistinctOnInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static DistinctClause TransformDistinctOn(PEGTransformer &transformer,
	                                          optional<vector<unique_ptr<ParsedExpression>>> distinct_on_targets);
	static unique_ptr<TransformResultValue> TransformDistinctOnTargetsInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformDistinctOnTargets(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformInnerTableRefInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTableSubqueryInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TableRef> TransformTableSubquery(PEGTransformer &transformer,
	                                                   const optional<Identifier> &table_alias_colon,
	                                                   const optional<bool> &lateral,
	                                                   unique_ptr<TableRef> subquery_reference,
	                                                   const optional<TableAlias> &table_alias);
	static unique_ptr<TransformResultValue> TransformBaseTableRefInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TableRef>
	TransformBaseTableRef(PEGTransformer &transformer, const optional<Identifier> &table_alias_colon,
	                      unique_ptr<BaseTableRef> base_table_name, const optional<TableAlias> &table_alias,
	                      optional<unique_ptr<AtClause>> at_clause, optional<unique_ptr<SampleOptions>> sample_clause);
	static unique_ptr<TransformResultValue> TransformTableAliasColonInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static Identifier TransformTableAliasColon(PEGTransformer &transformer, const Identifier &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformValuesRefInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TableRef> TransformValuesRef(PEGTransformer &transformer,
	                                               const optional<Identifier> &table_alias_colon,
	                                               unique_ptr<SelectStatement> values_clause,
	                                               const optional<TableAlias> &table_alias);
	static unique_ptr<TransformResultValue> TransformParensTableRefInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TableRef> TransformParensTableRef(PEGTransformer &transformer,
	                                                    const optional<Identifier> &table_alias_colon,
	                                                    unique_ptr<TableRef> table_ref,
	                                                    const optional<TableAlias> &table_alias,
	                                                    optional<unique_ptr<SampleOptions>> sample_clause);
	static unique_ptr<TransformResultValue> TransformJoinOrPivotInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTablePivotClauseInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TableRef> TransformTablePivotClause(PEGTransformer &transformer,
	                                                      unique_ptr<TableRef> table_pivot_clause_body,
	                                                      const optional<TableAlias> &table_alias);
	static unique_ptr<TransformResultValue> TransformTablePivotClauseBodyInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TableRef> TransformTablePivotClauseBody(PEGTransformer &transformer,
	                                                          vector<unique_ptr<ParsedExpression>> target_list,
	                                                          vector<PivotColumn> pivot_value_list,
	                                                          const optional<vector<string>> &pivot_group_by_list);
	static unique_ptr<TransformResultValue> TransformPivotGroupByListInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static vector<string> TransformPivotGroupByList(PEGTransformer &transformer,
	                                                const vector<Identifier> &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformTableUnpivotClauseInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TableRef> TransformTableUnpivotClause(PEGTransformer &transformer,
	                                                        const optional<bool> &include_or_exclude_nulls,
	                                                        unique_ptr<TableRef> table_unpivot_clause_body,
	                                                        const optional<TableAlias> &table_alias);
	static unique_ptr<TransformResultValue> TransformTableUnpivotClauseBodyInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<TableRef> TransformTableUnpivotClauseBody(PEGTransformer &transformer,
	                                                            const vector<string> &unpivot_header,
	                                                            vector<PivotColumn> unpivot_value_list);
	static unique_ptr<TransformResultValue> TransformPivotHeaderInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformPivotHeader(PEGTransformer &transformer,
	                                                         unique_ptr<ParsedExpression> base_expression);
	static unique_ptr<TransformResultValue> TransformPivotValueListInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static PivotColumn TransformPivotValueList(PEGTransformer &transformer, unique_ptr<ParsedExpression> pivot_header,
	                                           PivotColumn pivot_value_target);
	static unique_ptr<TransformResultValue> TransformPivotValueTargetInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPivotEnumTargetInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static PivotColumn TransformPivotEnumTarget(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformPivotListTargetInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static PivotColumn TransformPivotListTarget(PEGTransformer &transformer,
	                                            vector<PivotColumnEntry> pivot_target_list);
	static unique_ptr<TransformResultValue> TransformUnpivotValueListInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static PivotColumn TransformUnpivotValueList(PEGTransformer &transformer, const vector<string> &unpivot_header,
	                                             vector<PivotColumnEntry> unpivot_target_list);
	static unique_ptr<TransformResultValue> TransformPivotTargetListInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static vector<PivotColumnEntry> TransformPivotTargetList(PEGTransformer &transformer,
	                                                         vector<unique_ptr<ParsedExpression>> target_list);
	static unique_ptr<TransformResultValue> TransformUnpivotTargetListInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<PivotColumnEntry> TransformUnpivotTargetList(PEGTransformer &transformer,
	                                                           vector<unique_ptr<ParsedExpression>> target_list);
	static unique_ptr<TransformResultValue> TransformLateralInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static bool TransformLateral(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformBaseTableNameInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformUnqualifiedBaseTableNameInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<BaseTableRef> TransformUnqualifiedBaseTableName(PEGTransformer &transformer,
	                                                                  const Identifier &table_name);
	static unique_ptr<TransformResultValue> TransformQualifiedTableNameInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSchemaReservedTableInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<BaseTableRef> TransformSchemaReservedTable(PEGTransformer &transformer,
	                                                             const Identifier &schema_qualification,
	                                                             const Identifier &reserved_table_name);
	static unique_ptr<TransformResultValue> TransformCatalogReservedSchemaTableInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static unique_ptr<BaseTableRef>
	TransformCatalogReservedSchemaTable(PEGTransformer &transformer, const Identifier &catalog_qualification,
	                                    const vector<Identifier> &reserved_schema_qualification,
	                                    const Identifier &reserved_table_name);
	static unique_ptr<TransformResultValue> TransformTableFunctionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTableFunctionLateralOptInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<TableRef> TransformTableFunctionLateralOpt(PEGTransformer &transformer,
	                                                             const optional<bool> &lateral,
	                                                             const QualifiedName &qualified_table_function,
	                                                             vector<FunctionArgument> table_function_arguments,
	                                                             const optional<bool> &with_ordinality,
	                                                             const optional<TableAlias> &table_alias);
	static unique_ptr<TransformResultValue> TransformTableFunctionAliasColonInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<TableRef> TransformTableFunctionAliasColon(PEGTransformer &transformer,
	                                                             const Identifier &table_alias_colon,
	                                                             const QualifiedName &qualified_table_function,
	                                                             vector<FunctionArgument> table_function_arguments,
	                                                             const optional<bool> &with_ordinality,
	                                                             optional<unique_ptr<SampleOptions>> sample_clause);
	static unique_ptr<TransformResultValue> TransformWithOrdinalityInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformWithOrdinality(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformQualifiedTableFunctionInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static QualifiedName TransformQualifiedTableFunction(PEGTransformer &transformer,
	                                                     const optional<Identifier> &catalog_qualification,
	                                                     const optional<vector<Identifier>> &schema_qualification,
	                                                     const Identifier &table_function_name);
	static unique_ptr<TransformResultValue> TransformTableFunctionArgumentsInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static vector<FunctionArgument>
	TransformTableFunctionArguments(PEGTransformer &transformer, optional<vector<FunctionArgument>> function_argument);
	static unique_ptr<TransformResultValue> TransformFunctionArgumentInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNamedFunctionArgumentInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static FunctionArgument TransformNamedFunctionArgument(PEGTransformer &transformer, MacroParameter named_parameter);
	static unique_ptr<TransformResultValue> TransformPositionalFunctionArgumentInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static FunctionArgument TransformPositionalFunctionArgument(PEGTransformer &transformer,
	                                                            unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformNamedParameterInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static MacroParameter TransformNamedParameter(PEGTransformer &transformer, const Identifier &type_func_name,
	                                              const optional<LogicalType> &type,
	                                              unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformTableAliasInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTableAliasAsInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static TableAlias TransformTableAliasAs(PEGTransformer &transformer,
	                                        const QualifiedName &identifier_or_string_literal,
	                                        const optional<vector<string>> &column_aliases);
	static unique_ptr<TransformResultValue> TransformTableAliasWithoutAsInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static TableAlias TransformTableAliasWithoutAs(PEGTransformer &transformer, const Identifier &identifier,
	                                               const optional<vector<string>> &column_aliases);
	static unique_ptr<TransformResultValue> TransformAtClauseInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<AtClause> TransformAtClause(PEGTransformer &transformer, unique_ptr<AtClause> at_specifier);
	static unique_ptr<TransformResultValue> TransformAtSpecifierInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<AtClause> TransformAtSpecifier(PEGTransformer &transformer, const string &at_unit,
	                                                 unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformAtUnitInternal(PEGTransformer &transformer,
	                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformVersionAtUnitInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static string TransformVersionAtUnit(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTimestampAtUnitInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static string TransformTimestampAtUnit(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformJoinClauseInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNearestJoinClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNearestJoinAliasedInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TableRef>
	TransformNearestJoinAliased(PEGTransformer &transformer, const optional<JoinType> &join_type,
	                            unique_ptr<TableRef> table_ref, const optional<bool> &approx_or_exact,
	                            optional<unique_ptr<ParsedExpression>> number_literal,
	                            const OrderType &distance_or_similarity, unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformNearestJoinBareInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TableRef>
	TransformNearestJoinBare(PEGTransformer &transformer, const optional<JoinType> &join_type,
	                         unique_ptr<TableRef> nearest_bare_table_ref, const optional<bool> &approx_or_exact,
	                         optional<unique_ptr<ParsedExpression>> number_literal,
	                         const OrderType &distance_or_similarity, unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformNearestBareTableRefInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNearestValuesRefInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TableRef> TransformNearestValuesRef(PEGTransformer &transformer,
	                                                      unique_ptr<SelectStatement> values_clause);
	static unique_ptr<TransformResultValue> TransformNearestTableFunctionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TableRef> TransformNearestTableFunction(PEGTransformer &transformer,
	                                                          const optional<bool> &lateral,
	                                                          const QualifiedName &qualified_table_function,
	                                                          vector<FunctionArgument> table_function_arguments,
	                                                          const optional<bool> &with_ordinality);
	static unique_ptr<TransformResultValue> TransformNearestTableSubqueryInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TableRef> TransformNearestTableSubquery(PEGTransformer &transformer,
	                                                          const optional<bool> &lateral,
	                                                          unique_ptr<TableRef> subquery_reference);
	static unique_ptr<TransformResultValue> TransformNearestBaseTableRefInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TableRef> TransformNearestBaseTableRef(PEGTransformer &transformer,
	                                                         unique_ptr<BaseTableRef> base_table_name,
	                                                         optional<unique_ptr<AtClause>> at_clause,
	                                                         optional<unique_ptr<SampleOptions>> sample_clause);
	static unique_ptr<TransformResultValue> TransformNearestParensTableRefInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<TableRef> TransformNearestParensTableRef(PEGTransformer &transformer,
	                                                           unique_ptr<TableRef> table_ref,
	                                                           optional<unique_ptr<SampleOptions>> sample_clause);
	static unique_ptr<TransformResultValue> TransformApproxOrExactInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNearestApproxInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static bool TransformNearestApprox(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNearestExactInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformNearestExact(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDistanceOrSimilarityInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNearestDistanceInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static OrderType TransformNearestDistance(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNearestSimilarityInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static OrderType TransformNearestSimilarity(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRegularJoinClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TableRef> TransformRegularJoinClause(PEGTransformer &transformer, const optional<bool> &asof,
	                                                       const optional<JoinType> &join_type,
	                                                       unique_ptr<TableRef> table_ref,
	                                                       JoinQualifier join_qualifier);
	static unique_ptr<TransformResultValue> TransformJoinByClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TableRef> TransformJoinByClause(PEGTransformer &transformer, const string &col_label,
	                                                  unique_ptr<TableRef> table_ref, JoinQualifier join_qualifier);
	static unique_ptr<TransformResultValue> TransformAsofInternal(PEGTransformer &transformer,
	                                                              ParseResult &parse_result);
	static bool TransformAsof(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformJoinWithoutOnClauseInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TableRef> TransformJoinWithoutOnClause(PEGTransformer &transformer, const JoinPrefix &join_prefix,
	                                                         unique_ptr<TableRef> inner_table_ref);
	static unique_ptr<TransformResultValue> TransformJoinQualifierInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOnClauseInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static JoinQualifier TransformOnClause(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformUsingClauseInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static JoinQualifier TransformUsingClause(PEGTransformer &transformer, const vector<Identifier> &column_name);
	static unique_ptr<TransformResultValue> TransformJoinTypeInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformJoinPrefixInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCrossJoinPrefixInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static JoinPrefix TransformCrossJoinPrefix(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNaturalJoinPrefixInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static JoinPrefix TransformNaturalJoinPrefix(PEGTransformer &transformer, const optional<JoinType> &join_type);
	static unique_ptr<TransformResultValue> TransformPositionalJoinPrefixInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static JoinPrefix TransformPositionalJoinPrefix(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFullJoinInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static JoinType TransformFullJoin(PEGTransformer &transformer, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformLeftJoinInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static JoinType TransformLeftJoin(PEGTransformer &transformer, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformRightJoinInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static JoinType TransformRightJoin(PEGTransformer &transformer, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformSemiJoinInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static JoinType TransformSemiJoin(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformAntiJoinInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static JoinType TransformAntiJoin(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInnerJoinInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static JoinType TransformInnerJoin(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFromClauseInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TableRef> TransformFromClause(PEGTransformer &transformer,
	                                                vector<unique_ptr<TableRef>> table_ref);
	static unique_ptr<TransformResultValue> TransformWhereClauseInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformWhereClause(PEGTransformer &transformer,
	                                                         unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformGroupByClauseInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static GroupByNode TransformGroupByClause(PEGTransformer &transformer, GroupByNode group_by_expressions);
	static unique_ptr<TransformResultValue> TransformHavingClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformHavingClause(PEGTransformer &transformer,
	                                                          unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformQualifyClauseInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformQualifyClause(PEGTransformer &transformer,
	                                                           unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformSampleClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<SampleOptions> TransformSampleClause(PEGTransformer &transformer,
	                                                       unique_ptr<SampleOptions> sample_entry);
	static unique_ptr<TransformResultValue> TransformWindowClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformWindowClause(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> window_definition);
	static unique_ptr<TransformResultValue> TransformSampleEntryInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSampleEntryCountInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SampleOptions>
	TransformSampleEntryCount(PEGTransformer &transformer, unique_ptr<SampleOptions> sample_count,
	                          const optional<pair<SampleMethod, optional_idx>> &sample_properties);
	static unique_ptr<TransformResultValue> TransformSampleEntryFunctionInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<SampleOptions> TransformSampleEntryFunction(PEGTransformer &transformer,
	                                                              const optional<SampleMethod> &sample_function,
	                                                              unique_ptr<SampleOptions> sample_count,
	                                                              const optional<optional_idx> &repeatable_sample);
	static unique_ptr<TransformResultValue> TransformSampleFunctionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static SampleMethod TransformSampleFunction(PEGTransformer &transformer, const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformSamplePropertiesInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static pair<SampleMethod, optional_idx> TransformSampleProperties(PEGTransformer &transformer,
	                                                                  const Identifier &col_id,
	                                                                  const optional<optional_idx> &sample_seed);
	static unique_ptr<TransformResultValue> TransformRepeatableSampleInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static optional_idx TransformRepeatableSample(PEGTransformer &transformer, const optional_idx &sample_seed);
	static unique_ptr<TransformResultValue> TransformSampleSeedInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static optional_idx TransformSampleSeed(PEGTransformer &transformer, unique_ptr<ParsedExpression> number_literal);
	static unique_ptr<TransformResultValue> TransformSampleCountInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<SampleOptions> TransformSampleCount(PEGTransformer &transformer,
	                                                      unique_ptr<ParsedExpression> sample_value,
	                                                      const optional<bool> &sample_unit);
	static unique_ptr<TransformResultValue> TransformSampleValueInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSampleUnitInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSamplePercentageInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static bool TransformSamplePercentage(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSampleRowsInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static bool TransformSampleRows(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformGroupByExpressionsInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformGroupByAllInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static GroupByNode TransformGroupByAll(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformGroupByListInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static GroupByNode TransformGroupByList(PEGTransformer &transformer,
	                                        vector<GroupByExpressionInfo> group_by_expression);
	static unique_ptr<TransformResultValue> TransformGroupByExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformGroupByBaseExpressionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static GroupByExpressionInfo TransformGroupByBaseExpression(PEGTransformer &transformer,
	                                                            unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformEmptyGroupingItemInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static GroupByExpressionInfo TransformEmptyGroupingItem(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCubeOrRollupClauseInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static GroupByExpressionInfo TransformCubeOrRollupClause(PEGTransformer &transformer, const string &cube_or_rollup,
	                                                         optional<vector<unique_ptr<ParsedExpression>>> expression);
	static unique_ptr<TransformResultValue> TransformCubeOrRollupInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCubeKeywordInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static string TransformCubeKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRollupKeywordInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static string TransformRollupKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformGroupingSetsClauseInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static GroupByExpressionInfo TransformGroupingSetsClause(PEGTransformer &transformer,
	                                                         vector<GroupByExpressionInfo> group_by_expression);
	static unique_ptr<TransformResultValue> TransformSubqueryReferenceInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TableRef> TransformSubqueryReference(PEGTransformer &transformer,
	                                                       unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformOrderByExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static OrderByNode TransformOrderByExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                                              const optional<OrderType> &desc_or_asc,
	                                              const optional<OrderByNullType> &nulls_first_or_last);
	static unique_ptr<TransformResultValue> TransformDescOrAscInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDescendingOrderInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static OrderType TransformDescendingOrder(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformAscendingOrderInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static OrderType TransformAscendingOrder(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNullsFirstOrLastInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNullsFirstInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static OrderByNullType TransformNullsFirst(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNullsLastInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static OrderByNullType TransformNullsLast(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOrderByClauseInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static vector<OrderByNode> TransformOrderByClause(PEGTransformer &transformer,
	                                                  vector<OrderByNode> order_by_expressions);
	static unique_ptr<TransformResultValue> TransformOrderByExpressionsInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOrderByExpressionListInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static vector<OrderByNode> TransformOrderByExpressionList(PEGTransformer &transformer,
	                                                          vector<OrderByNode> order_by_expression);
	static unique_ptr<TransformResultValue> TransformOrderByAllInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static vector<OrderByNode> TransformOrderByAll(PEGTransformer &transformer, const optional<OrderType> &desc_or_asc,
	                                               const optional<OrderByNullType> &nulls_first_or_last);
	static unique_ptr<TransformResultValue> TransformLimitClauseInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static LimitPercentResult TransformLimitClause(PEGTransformer &transformer, LimitPercentResult limit_value);
	static unique_ptr<TransformResultValue> TransformOffsetClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static LimitPercentResult TransformOffsetClause(PEGTransformer &transformer, LimitPercentResult offset_value);
	static unique_ptr<TransformResultValue> TransformOffsetValueInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static LimitPercentResult TransformOffsetValue(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                                               const bool &has_result);
	static unique_ptr<TransformResultValue> TransformLimitValueInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLimitAllInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static LimitPercentResult TransformLimitAll(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformLimitLiteralPercentInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static LimitPercentResult TransformLimitLiteralPercent(PEGTransformer &transformer,
	                                                       unique_ptr<ParsedExpression> number_literal);
	static unique_ptr<TransformResultValue> TransformLimitExpressionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static LimitPercentResult TransformLimitExpression(PEGTransformer &transformer,
	                                                   unique_ptr<ParsedExpression> expression, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformFetchClauseInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformFetchValueInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static LimitPercentResult TransformFetchValue(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformAliasedExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformColIdExpressionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformColIdExpression(PEGTransformer &transformer, const Identifier &col_id,
	                                                             unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformExpressionAsCollabelInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformExpressionAsCollabel(PEGTransformer &transformer,
	                                                                  unique_ptr<ParsedExpression> expression,
	                                                                  const Identifier &col_label_or_string);
	static unique_ptr<TransformResultValue> TransformExpressionOptIdentifierInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformExpressionOptIdentifier(PEGTransformer &transformer,
	                                                                     unique_ptr<ParsedExpression> expression,
	                                                                     const optional<Identifier> &identifier);
	static unique_ptr<TransformResultValue> TransformValuesClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<SelectStatement>
	TransformValuesClause(PEGTransformer &transformer, vector<vector<unique_ptr<ParsedExpression>>> values_expressions);
	static unique_ptr<TransformResultValue> TransformValuesExpressionsInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformValuesExpressions(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformSetStatementInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformSetStatement(PEGTransformer &transformer,
	                                                      unique_ptr<SetStatement> set_assignment_or_time_zone);
	static unique_ptr<TransformResultValue> TransformSetAssignmentOrTimeZoneInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformResetStatementInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformResetStatement(PEGTransformer &transformer,
	                                                        const SettingInfo &set_variable_or_setting);
	static unique_ptr<TransformResultValue> TransformSetSchemaInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<SetStatement> TransformSetSchema(PEGTransformer &transformer, const string &string_literal);
	static unique_ptr<TransformResultValue> TransformStandardAssignmentInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<SetStatement> TransformStandardAssignment(PEGTransformer &transformer,
	                                                            const SettingInfo &set_variable_or_setting,
	                                                            vector<unique_ptr<ParsedExpression>> set_assignment);
	static unique_ptr<TransformResultValue> TransformSetVariableOrSettingInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSetTimeZoneInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<SetStatement> TransformSetTimeZone(PEGTransformer &transformer,
	                                                     unique_ptr<ParsedExpression> zone_value);
	static unique_ptr<TransformResultValue> TransformZoneValueInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformZoneLocalInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformZoneLocal(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformZoneDefaultInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformZoneDefault(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformZoneStringLiteralInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformZoneStringLiteral(PEGTransformer &transformer,
	                                                               const string &string_literal);
	static unique_ptr<TransformResultValue> TransformZoneIdentifierInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformZoneIdentifier(PEGTransformer &transformer,
	                                                            const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformZoneIntervalWithIntervalInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformZoneIntervalWithInterval(PEGTransformer &transformer,
	                                                                      const string &string_literal,
	                                                                      const optional<DatePartSpecifier> &interval);
	static unique_ptr<TransformResultValue> TransformZoneIntervalWithPrecisionInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformZoneIntervalWithPrecision(PEGTransformer &transformer,
	                                                                       unique_ptr<ParsedExpression> number_literal,
	                                                                       const string &string_literal);
	static unique_ptr<TransformResultValue> TransformSetSettingInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static SettingInfo TransformSetSetting(PEGTransformer &transformer, const optional<SetScope> &setting_scope,
	                                       const Identifier &setting_name);
	static unique_ptr<TransformResultValue> TransformSetVariableInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static SettingInfo TransformSetVariable(PEGTransformer &transformer, const SetScope &variable_scope,
	                                        const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformVariableScopeInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static SetScope TransformVariableScope(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSettingScopeInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLocalScopeInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static SetScope TransformLocalScope(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSessionScopeInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static SetScope TransformSessionScope(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformGlobalScopeInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static SetScope TransformGlobalScope(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSetAssignmentInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformSetAssignment(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> variable_list);
	static unique_ptr<TransformResultValue> TransformVariableListInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformVariableList(PEGTransformer &transformer,
	                                                                  vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformTransactionStatementInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformBeginTransactionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformBeginTransaction(PEGTransformer &transformer, const bool &has_result,
	                                                          const optional<TransactionModifierType> &read_or_write);
	static unique_ptr<TransformResultValue> TransformRollbackTransactionInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformRollbackTransaction(PEGTransformer &transformer, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformCommitTransactionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCommitTransaction(PEGTransformer &transformer, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformReadOrWriteInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static TransactionModifierType TransformReadOrWrite(PEGTransformer &transformer,
	                                                    const TransactionModifierType &read_only_or_read_write);
	static unique_ptr<TransformResultValue> TransformReadOnlyOrReadWriteInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformReadOnlyInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static TransactionModifierType TransformReadOnly(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformReadWriteInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static TransactionModifierType TransformReadWrite(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformUpdateStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformUpdateStatement(PEGTransformer &transformer, optional<CommonTableExpressionMap> with_clause,
	                         unique_ptr<TableRef> update_target, unique_ptr<UpdateSetInfo> update_set_clause,
	                         optional<unique_ptr<TableRef>> from_clause,
	                         optional<unique_ptr<ParsedExpression>> where_clause,
	                         optional<vector<unique_ptr<ParsedExpression>>> returning_clause);
	static unique_ptr<TransformResultValue> TransformUpdateTargetInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformBaseTableSetInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TableRef> TransformBaseTableSet(PEGTransformer &transformer,
	                                                  unique_ptr<BaseTableRef> base_table_name);
	static unique_ptr<TransformResultValue> TransformBaseTableAliasSetInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TableRef> TransformBaseTableAliasSet(PEGTransformer &transformer,
	                                                       unique_ptr<BaseTableRef> base_table_name,
	                                                       const optional<Identifier> &update_alias);
	static unique_ptr<TransformResultValue> TransformUpdateAliasInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static Identifier TransformUpdateAlias(PEGTransformer &transformer, const bool &has_result,
	                                       const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformUpdateSetClauseInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformUpdateSetTupleInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<UpdateSetInfo> TransformUpdateSetTuple(PEGTransformer &transformer,
	                                                         const vector<Identifier> &column_name,
	                                                         unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformUpdateSetElementListInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<UpdateSetInfo>
	TransformUpdateSetElementList(PEGTransformer &transformer,
	                              vector<pair<string, unique_ptr<ParsedExpression>>> update_set_element);
	static unique_ptr<TransformResultValue> TransformUpdateSetElementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static pair<string, unique_ptr<ParsedExpression>>
	TransformUpdateSetElement(PEGTransformer &transformer, const string &update_set_column_target,
	                          unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformUpdateSetColumnTargetInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static string TransformUpdateSetColumnTarget(PEGTransformer &transformer, const Identifier &column_name,
	                                             const optional<vector<Identifier>> &dot_identifier);
	static unique_ptr<TransformResultValue> TransformUseStatementInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformUseStatement(PEGTransformer &transformer, const QualifiedName &use_target);
	static unique_ptr<TransformResultValue> TransformUseTargetInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSchemaNameAsUseTargetInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static QualifiedName TransformSchemaNameAsUseTarget(PEGTransformer &transformer, const Identifier &schema_name);
	static unique_ptr<TransformResultValue> TransformCatalogNameAsUseTargetInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static QualifiedName TransformCatalogNameAsUseTarget(PEGTransformer &transformer, const Identifier &catalog_name);
	static unique_ptr<TransformResultValue> TransformUseTargetCatalogSchemaInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static QualifiedName TransformUseTargetCatalogSchema(PEGTransformer &transformer, const Identifier &catalog_name,
	                                                     const Identifier &reserved_schema_name,
	                                                     const optional<vector<Identifier>> &dot_identifier);
	static unique_ptr<TransformResultValue> TransformDotIdentifierInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static Identifier TransformDotIdentifier(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformVacuumStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformVacuumStatement(PEGTransformer &transformer,
	                                                         const optional<VacuumOptions> &vacuum_options,
	                                                         optional<AnalyzeTarget> analyze_target);
	static unique_ptr<TransformResultValue> TransformVacuumOptionsInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformVacuumParensOptionsInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static VacuumOptions TransformVacuumParensOptions(PEGTransformer &transformer, const vector<string> &vacuum_option);
	static unique_ptr<TransformResultValue> TransformVacuumLegacyOptionsInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static VacuumOptions TransformVacuumLegacyOptions(PEGTransformer &transformer, const optional<string> &opt_full,
	                                                  const optional<string> &opt_freeze,
	                                                  const optional<string> &opt_verbose,
	                                                  const optional<string> &opt_analyze);
	static unique_ptr<TransformResultValue> TransformVacuumOptionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOptAnalyzeInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static string TransformOptAnalyze(PEGTransformer &transformer, const Identifier &analyze_keyword);
	static unique_ptr<TransformResultValue> TransformOptFullInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static string TransformOptFull(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOptFreezeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static string TransformOptFreeze(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOptVerboseInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static string TransformOptVerbose(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNameListInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static vector<string> TransformNameList(PEGTransformer &transformer, const vector<Identifier> &col_id);
	//===--------------------------------------------------------------------===//
	// END GENERATED RULES
	//===--------------------------------------------------------------------===//

private:
	explicit PEGTransformerFactory(ParsedGrammar &grammar_p);
	ParsedGrammar &grammar;
};

} // namespace duckdb
