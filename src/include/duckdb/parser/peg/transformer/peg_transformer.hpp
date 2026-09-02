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

using transform_process_initialize_t = void (*)(PEGTransformer &transformer, TransformProcess &process);
using transform_process_finalize_t = unique_ptr<TransformResultValue> (*)(PEGTransformer &transformer,
                                                                          TransformProcess &process);

struct TransformProcessInfo {
	const char *name;
	transform_process_initialize_t initialize;
	transform_process_finalize_t finalize;
};

template <typename T>
unique_ptr<TypedTransformResult<T>> TryBridgeTransformResultValue(TransformResultValue &base_result);

struct TransformInput {
	ParseResult &parse_result;
	const TransformProcessInfo &info;
};

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
	TransformProcess(PEGTransformer &transformer, TransformInput input);

	void ReserveChildSlots(idx_t count);
	void SetChildResult(idx_t slot, unique_ptr<TransformResultValue> result);
	void PushChild(TransformInput input, idx_t slot);
	TransformStep Resume(unique_ptr<TransformResultValue> child_result);

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
	bool initialized = false;
	bool completed = false;
};

struct TransformStackFrame {
	explicit TransformStackFrame(PEGTransformer &transformer, TransformInput input);

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
			throw InternalException("Unexpected transformer result type for root rule '%s'", input.info.name);
		}
		return std::move(*result_value);
	}

	string FormatStack() const;

private:
	void PushFrame(TransformInput input);
	unique_ptr<TransformResultValue> ExecuteInternal(TransformInput input);

private:
	PEGTransformer &transformer;
	vector<unique_ptr<TransformStackFrame>> frames;
};

class PEGTransformer {
public:
	using AnyTransformFunction = grammar_transform_function_t;

	PEGTransformer(ArenaAllocator &allocator, TokenIterator &token_iterator, ParserOptions &options_p,
	               const CompiledGrammar &grammar_p)
	    : allocator(allocator), token_iterator(token_iterator), options(options_p), grammar(grammar_p) {
	}

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
	friend class TransformProcess;
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

typedef unique_ptr<TransformResultValue> (*transform_function_t)(PEGTransformer &transformer,
                                                                 ParseResult &parse_result);

struct TransformRule {
	const char *name;
	transform_function_t transform;
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

	static void InitializePivotStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePivotStatementTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeUnpivotStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUnpivotStatementTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeLiteralExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLiteralExpressionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializePrefixExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePrefixExpressionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeOverClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOverClauseTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeSelectStatementInternalTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSelectStatementInternalTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeSimpleSelectTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSimpleSelectTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeTableRefTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableRefTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeWithClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWithClauseTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeWindowDefinitionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWindowDefinitionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);

	//===--------------------------------------------------------------------===//
	// START GENERATED TRAMPOLINE RULES
	//===--------------------------------------------------------------------===//
	static void InitializeStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeStatementTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeAlterStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAlterStatementTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeAlterOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAlterOptionsTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeAlterTableStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAlterTableStmtTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeAlterSchemaStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAlterSchemaStmtTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeAlterTableOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAlterTableOptionsTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeAddConstraintTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAddConstraintTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeAddColumnTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAddColumnTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeAddColumnEntryTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAddColumnEntryTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeDropColumnTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropColumnTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeAlterColumnTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAlterColumnTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeRenameColumnTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRenameColumnTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeNestedColumnNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNestedColumnNameTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeIdentifierDotTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIdentifierDotTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeRenameAlterTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRenameAlterTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeSetPartitionedByTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetPartitionedByTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeResetPartitionedByTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeResetPartitionedByTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeSetSortedByTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetSortedByTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeResetSortedByTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeResetSortedByTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeSetOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetOptionsTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeResetOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeResetOptionsTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeAlterColumnEntryTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAlterColumnEntryTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeAddOrDropDefaultTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAddOrDropDefaultTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeAddDefaultTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAddDefaultTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeDropDefaultTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropDefaultTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeChangeNullabilityTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeChangeNullabilityTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeDropOrSetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropOrSetTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeDropNullabilityTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropNullabilityTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeSetNullabilityTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetNullabilityTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeAlterTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAlterTypeTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeUsingExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUsingExpressionTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeAlterViewStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAlterViewStmtTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeAlterSequenceStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAlterSequenceStmtTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeQualifiedSequenceNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeQualifiedSequenceNameTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeAlterSequenceOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAlterSequenceOptionsTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeRenameAlterSequenceOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRenameAlterSequenceOptionsTrampoline(PEGTransformer &transformer,
	                                                                                     TransformProcess &process);
	static void InitializeSetSequenceOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetSequenceOptionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeAlterDatabaseStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAlterDatabaseStmtTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeAnalyzeStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAnalyzeStatementTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeAnalyzeTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAnalyzeTargetTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeAnalyzeVerboseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAnalyzeVerboseTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeAttachStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAttachStatementTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeDatabasePathTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDatabasePathTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeAttachAliasTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAttachAliasTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeAttachOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAttachOptionsTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeCallStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCallStatementTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeCheckpointStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCheckpointStatementTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeCheckpointForceTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCheckpointForceTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeCommentStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommentStatementTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeCommentOnTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommentOnTypeTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeCommentTableTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommentTableTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeCommentSequenceTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommentSequenceTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeCommentFunctionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommentFunctionTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeCommentMacroTableTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommentMacroTableTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeCommentMacroTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommentMacroTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeCommentViewTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommentViewTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeCommentDatabaseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommentDatabaseTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeCommentIndexTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommentIndexTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeCommentSchemaTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommentSchemaTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeCommentTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommentTypeTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeCommentColumnTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommentColumnTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeCommentValueTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommentValueTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeStringLiteralValueTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeStringLiteralValueTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeAnalyzeKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAnalyzeKeywordTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeExpressionStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExpressionStatementTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeExpressionAliasTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExpressionAliasTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeIndexNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIndexNameTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeConstraintNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeConstraintNameTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeSequenceNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSequenceNameTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeCollationNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCollationNameTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeNumberLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNumberLiteralTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeStringLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeStringLiteralTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTypeTrampoline(PEGTransformer &transformer,
	                                                               TransformProcess &process);
	static void InitializeTypeVariationsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTypeVariationsTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeSimpleTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSimpleTypeTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeCharacterSimpleTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCharacterSimpleTypeTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeQualifiedSimpleTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeQualifiedSimpleTypeTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeIntervalTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntervalTypeTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeIntervalIntervalTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntervalIntervalTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeIntervalWithSpecifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntervalWithSpecifierTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeIntervalWithRangeSpecifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntervalWithRangeSpecifierTrampoline(PEGTransformer &transformer,
	                                                                                     TransformProcess &process);
	static void InitializeIntervalWithSimpleSpecifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntervalWithSimpleSpecifierTrampoline(PEGTransformer &transformer,
	                                                                                      TransformProcess &process);
	static void InitializeIntervalWithoutSpecifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntervalWithoutSpecifierTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializeIntervalToIntervalAsTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntervalToIntervalAsTypeTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializeYearKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeYearKeywordTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeMonthKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMonthKeywordTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeDayKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDayKeywordTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeHourKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeHourKeywordTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeMinuteKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMinuteKeywordTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeSecondKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSecondKeywordTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeMillisecondKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMillisecondKeywordTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeMicrosecondKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMicrosecondKeywordTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeWeekKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWeekKeywordTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeQuarterKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeQuarterKeywordTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeDecadeKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDecadeKeywordTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeCenturyKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCenturyKeywordTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeMillenniumKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMillenniumKeywordTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeIntervalTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntervalTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeIntervalToIntervalTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntervalToIntervalTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeYearToMonthTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeYearToMonthTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeDayToHourTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDayToHourTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeDayToMinuteTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDayToMinuteTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeDayToSecondTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDayToSecondTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeHourToMinuteTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeHourToMinuteTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeHourToSecondTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeHourToSecondTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeMinuteToSecondTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMinuteToSecondTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeBitTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBitTypeTrampoline(PEGTransformer &transformer,
	                                                                  TransformProcess &process);
	static void InitializeGeometryTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGeometryTypeTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeVariantTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeVariantTypeTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeNumericTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNumericTypeTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeSimpleNumericTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSimpleNumericTypeTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeDecimalNumericTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDecimalNumericTypeTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeIntTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntTypeTrampoline(PEGTransformer &transformer,
	                                                                  TransformProcess &process);
	static void InitializeIntegerTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntegerTypeTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeSmallintTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSmallintTypeTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeBigintTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBigintTypeTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeRealTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRealTypeTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeBooleanTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBooleanTypeTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeDoubleTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDoubleTypeTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeFloatTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFloatTypeTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeDecimalTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDecimalTypeTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeDecTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDecTypeTrampoline(PEGTransformer &transformer,
	                                                                  TransformProcess &process);
	static void InitializeNumericModTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNumericModTypeTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeQualifiedTypeNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeQualifiedTypeNameTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeTypeNameAsQualifiedNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTypeNameAsQualifiedNameTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeCatalogReservedSchemaTypeNameTrampoline(PEGTransformer &transformer,
	                                                              TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCatalogReservedSchemaTypeNameTrampoline(PEGTransformer &transformer,
	                                                                                        TransformProcess &process);
	static void InitializeSchemaReservedTypeNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSchemaReservedTypeNameTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeTypeModifiersTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTypeModifiersTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeRowTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRowTypeTrampoline(PEGTransformer &transformer,
	                                                                  TransformProcess &process);
	static void InitializeSetofTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetofTypeTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeUnionTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUnionTypeTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeColIdTypeListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColIdTypeListTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeMapTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMapTypeTrampoline(PEGTransformer &transformer,
	                                                                  TransformProcess &process);
	static void InitializeTupleTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTupleTypeTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeColIdTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColIdTypeTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeArrayBoundsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeArrayBoundsTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeArrayKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeArrayKeywordTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeArrayKeywordWithBoundsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeArrayKeywordWithBoundsTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeSquareBracketsArrayTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSquareBracketsArrayTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeTimeTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTimeTypeTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeTimeOrTimestampTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTimeOrTimestampTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeTimeTypeIdTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTimeTypeIdTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeTimestampTypeIdTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTimestampTypeIdTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeTimeZoneTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTimeZoneTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeWithOrWithoutTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWithOrWithoutTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeWithRuleTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWithRuleTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeWithoutRuleTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWithoutRuleTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeConnectStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeConnectStatementTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeDisconnectStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDisconnectStatementTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeSessionTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSessionTargetTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeLocalSessionTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLocalSessionTargetTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeStringSessionTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeStringSessionTargetTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeCatalogSessionTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCatalogSessionTargetTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeCopyStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyStatementTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeCopyVariationsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyVariationsTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeCopyTableTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyTableTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeFromOrToTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFromOrToTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeCopyFromTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyFromTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeCopyToTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyToTrampoline(PEGTransformer &transformer,
	                                                                 TransformProcess &process);
	static void InitializeCopySelectTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopySelectTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeCopyFileNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyFileNameTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeCopyFileNameExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyFileNameExpressionTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeCopyFileNameStringLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyFileNameStringLiteralTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializeCopyFileNameIdentifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyFileNameIdentifierTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeCopyFileNameIdentifierColIdTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyFileNameIdentifierColIdTrampoline(PEGTransformer &transformer,
	                                                                                      TransformProcess &process);
	static void InitializeIdentifierColIdTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIdentifierColIdTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeCopyOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyOptionsTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeCopyOptionListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyOptionListTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeSpecializedOptionListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSpecializedOptionListTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeSpecializedOptionTailTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSpecializedOptionTailTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeSpecializedOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSpecializedOptionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeSingleOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSingleOptionTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeBinaryOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBinaryOptionTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeFreezeOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFreezeOptionTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeOidsOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOidsOptionTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeCsvOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCsvOptionTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeHeaderOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeHeaderOptionTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeNullAsOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNullAsOptionTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeDelimiterAsOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDelimiterAsOptionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeQuoteAsOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeQuoteAsOptionTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeEscapeAsOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeEscapeAsOptionTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeEncodingOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeEncodingOptionTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeForceQuoteOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeForceQuoteOptionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeStarSymbolColumnListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeStarSymbolColumnListTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeForceQuoteTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeForceQuoteTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializePartitionByOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePartitionByOptionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializePartitionByColumnListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePartitionByColumnListTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeStarPartitionByColumnListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeStarPartitionByColumnListTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializeParenthesizedPartitionByColumnListTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static unique_ptr<TransformResultValue>
	FinalizeParenthesizedPartitionByColumnListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static void InitializeSinglePartitionByColumnListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSinglePartitionByColumnListTrampoline(PEGTransformer &transformer,
	                                                                                      TransformProcess &process);
	static void InitializeForceNullOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeForceNullOptionTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeForceNotNullTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeForceNotNullTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeCopyGenericOptionListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyGenericOptionListTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeCopyGenericOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyGenericOptionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeOrderByCopyOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOrderByCopyOptionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializePartitionedByCopyOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePartitionedByCopyOptionTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeGenericCopyOptionListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGenericCopyOptionListTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeGenericCopyOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGenericCopyOptionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeGenericCopyOptionValueTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGenericCopyOptionValueTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeGenericCopyOptionOrderListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGenericCopyOptionOrderListTrampoline(PEGTransformer &transformer,
	                                                                                     TransformProcess &process);
	static void InitializeGenericCopyOptionExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGenericCopyOptionExpressionTrampoline(PEGTransformer &transformer,
	                                                                                      TransformProcess &process);
	static void InitializeGenericCopyOptionParenthesizedExpressionListTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static unique_ptr<TransformResultValue>
	FinalizeGenericCopyOptionParenthesizedExpressionListTrampoline(PEGTransformer &transformer,
	                                                               TransformProcess &process);
	static void InitializeCopyFromDatabaseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyFromDatabaseTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeCopyFromDatabaseWithFlagTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyFromDatabaseWithFlagTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializeCopyFromDatabaseWithoutFlagTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyFromDatabaseWithoutFlagTrampoline(PEGTransformer &transformer,
	                                                                                      TransformProcess &process);
	static void InitializeCopyDatabaseFlagTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyDatabaseFlagTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeSchemaOrDataTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSchemaOrDataTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeCopySchemaTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopySchemaTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeCopyDataTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCopyDataTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeCreateIndexStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateIndexStmtTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeWithListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWithListTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeRelOptionOrOidsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRelOptionOrOidsTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeRelOptionListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRelOptionListTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeOidsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOidsTrampoline(PEGTransformer &transformer,
	                                                               TransformProcess &process);
	static void InitializeWithOrWithoutOidsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWithOrWithoutOidsTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeWithOidsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWithOidsTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeWithoutOidsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWithoutOidsTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeIndexElementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIndexElementTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeUniqueIndexTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUniqueIndexTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeIndexTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIndexTypeTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeRelOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRelOptionTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeRelOptionNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRelOptionNameTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeDottedIdentifierStringTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDottedIdentifierStringTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeRelOptionArgumentOptTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRelOptionArgumentOptTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeDefArgTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDefArgTrampoline(PEGTransformer &transformer,
	                                                                 TransformProcess &process);
	static void InitializeDefArgNullTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDefArgNullTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeDefArgKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDefArgKeywordTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeDefArgStringLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDefArgStringLiteralTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeNoneLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNoneLiteralTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeCreateMacroStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateMacroStmtTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeMacroOrFunctionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMacroOrFunctionTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeMacroKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMacroKeywordTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeFunctionKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFunctionKeywordTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeMacroDefinitionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMacroDefinitionTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeMacroDefinitionBodyTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMacroDefinitionBodyTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeMacroParametersTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMacroParametersTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeMacroParameterTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMacroParameterTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeSimpleParameterTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSimpleParameterTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeScalarMacroDefinitionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeScalarMacroDefinitionTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeTableMacroDefinitionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableMacroDefinitionTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeCreateSchemaStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateSchemaStmtTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeCreateSecretStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateSecretStmtTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeSecretStorageSpecifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSecretStorageSpecifierTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeSecretNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSecretNameTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeCreateSequenceStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateSequenceStmtTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeSequenceOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSequenceOptionTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeSeqSetCycleTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSeqSetCycleTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeSeqCycleTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSeqCycleTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeSeqNoCycleTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSeqNoCycleTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeSeqSetIncrementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSeqSetIncrementTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeSeqSetMinMaxTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSeqSetMinMaxTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeSeqNoMinMaxTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSeqNoMinMaxTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeSeqStartWithTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSeqStartWithTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeSeqOwnedByTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSeqOwnedByTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeSeqMinOrMaxTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSeqMinOrMaxTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeMinValueTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMinValueTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeMaxValueTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMaxValueTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeCreateStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateStatementTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeCreateStatementVariationTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateStatementVariationTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializeOrReplaceTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOrReplaceTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeTemporaryTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTemporaryTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializePersistentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePersistentTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeTempPersistentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTempPersistentTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeTemporaryPersistentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTemporaryPersistentTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeCreateTableStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateTableStmtTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeCreateTableDefinitionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateTableDefinitionTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeCreateTableAsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateTableAsTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializePartitionSortedOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePartitionSortedOptionsTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializePartitionOptSortedOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePartitionOptSortedOptionsTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializeSortedOptPartitionOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSortedOptPartitionOptionsTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializePartitionOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePartitionOptionsTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeSortedOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSortedOptionsTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeWithDataTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWithDataTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeWithDataOnlyTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWithDataOnlyTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeWithNoDataTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWithNoDataTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeIdentifierListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIdentifierListTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeCreateColumnListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateColumnListTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeIfNotExistsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIfNotExistsTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeQualifiedNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeQualifiedNameTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeSchemaReservedIdentifierOrStringLiteralTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static unique_ptr<TransformResultValue>
	FinalizeSchemaReservedIdentifierOrStringLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static void InitializeCatalogReservedSchemaIdentifierTrampoline(PEGTransformer &transformer,
	                                                                TransformProcess &process);
	static unique_ptr<TransformResultValue>
	FinalizeCatalogReservedSchemaIdentifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static void InitializeIdentifierOrStringLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIdentifierOrStringLiteralTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializeReservedIdentifierOrStringLiteralTrampoline(PEGTransformer &transformer,
	                                                                  TransformProcess &process);
	static unique_ptr<TransformResultValue>
	FinalizeReservedIdentifierOrStringLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static void InitializeCatalogQualificationTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCatalogQualificationTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeSchemaQualificationTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSchemaQualificationTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeReservedSchemaQualificationTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReservedSchemaQualificationTrampoline(PEGTransformer &transformer,
	                                                                                      TransformProcess &process);
	static void InitializeTableQualificationTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableQualificationTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeReservedTableQualificationTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReservedTableQualificationTrampoline(PEGTransformer &transformer,
	                                                                                     TransformProcess &process);
	static void InitializeCreateTableColumnListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateTableColumnListTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeCreateTableColumnElementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateTableColumnElementTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializeCreateTableColumnDefinitionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateTableColumnDefinitionTrampoline(PEGTransformer &transformer,
	                                                                                      TransformProcess &process);
	static void InitializeCreateTableConstraintTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateTableConstraintTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeColumnDefinitionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColumnDefinitionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeColumnConstraintTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColumnConstraintTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeNotNullConstraintTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNotNullConstraintTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeNullConstraintTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNullConstraintTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeNotNullColumnConstraintTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNotNullColumnConstraintTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeUniqueConstraintTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUniqueConstraintTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializePrimaryKeyConstraintTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePrimaryKeyConstraintTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeDefaultValueTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDefaultValueTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeCheckConstraintTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCheckConstraintTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeForeignKeyConstraintTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeForeignKeyConstraintTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeColumnCollationTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColumnCollationTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeColumnCompressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColumnCompressionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeKeyActionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeKeyActionsTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeUpdateActionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateActionTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeDeleteActionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDeleteActionTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeKeyActionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeKeyActionTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeNoKeyActionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNoKeyActionTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeRestrictKeyActionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRestrictKeyActionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeCascadeKeyActionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCascadeKeyActionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeSetNullKeyActionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetNullKeyActionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeSetDefaultKeyActionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetDefaultKeyActionTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeTopLevelConstraintTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTopLevelConstraintTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeTopLevelConstraintListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTopLevelConstraintListTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeTopCheckConstraintTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTopCheckConstraintTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeTopPrimaryKeyConstraintTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTopPrimaryKeyConstraintTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeTopUniqueConstraintTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTopUniqueConstraintTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeTopForeignKeyConstraintTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTopForeignKeyConstraintTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeColumnIdListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColumnIdListTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeDottedIdentifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDottedIdentifierTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeDotColLabelTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDotColLabelTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeIdentifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIdentifierTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeColIdTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColIdTrampoline(PEGTransformer &transformer,
	                                                                TransformProcess &process);
	static void InitializeColIdOrStringTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColIdOrStringTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeTypeFuncNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTypeFuncNameTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeTypeFuncKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTypeFuncKeywordTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeColLabelTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColLabelTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeColLabelOrStringTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColLabelOrStringTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeColLabelIdentifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColLabelIdentifierTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeStringLiteralIdentifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeStringLiteralIdentifierTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeGeneratedColumnTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGeneratedColumnTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeGeneratedColumnTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGeneratedColumnTypeTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeCommitActionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommitActionTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializePreserveOrDeleteTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePreserveOrDeleteTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializePreserveRowsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePreserveRowsTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeDeleteRowsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDeleteRowsTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeVirtualGeneratedColumnTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeVirtualGeneratedColumnTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeStoredGeneratedColumnTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeStoredGeneratedColumnTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeCreateTriggerStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateTriggerStmtTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeTriggerBodyTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTriggerBodyTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeTriggerNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTriggerNameTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeReferencingClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReferencingClauseTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeReferencingItemTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReferencingItemTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeReferencingNewTableAsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReferencingNewTableAsTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeReferencingOldTableAsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReferencingOldTableAsTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeTriggerTimingTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTriggerTimingTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeTriggerBeforeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTriggerBeforeTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeTriggerAfterTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTriggerAfterTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeTriggerInsteadOfTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTriggerInsteadOfTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeTriggerEventTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTriggerEventTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeTriggerEventInsertTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTriggerEventInsertTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeTriggerEventDeleteTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTriggerEventDeleteTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeTriggerEventUpdateTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTriggerEventUpdateTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeTriggerEventUpdateOfTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTriggerEventUpdateOfTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeTriggerColumnListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTriggerColumnListTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeForEachClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeForEachClauseTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeForEachRowTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeForEachRowTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeForEachStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeForEachStatementTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeCreateTypeStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateTypeStmtTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeCreateTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateTypeTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeCreateTypeFromTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateTypeFromTypeTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeEnumSelectTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeEnumSelectTypeTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeEnumStringLiteralListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeEnumStringLiteralListTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeCreateViewStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateViewStmtTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeCreateRecursiveTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateRecursiveTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeCreateSecureTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateSecureTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeDeallocateStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDeallocateStatementTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeDeallocatePrepareTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDeallocatePrepareTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeDeleteStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDeleteStatementTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeTruncateStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTruncateStatementTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeTargetOptAliasTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTargetOptAliasTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeDeleteUsingClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDeleteUsingClauseTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeDescribeStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDescribeStatementTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeShowDeprecatedSelectTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeShowDeprecatedSelectTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeDescribeSelectTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDescribeSelectTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeShowAllTablesTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeShowAllTablesTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeShowTablesTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeShowTablesTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeShowByNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeShowByNameTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeDescribeByNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDescribeByNameTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeDescribeOrSummarizeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDescribeOrSummarizeTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeShowTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeShowTargetTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeShowDeprecatedQualifiedTableNameTrampoline(PEGTransformer &transformer,
	                                                                 TransformProcess &process);
	static unique_ptr<TransformResultValue>
	FinalizeShowDeprecatedQualifiedTableNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static void InitializeShowSettingNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeShowSettingNameTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeDescribeTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDescribeTargetTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeDescribeBaseTableNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDescribeBaseTableNameTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeDescribeStringLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDescribeStringLiteralTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeSummarizeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSummarizeTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeSummarizeRuleTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSummarizeRuleTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeShowOrDescribeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeShowOrDescribeTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeShowRuleTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeShowRuleTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeDescribeRuleTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDescribeRuleTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeDescribeLongRuleTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDescribeLongRuleTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeDescRuleTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDescRuleTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeDetachStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDetachStatementTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeDropStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropStatementTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeDropEntriesTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropEntriesTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeDropTriggerTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropTriggerTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeDropTableTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropTableTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeDropTableFunctionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropTableFunctionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeDropFunctionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropFunctionTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeDropSchemaTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropSchemaTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeDropIndexTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropIndexTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeQualifiedIndexNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeQualifiedIndexNameTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeQualifiedIndexNameStringTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeQualifiedIndexNameStringTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializeSchemaReservedIndexTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSchemaReservedIndexTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeCatalogReservedSchemaIndexTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCatalogReservedSchemaIndexTrampoline(PEGTransformer &transformer,
	                                                                                     TransformProcess &process);
	static void InitializeDropSequenceTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropSequenceTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeDropCollationTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropCollationTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeDropTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropTypeTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeDropSecretTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropSecretTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeTableOrViewTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableOrViewTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeMaterializedViewEntryTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMaterializedViewEntryTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeFunctionTypeMacroTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFunctionTypeMacroTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeFunctionTypeMacroKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFunctionTypeMacroKeywordTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializeFunctionTypeFunctionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFunctionTypeFunctionTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeDropBehaviorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropBehaviorTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeCascadeDropBehaviorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCascadeDropBehaviorTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeRestrictDropBehaviorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRestrictDropBehaviorTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeIfExistsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIfExistsTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeDropSecretStorageTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropSecretStorageTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeExecuteStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExecuteStatementTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeExplainStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExplainStatementTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeExplainOptionListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExplainOptionListTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeExplainOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExplainOptionTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeExplainOptionNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExplainOptionNameTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeExplainSelectStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExplainSelectStatementTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeExplainableStatementsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExplainableStatementsTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeExportStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExportStatementTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeExportSourceTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExportSourceTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeImportStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeImportStatementTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeColumnReferenceTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColumnReferenceTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeNestedSchemaTableColumnNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNestedSchemaTableColumnNameTrampoline(PEGTransformer &transformer,
	                                                                                      TransformProcess &process);
	static void InitializeCatalogReservedSchemaTableColumnNameTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static unique_ptr<TransformResultValue>
	FinalizeCatalogReservedSchemaTableColumnNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static void InitializeSchemaReservedTableColumnNameTrampoline(PEGTransformer &transformer,
	                                                              TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSchemaReservedTableColumnNameTrampoline(PEGTransformer &transformer,
	                                                                                        TransformProcess &process);
	static void InitializeTableReservedColumnNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableReservedColumnNameTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeFunctionExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFunctionExpressionTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeFunctionExpressionArgumentsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFunctionExpressionArgumentsTrampoline(PEGTransformer &transformer,
	                                                                                      TransformProcess &process);
	static void InitializeFunctionExpressionArgumentListTrampoline(PEGTransformer &transformer,
	                                                               TransformProcess &process);
	static unique_ptr<TransformResultValue>
	FinalizeFunctionExpressionArgumentListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static void InitializeFunctionArgumentListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFunctionArgumentListTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeFunctionIdentifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFunctionIdentifierTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeFunctionNameAsQualifiedNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFunctionNameAsQualifiedNameTrampoline(PEGTransformer &transformer,
	                                                                                      TransformProcess &process);
	static void InitializeCatalogReservedSchemaFunctionNameTrampoline(PEGTransformer &transformer,
	                                                                  TransformProcess &process);
	static unique_ptr<TransformResultValue>
	FinalizeCatalogReservedSchemaFunctionNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static void InitializeSchemaReservedFunctionNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSchemaReservedFunctionNameTrampoline(PEGTransformer &transformer,
	                                                                                     TransformProcess &process);
	static void InitializeDistinctOrAllTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDistinctOrAllTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeDistinctKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDistinctKeywordTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeAllKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAllKeywordTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeWithinGroupClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWithinGroupClauseTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeFilterClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFilterClauseTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeFilterClauseExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFilterClauseExpressionTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeFilterClauseContentsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFilterClauseContentsTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeIgnoreOrRespectNullsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIgnoreOrRespectNullsTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeIgnoreNullsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIgnoreNullsTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeRespectNullsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRespectNullsTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeParenthesisExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeParenthesisExpressionTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeConstantLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeConstantLiteralTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeNullLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNullLiteralTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeTrueLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTrueLiteralTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeFalseLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFalseLiteralTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeCastExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCastExpressionTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeCastArgumentsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCastArgumentsTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeCastOrTryCastTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCastOrTryCastTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeCastKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCastKeywordTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeTryCastKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTryCastKeywordTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeColIdDotTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColIdDotTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeStarExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeStarExpressionTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeStarQualifierListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeStarQualifierListTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeExcludeListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExcludeListTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeExcludeNamesTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExcludeNamesTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeExcludeNameListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExcludeNameListTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeExcludeNameSingleTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExcludeNameSingleTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeExcludeNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExcludeNameTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeExcludeDottedNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExcludeDottedNameTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeExcludeColumnNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExcludeColumnNameTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeReplaceListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReplaceListTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeReplaceEntriesTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReplaceEntriesTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeReplaceEntrySingleTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReplaceEntrySingleTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeReplaceEntryListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReplaceEntryListTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeReplaceEntryTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReplaceEntryTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeRenameListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRenameListTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeRenameEntriesTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRenameEntriesTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeRenameEntryListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRenameEntryListTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeSingleRenameEntryTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSingleRenameEntryTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeRenameEntryTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRenameEntryTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeSubqueryExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSubqueryExpressionTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeSubqueryNotTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSubqueryNotTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeSubqueryExistsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSubqueryExistsTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeCaseExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCaseExpressionTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeCaseWhenThenTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCaseWhenThenTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeCaseElseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCaseElseTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeTypeLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTypeLiteralTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeIntervalLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntervalLiteralTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeIntervalParameterTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntervalParameterTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeIntervalStringParameterTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntervalStringParameterTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeFrameClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFrameClauseTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeFramingTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFramingTrampoline(PEGTransformer &transformer,
	                                                                  TransformProcess &process);
	static void InitializeRowsFramingTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRowsFramingTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeRangeFramingTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRangeFramingTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeGroupsFramingTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGroupsFramingTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeFrameExtentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFrameExtentTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeSingleFrameExtentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSingleFrameExtentTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeBetweenFrameExtentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBetweenFrameExtentTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeFrameBoundTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFrameBoundTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeFrameUnboundedTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFrameUnboundedTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeFrameExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFrameExpressionTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeFrameCurrentRowTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFrameCurrentRowTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializePrecedingOrFollowingTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePrecedingOrFollowingTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializePrecedingFrameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePrecedingFrameTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeFollowingFrameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFollowingFrameTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeWindowExcludeClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWindowExcludeClauseTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeWindowExcludeElementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWindowExcludeElementTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeExcludeCurrentRowTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExcludeCurrentRowTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeExcludeGroupTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExcludeGroupTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeExcludeTiesTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExcludeTiesTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeExcludeNoOthersTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExcludeNoOthersTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeWindowFrameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWindowFrameTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeIdentifierWindowFrameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIdentifierWindowFrameTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeParensIdentifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeParensIdentifierTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeWindowFrameDefinitionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWindowFrameDefinitionTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeWindowFrameNameContentsParensTrampoline(PEGTransformer &transformer,
	                                                              TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWindowFrameNameContentsParensTrampoline(PEGTransformer &transformer,
	                                                                                        TransformProcess &process);
	static void InitializeWindowFrameNameContentsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWindowFrameNameContentsTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeWindowFrameContentsParensTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWindowFrameContentsParensTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializeWindowFrameContentsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWindowFrameContentsTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeBaseWindowNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBaseWindowNameTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeWindowPartitionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWindowPartitionTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeListExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeListExpressionTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeArrayBoundedListExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeArrayBoundedListExpressionTrampoline(PEGTransformer &transformer,
	                                                                                     TransformProcess &process);
	static void InitializeArrayParensSelectTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeArrayParensSelectTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeBoundedListExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBoundedListExpressionTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeStructExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeStructExpressionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeStructFieldTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeStructFieldTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeMapExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMapExpressionTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeMapStructExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMapStructExpressionTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeMapStructFieldTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMapStructFieldTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeGroupingExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGroupingExpressionTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeGroupingOrGroupingIdTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGroupingOrGroupingIdTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeGroupingKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGroupingKeywordTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeGroupingIdKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGroupingIdKeywordTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeParameterTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeParameterTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeQuestionMarkNumberedParameterTrampoline(PEGTransformer &transformer,
	                                                              TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeQuestionMarkNumberedParameterTrampoline(PEGTransformer &transformer,
	                                                                                        TransformProcess &process);
	static void InitializeAnonymousParameterTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAnonymousParameterTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeNumberedParameterTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNumberedParameterTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeColLabelParameterTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColLabelParameterTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializePositionalExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePositionalExpressionTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeDefaultExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDefaultExpressionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeListComprehensionExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeListComprehensionExpressionTrampoline(PEGTransformer &transformer,
	                                                                                      TransformProcess &process);
	static void InitializeListComprehensionFilterTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeListComprehensionFilterTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeParensExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeParensExpressionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeSingleExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSingleExpressionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExpressionTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeColumnDefaultExprTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColumnDefaultExprTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeLambdaArrowExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLambdaArrowExpressionTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeSingleArrowPairTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSingleArrowPairTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeLogicalOrExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLogicalOrExpressionTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeLogicalOrExpressionTailTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLogicalOrExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeColDefOrExprTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColDefOrExprTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeColDefOrExpressionTailTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColDefOrExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeLogicalAndExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLogicalAndExpressionTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeLogicalAndExpressionTailTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLogicalAndExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializeColDefAndExprTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColDefAndExprTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeColDefAndExpressionTailTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColDefAndExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeLogicalNotExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLogicalNotExpressionTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeNotExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNotExpressionTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeNotKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNotKeywordTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeIsExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIsExpressionTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeIsTestTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIsTestTrampoline(PEGTransformer &transformer,
	                                                                 TransformProcess &process);
	static void InitializeIsLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIsLiteralTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeIsLiteralValueTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIsLiteralValueTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeUnknownLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUnknownLiteralTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeNotNullTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNotNullTrampoline(PEGTransformer &transformer,
	                                                                  TransformProcess &process);
	static void InitializeNotNullKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNotNullKeywordTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeNotNullOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNotNullOperatorTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeIsNullTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIsNullTrampoline(PEGTransformer &transformer,
	                                                                 TransformProcess &process);
	static void InitializeIsNullOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIsNullOperatorTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeIsDistinctFromExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIsDistinctFromExpressionTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializeIsDistinctFromTailTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIsDistinctFromTailTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeIsDistinctFromOpTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIsDistinctFromOpTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeComparisonExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeComparisonExpressionTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeComparisonExpressionTailTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeComparisonExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializeComparisonOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeComparisonOperatorTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeOperatorEqualTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOperatorEqualTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeOperatorNotEqualTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOperatorNotEqualTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeOperatorLessThanTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOperatorLessThanTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeOperatorGreaterThanTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOperatorGreaterThanTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeOperatorLessThanEqualsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOperatorLessThanEqualsTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeOperatorGreaterThanEqualsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOperatorGreaterThanEqualsTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializeBetweenInLikeExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBetweenInLikeExpressionTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeBetweenInLikeOpTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBetweenInLikeOpTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeBetweenInLikeOpExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBetweenInLikeOpExpressionTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializeLikeClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLikeClauseTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeEscapeClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeEscapeClauseTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeLikeVariationsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLikeVariationsTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeLikeTokenTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLikeTokenTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeILikeTokenTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeILikeTokenTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeGlobTokenTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGlobTokenTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeSimilarToTokenTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSimilarToTokenTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeRegexMatchTokenTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRegexMatchTokenTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeRegexInsensitiveMatchTokenTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRegexInsensitiveMatchTokenTrampoline(PEGTransformer &transformer,
	                                                                                     TransformProcess &process);
	static void InitializeNotILikeOpTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNotILikeOpTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeNotLikeOpTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNotLikeOpTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeNotRegexInsensitiveMatchOpTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNotRegexInsensitiveMatchOpTrampoline(PEGTransformer &transformer,
	                                                                                     TransformProcess &process);
	static void InitializeNotSimilarToOpTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNotSimilarToOpTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeInClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInClauseTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeInExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInExpressionTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeInContainsExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInContainsExpressionTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeInExpressionListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInExpressionListTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeInSelectStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInSelectStatementTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeBetweenClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBetweenClauseTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeOtherOperatorExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOtherOperatorExpressionTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeOtherOperatorTailTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOtherOperatorTailTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeOtherOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOtherOperatorTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeAnyAllParsedOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAnyAllParsedOperatorTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeNamedOtherOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNamedOtherOperatorTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeOperatorLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOperatorLiteralTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeAnyAllOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAnyAllOperatorTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeAnyOrAllTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAnyOrAllTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeSubqueryAnyTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSubqueryAnyTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeSubqueryAllTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSubqueryAllTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeInetOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInetOperatorTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeJsonOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeJsonOperatorTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeListOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeListOperatorTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeStringOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeStringOperatorTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeQualifiedOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeQualifiedOperatorTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeQualifiedOperatorContentsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeQualifiedOperatorContentsTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializeAnyOpTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAnyOpTrampoline(PEGTransformer &transformer,
	                                                                TransformProcess &process);
	static void InitializeBitwiseExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBitwiseExpressionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeBitwiseExpressionTailTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBitwiseExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeBitOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBitOperatorTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeAdditiveExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAdditiveExpressionTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeAdditiveExpressionTailTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAdditiveExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeTermTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTermTrampoline(PEGTransformer &transformer,
	                                                               TransformProcess &process);
	static void InitializeMultiplicativeExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMultiplicativeExpressionTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializeMultiplicativeExpressionTailTrampoline(PEGTransformer &transformer,
	                                                             TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMultiplicativeExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                       TransformProcess &process);
	static void InitializeFactorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFactorTrampoline(PEGTransformer &transformer,
	                                                                 TransformProcess &process);
	static void InitializeExponentiationExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExponentiationExpressionTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializeExponentiationExpressionTailTrampoline(PEGTransformer &transformer,
	                                                             TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExponentiationExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                       TransformProcess &process);
	static void InitializeExponentOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExponentOperatorTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeCollateExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCollateExpressionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeCollateExpressionTailTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCollateExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeAtTimeZoneExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAtTimeZoneExpressionTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeAtTimeZoneExpressionTailTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAtTimeZoneExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializePrefixOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePrefixOperatorTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeMinusPrefixOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMinusPrefixOperatorTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializePlusPrefixOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePlusPrefixOperatorTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeTildePrefixOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTildePrefixOperatorTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeBaseExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBaseExpressionTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeIndirectionListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIndirectionListTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeIndirectionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIndirectionTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeCastOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCastOperatorTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeDotOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDotOperatorTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeDotMethodOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDotMethodOperatorTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeDotColumnOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDotColumnOperatorTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeMethodExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMethodExpressionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeMethodExpressionArgumentsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMethodExpressionArgumentsTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializeMethodExpressionArgumentListTrampoline(PEGTransformer &transformer,
	                                                             TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMethodExpressionArgumentListTrampoline(PEGTransformer &transformer,
	                                                                                       TransformProcess &process);
	static void InitializeMethodFunctionArgumentsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMethodFunctionArgumentsTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeSliceExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSliceExpressionTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeSliceBoundTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSliceBoundTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeEndSliceBoundTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeEndSliceBoundTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeEndSliceValueTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeEndSliceValueTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeEndSliceMinusTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeEndSliceMinusTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeStepSliceBoundTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeStepSliceBoundTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializePostfixOperatorTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePostfixOperatorTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeSpecialFunctionExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSpecialFunctionExpressionTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializeCoalesceExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCoalesceExpressionTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeUnpackExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUnpackExpressionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeTryExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTryExpressionTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeColumnsExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColumnsExpressionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeExtractExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExtractExpressionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeExtractArgumentsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExtractArgumentsTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeLambdaExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLambdaExpressionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeNullIfExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNullIfExpressionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeNullIfArgumentsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNullIfArgumentsTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializePositionExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePositionExpressionTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializePositionArgumentsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePositionArgumentsTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeRowExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRowExpressionTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeSubstringExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSubstringExpressionTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeSubstringArgumentsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSubstringArgumentsTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeSubstringExpressionListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSubstringExpressionListTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeSubstringParametersTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSubstringParametersTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeSubstringFromForTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSubstringFromForTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeSubstringFromOptionalForTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSubstringFromOptionalForTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializeSubstringForTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSubstringForTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeTrimExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTrimExpressionTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeTrimArgumentsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTrimArgumentsTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeTrimDirectionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTrimDirectionTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeTrimBothTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTrimBothTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeTrimLeadingTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTrimLeadingTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeTrimTrailingTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTrimTrailingTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeTrimSourceTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTrimSourceTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeOverlayExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOverlayExpressionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeOverlayArgumentsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOverlayArgumentsTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeOverlayParametersTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOverlayParametersTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeFromExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFromExpressionTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeForExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeForExpressionTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeOverlayExpressionListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOverlayExpressionListTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeExtractArgumentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExtractArgumentTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeExtractDatePartArgumentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExtractDatePartArgumentTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeExtractIdentifierArgumentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExtractIdentifierArgumentTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializeExtractStringArgumentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExtractStringArgumentTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeExtractDatePartTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExtractDatePartTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeExternalResourceStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExternalResourceStatementTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializeCreateExternalResourceStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateExternalResourceStmtTrampoline(PEGTransformer &transformer,
	                                                                                     TransformProcess &process);
	static void InitializeRegisterExternalResourceStmtTrampoline(PEGTransformer &transformer,
	                                                             TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRegisterExternalResourceStmtTrampoline(PEGTransformer &transformer,
	                                                                                       TransformProcess &process);
	static void InitializeDestroyExternalResourceStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDestroyExternalResourceStmtTrampoline(PEGTransformer &transformer,
	                                                                                      TransformProcess &process);
	static void InitializeShowExternalResourcesStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeShowExternalResourcesStmtTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializeShowAllModifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeShowAllModifierTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeExternalResourceCreationOptionsTrampoline(PEGTransformer &transformer,
	                                                                TransformProcess &process);
	static unique_ptr<TransformResultValue>
	FinalizeExternalResourceCreationOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static void InitializeInsertStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertStatementTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeOrActionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOrActionTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeInsertOrReplaceTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertOrReplaceTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeInsertOrIgnoreTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertOrIgnoreTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeByNameOrPositionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeByNameOrPositionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeInsertByNameOrderTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertByNameOrderTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeInsertByPositionOrderTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertByPositionOrderTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeInsertByNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertByNameTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeInsertByPositionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertByPositionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeInsertTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertTargetTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeInsertAliasTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertAliasTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeColumnListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColumnListTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeInsertColumnListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertColumnListTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeInsertValuesTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertValuesTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeSelectInsertValuesTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSelectInsertValuesTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeDefaultValuesTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDefaultValuesTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeOnConflictClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOnConflictClauseTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeOnConflictTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOnConflictTargetTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeOnConflictExpressionTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOnConflictExpressionTargetTrampoline(PEGTransformer &transformer,
	                                                                                     TransformProcess &process);
	static void InitializeOnConflictIndexTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOnConflictIndexTargetTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeOnConflictActionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOnConflictActionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeOnConflictUpdateTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOnConflictUpdateTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeOnConflictNothingTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOnConflictNothingTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeReturningClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReturningClauseTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeLoadStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLoadStatementTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeExtensionAliasTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExtensionAliasTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeInstallStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInstallStatementTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeInstallAndLoadTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInstallAndLoadTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeUpdateExtensionsStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateExtensionsStatementTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializeFromSourceTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFromSourceTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeFromSourceIdentifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFromSourceIdentifierTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeFromSourceStringTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFromSourceStringTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeVersionNumberTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeVersionNumberTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeExtensionRepositoryStatementTrampoline(PEGTransformer &transformer,
	                                                             TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExtensionRepositoryStatementTrampoline(PEGTransformer &transformer,
	                                                                                       TransformProcess &process);
	static void InitializeCreateExtensionRepositoryStmtTrampoline(PEGTransformer &transformer,
	                                                              TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCreateExtensionRepositoryStmtTrampoline(PEGTransformer &transformer,
	                                                                                        TransformProcess &process);
	static void InitializeRepositoryPrefixTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRepositoryPrefixTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeRepositoryPublicKeyTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRepositoryPublicKeyTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeDropExtensionRepositoryStmtTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDropExtensionRepositoryStmtTrampoline(PEGTransformer &transformer,
	                                                                                      TransformProcess &process);
	static void InitializeMergeIntoStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMergeIntoStatementTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeMergeIntoUsingClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMergeIntoUsingClauseTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeMergeMatchTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMergeMatchTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeMatchedClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMatchedClauseTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeMatchedClauseActionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMatchedClauseActionTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeUpdateMatchClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateMatchClauseTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeUpdateMatchInfoTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateMatchInfoTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeUpdateMatchSetActionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateMatchSetActionTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeUpdateByNameOrPositionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateByNameOrPositionTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeDeleteMatchClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDeleteMatchClauseTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeInsertMatchClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertMatchClauseTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeInsertMatchInfoTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertMatchInfoTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeInsertDefaultValuesTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertDefaultValuesTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeInsertByNameOrPositionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertByNameOrPositionTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeInsertValuesListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInsertValuesListTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeDoNothingMatchClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDoNothingMatchClauseTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeErrorMatchClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeErrorMatchClauseTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeUpdateMatchSetClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateMatchSetClauseTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeUpdateMatchSetInfoTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateMatchSetInfoTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeAndExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAndExpressionTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeNotMatchedClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNotMatchedClauseTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeBySourceOrTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBySourceOrTargetTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeBySourceTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBySourceTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeByTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeByTargetTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializePivotOnTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePivotOnTrampoline(PEGTransformer &transformer,
	                                                                  TransformProcess &process);
	static void InitializePivotUsingTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePivotUsingTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializePivotColumnListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePivotColumnListTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializePivotColumnEntryTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePivotColumnEntryTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializePivotColumnExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePivotColumnExpressionTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializePivotColumnSubqueryTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePivotColumnSubqueryTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeIntoNameValuesTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntoNameValuesTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeIncludeOrExcludeNullsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIncludeOrExcludeNullsTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeIncludeNullsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIncludeNullsTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeExcludeNullsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExcludeNullsTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeUnpivotHeaderTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUnpivotHeaderTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeUnpivotHeaderSingleTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUnpivotHeaderSingleTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeUnpivotHeaderListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUnpivotHeaderListTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializePragmaStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePragmaStatementTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializePragmaAssignOrFunctionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePragmaAssignOrFunctionTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializePragmaAssignTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePragmaAssignTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializePragmaFunctionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePragmaFunctionTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializePragmaParametersTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePragmaParametersTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializePrepareStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePrepareStatementTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeTypeListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTypeListTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeSelectStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSelectStatementTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeSelectSetOpChainTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSelectSetOpChainTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeSelectSetOpChainTailTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSelectSetOpChainTailTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeIntersectChainTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntersectChainTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeIntersectChainTailTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeIntersectChainTailTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeSetIntersectClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetIntersectClauseTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeSelectAtomTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSelectAtomTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeSelectParensTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSelectParensTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeSetopClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetopClauseTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeSetopTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetopTypeTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeSetopUnionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetopUnionTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeSetopExceptTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetopExceptTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeSelectStatementTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSelectStatementTypeTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeResultModifiersTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeResultModifiersTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeLimitOffsetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLimitOffsetTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeLimitOffsetClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLimitOffsetClauseTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeOffsetLimitClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOffsetLimitClauseTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeOffsetFetchClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOffsetFetchClauseTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeFetchOnlyClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFetchOnlyClauseTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeTableStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableStatementTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeOptionalParensSimpleSelectTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOptionalParensSimpleSelectTrampoline(PEGTransformer &transformer,
	                                                                                     TransformProcess &process);
	static void InitializeSimpleSelectParensTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSimpleSelectParensTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeSelectFromTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSelectFromTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeSelectFromClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSelectFromClauseTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeFromSelectClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFromSelectClauseTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeWithStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWithStatementTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeCTEBodyTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCTEBodyTrampoline(PEGTransformer &transformer,
	                                                                  TransformProcess &process);
	static void InitializeCTESelectBodyTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCTESelectBodyTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeCTEDMLBodyTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCTEDMLBodyTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeUsingKeyTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUsingKeyTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeMaterializedTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeMaterializedTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeSelectClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSelectClauseTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeTargetListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTargetListTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeColumnAliasesTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColumnAliasesTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeDistinctClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDistinctClauseTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeDistinctAllTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDistinctAllTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeDistinctOnTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDistinctOnTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeDistinctOnTargetsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDistinctOnTargetsTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeInnerTableRefTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInnerTableRefTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeTableSubqueryTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableSubqueryTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeBaseTableRefTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBaseTableRefTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeTableAliasColonTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableAliasColonTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeValuesRefTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeValuesRefTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeParensTableRefTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeParensTableRefTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeJoinOrPivotTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeJoinOrPivotTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeTablePivotClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTablePivotClauseTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeTablePivotClauseBodyTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTablePivotClauseBodyTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializePivotGroupByListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePivotGroupByListTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeTableUnpivotClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableUnpivotClauseTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeTableUnpivotClauseBodyTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableUnpivotClauseBodyTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializePivotHeaderTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePivotHeaderTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializePivotValueListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePivotValueListTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializePivotValueTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePivotValueTargetTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializePivotEnumTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePivotEnumTargetTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializePivotListTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePivotListTargetTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeUnpivotValueListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUnpivotValueListTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializePivotTargetListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePivotTargetListTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeUnpivotTargetListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUnpivotTargetListTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeLateralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLateralTrampoline(PEGTransformer &transformer,
	                                                                  TransformProcess &process);
	static void InitializeBaseTableNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBaseTableNameTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeUnqualifiedBaseTableNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUnqualifiedBaseTableNameTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializeQualifiedTableNameTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeQualifiedTableNameTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeSchemaReservedTableTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSchemaReservedTableTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeCatalogReservedSchemaTableTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCatalogReservedSchemaTableTrampoline(PEGTransformer &transformer,
	                                                                                     TransformProcess &process);
	static void InitializeTableFunctionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableFunctionTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeTableFunctionLateralOptTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableFunctionLateralOptTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeTableFunctionAliasColonTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableFunctionAliasColonTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeWithOrdinalityTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWithOrdinalityTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeQualifiedTableFunctionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeQualifiedTableFunctionTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeTableFunctionArgumentsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableFunctionArgumentsTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeFunctionArgumentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFunctionArgumentTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeNamedFunctionArgumentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNamedFunctionArgumentTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializePositionalFunctionArgumentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePositionalFunctionArgumentTrampoline(PEGTransformer &transformer,
	                                                                                     TransformProcess &process);
	static void InitializeNamedParameterTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNamedParameterTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeTableAliasTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableAliasTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeTableAliasAsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableAliasAsTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeTableAliasWithoutAsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTableAliasWithoutAsTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeAtClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAtClauseTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeAtSpecifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAtSpecifierTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeAtUnitTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAtUnitTrampoline(PEGTransformer &transformer,
	                                                                 TransformProcess &process);
	static void InitializeVersionAtUnitTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeVersionAtUnitTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeTimestampAtUnitTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTimestampAtUnitTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeJoinClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeJoinClauseTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeNearestJoinClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNearestJoinClauseTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeNearestJoinAliasedTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNearestJoinAliasedTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeNearestJoinBareTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNearestJoinBareTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeNearestBareTableRefTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNearestBareTableRefTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeNearestValuesRefTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNearestValuesRefTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeNearestTableFunctionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNearestTableFunctionTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeNearestTableSubqueryTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNearestTableSubqueryTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeNearestBaseTableRefTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNearestBaseTableRefTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeNearestParensTableRefTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNearestParensTableRefTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeApproxOrExactTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeApproxOrExactTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeNearestApproxTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNearestApproxTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeNearestExactTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNearestExactTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeDistanceOrSimilarityTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDistanceOrSimilarityTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeNearestDistanceTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNearestDistanceTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeNearestSimilarityTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNearestSimilarityTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeRegularJoinClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRegularJoinClauseTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeJoinByClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeJoinByClauseTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeAsofTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAsofTrampoline(PEGTransformer &transformer,
	                                                               TransformProcess &process);
	static void InitializeJoinWithoutOnClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeJoinWithoutOnClauseTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeJoinQualifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeJoinQualifierTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeOnClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOnClauseTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeUsingClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUsingClauseTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeJoinTypeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeJoinTypeTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeJoinPrefixTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeJoinPrefixTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeCrossJoinPrefixTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCrossJoinPrefixTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeNaturalJoinPrefixTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNaturalJoinPrefixTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializePositionalJoinPrefixTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizePositionalJoinPrefixTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeFullJoinTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFullJoinTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeLeftJoinTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLeftJoinTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeRightJoinTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRightJoinTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeSemiJoinTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSemiJoinTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeAntiJoinTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAntiJoinTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeInnerJoinTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeInnerJoinTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeFromClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFromClauseTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeWhereClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWhereClauseTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeGroupByClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGroupByClauseTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeHavingClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeHavingClauseTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeQualifyClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeQualifyClauseTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeSampleClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSampleClauseTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeWindowClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeWindowClauseTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeSampleEntryTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSampleEntryTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeSampleEntryCountTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSampleEntryCountTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeSampleEntryFunctionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSampleEntryFunctionTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeSampleFunctionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSampleFunctionTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeSamplePropertiesTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSamplePropertiesTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeRepeatableSampleTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRepeatableSampleTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeSampleSeedTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSampleSeedTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeSampleCountTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSampleCountTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeSampleValueTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSampleValueTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeSampleUnitTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSampleUnitTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeSamplePercentageTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSamplePercentageTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeSampleRowsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSampleRowsTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeGroupByExpressionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGroupByExpressionsTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeGroupByAllTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGroupByAllTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeGroupByListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGroupByListTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeGroupByExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGroupByExpressionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeGroupByBaseExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGroupByBaseExpressionTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeEmptyGroupingItemTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeEmptyGroupingItemTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeCubeOrRollupClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCubeOrRollupClauseTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeCubeOrRollupTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCubeOrRollupTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeCubeKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCubeKeywordTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeRollupKeywordTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRollupKeywordTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeGroupingSetsClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGroupingSetsClauseTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeSubqueryReferenceTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSubqueryReferenceTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeOrderByExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOrderByExpressionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeDescOrAscTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDescOrAscTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeDescendingOrderTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDescendingOrderTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeAscendingOrderTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAscendingOrderTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeNullsFirstOrLastTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNullsFirstOrLastTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeNullsFirstTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNullsFirstTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeNullsLastTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNullsLastTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeOrderByClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOrderByClauseTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeOrderByExpressionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOrderByExpressionsTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeOrderByExpressionListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOrderByExpressionListTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeOrderByAllTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOrderByAllTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeLimitClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLimitClauseTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeOffsetClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOffsetClauseTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeOffsetValueTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOffsetValueTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeLimitValueTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLimitValueTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeLimitAllTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLimitAllTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeLimitLiteralPercentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLimitLiteralPercentTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeLimitExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLimitExpressionTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeFetchClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFetchClauseTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeFetchValueTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeFetchValueTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeAliasedExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeAliasedExpressionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeColIdExpressionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeColIdExpressionTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeExpressionAsCollabelTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExpressionAsCollabelTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeExpressionOptIdentifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeExpressionOptIdentifierTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeValuesClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeValuesClauseTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeValuesExpressionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeValuesExpressionsTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeSetStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetStatementTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeSetAssignmentOrTimeZoneTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetAssignmentOrTimeZoneTrampoline(PEGTransformer &transformer,
	                                                                                  TransformProcess &process);
	static void InitializeResetStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeResetStatementTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeSetSchemaTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetSchemaTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeStandardAssignmentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeStandardAssignmentTrampoline(PEGTransformer &transformer,
	                                                                             TransformProcess &process);
	static void InitializeSetVariableOrSettingTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetVariableOrSettingTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeSetTimeZoneTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetTimeZoneTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeZoneValueTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeZoneValueTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeZoneLocalTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeZoneLocalTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeZoneDefaultTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeZoneDefaultTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeZoneStringLiteralTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeZoneStringLiteralTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeZoneIdentifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeZoneIdentifierTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeZoneIntervalWithIntervalTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeZoneIntervalWithIntervalTrampoline(PEGTransformer &transformer,
	                                                                                   TransformProcess &process);
	static void InitializeZoneIntervalWithPrecisionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeZoneIntervalWithPrecisionTrampoline(PEGTransformer &transformer,
	                                                                                    TransformProcess &process);
	static void InitializeSetSettingTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetSettingTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeSetVariableTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetVariableTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeVariableScopeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeVariableScopeTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeSettingScopeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSettingScopeTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeLocalScopeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeLocalScopeTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeSessionScopeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSessionScopeTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeGlobalScopeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeGlobalScopeTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeSetAssignmentTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSetAssignmentTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeVariableListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeVariableListTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeTransactionStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeTransactionStatementTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeBeginTransactionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBeginTransactionTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeRollbackTransactionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeRollbackTransactionTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeCommitTransactionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCommitTransactionTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeReadOrWriteTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReadOrWriteTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeReadOnlyOrReadWriteTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReadOnlyOrReadWriteTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeReadOnlyTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReadOnlyTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	static void InitializeReadWriteTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeReadWriteTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeUpdateStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateStatementTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeUpdateTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateTargetTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeBaseTableSetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBaseTableSetTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeBaseTableAliasSetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeBaseTableAliasSetTrampoline(PEGTransformer &transformer,
	                                                                            TransformProcess &process);
	static void InitializeUpdateAliasTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateAliasTrampoline(PEGTransformer &transformer,
	                                                                      TransformProcess &process);
	static void InitializeUpdateSetClauseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateSetClauseTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeUpdateSetTupleTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateSetTupleTrampoline(PEGTransformer &transformer,
	                                                                         TransformProcess &process);
	static void InitializeUpdateSetElementListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateSetElementListTrampoline(PEGTransformer &transformer,
	                                                                               TransformProcess &process);
	static void InitializeUpdateSetElementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateSetElementTrampoline(PEGTransformer &transformer,
	                                                                           TransformProcess &process);
	static void InitializeUpdateSetColumnTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUpdateSetColumnTargetTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeUseStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUseStatementTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeUseTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUseTargetTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeSchemaNameAsUseTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeSchemaNameAsUseTargetTrampoline(PEGTransformer &transformer,
	                                                                                TransformProcess &process);
	static void InitializeCatalogNameAsUseTargetTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeCatalogNameAsUseTargetTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeUseTargetCatalogSchemaTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeUseTargetCatalogSchemaTrampoline(PEGTransformer &transformer,
	                                                                                 TransformProcess &process);
	static void InitializeDotIdentifierTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeDotIdentifierTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeVacuumStatementTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeVacuumStatementTrampoline(PEGTransformer &transformer,
	                                                                          TransformProcess &process);
	static void InitializeVacuumOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeVacuumOptionsTrampoline(PEGTransformer &transformer,
	                                                                        TransformProcess &process);
	static void InitializeVacuumParensOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeVacuumParensOptionsTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeVacuumLegacyOptionsTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeVacuumLegacyOptionsTrampoline(PEGTransformer &transformer,
	                                                                              TransformProcess &process);
	static void InitializeVacuumOptionTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeVacuumOptionTrampoline(PEGTransformer &transformer,
	                                                                       TransformProcess &process);
	static void InitializeOptAnalyzeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOptAnalyzeTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeOptFullTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOptFullTrampoline(PEGTransformer &transformer,
	                                                                  TransformProcess &process);
	static void InitializeOptFreezeTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOptFreezeTrampoline(PEGTransformer &transformer,
	                                                                    TransformProcess &process);
	static void InitializeOptVerboseTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeOptVerboseTrampoline(PEGTransformer &transformer,
	                                                                     TransformProcess &process);
	static void InitializeNameListTrampoline(PEGTransformer &transformer, TransformProcess &process);
	static unique_ptr<TransformResultValue> FinalizeNameListTrampoline(PEGTransformer &transformer,
	                                                                   TransformProcess &process);
	//===--------------------------------------------------------------------===//
	// END GENERATED TRAMPOLINE RULES
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
		if (rule.transform) {
			throw InternalException("Rule %s already exists", rule_name);
		}
		grammar.SetTransform(
		    rule_name,
		    [function](PEGTransformer &transformer, ParseResult &parse_result) -> unique_ptr<TransformResultValue> {
			    auto result_value = function(transformer, parse_result);
			    return make_uniq<TypedTransformResult<decltype(result_value)>>(std::move(result_value));
		    });
	}

	PEGTransformerFactory(const PEGTransformerFactory &) = delete;

	static unique_ptr<SQLStatement> TransformStatement(PEGTransformer &, ParseResult &list);
	static const case_insensitive_map_t<const TransformProcessInfo *> &GeneratedTrampolineOps();
	static optional_ptr<const TransformProcessInfo> TryGetTransformProcessInfo(const ParseResult &parse_result);
	static const TransformProcessInfo &GetTrampolineOps(const ParseResult &parse_result);

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
