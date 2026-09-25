#include "duckdb/execution/operator/persistent/physical_update.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/chunk_layout.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/row_id_deduplicator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/delete_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

PhysicalUpdate::PhysicalUpdate(PhysicalPlan &physical_plan, vector<LogicalType> types, DuckTableEntry &tableref,
                               DataTable &table, vector<PhysicalIndex> columns,
                               vector<unique_ptr<Expression>> expressions,
                               vector<unique_ptr<Expression>> bound_defaults,
                               vector<unique_ptr<BoundConstraint>> bound_constraints, idx_t estimated_cardinality,
                               bool return_chunk, bool capture_old_rows, vector<idx_t> old_row_columns,
                               RowIdHandling row_id_handling)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::UPDATE, std::move(types), estimated_cardinality),
      tableref(tableref), table(table), columns(std::move(columns)), expressions(std::move(expressions)),
      bound_defaults(std::move(bound_defaults)), bound_constraints(std::move(bound_constraints)),
      return_chunk(return_chunk), capture_old_rows(capture_old_rows), old_row_columns(std::move(old_row_columns)),
      row_id_handling(row_id_handling), index_update(false) {
	auto &indexes = table.GetDataTableInfo().get()->GetIndexes();
	auto index_columns = indexes.GetIndexedColumns();

	unordered_set<column_t> update_columns;
	update_columns.reserve(this->columns.size());
	for (const auto col : this->columns) {
		update_columns.insert(col.index);
	}

	for (const auto &col : table.Columns()) {
		if (index_columns.find(col.Physical().index) == index_columns.end()) {
			continue;
		}
		if (update_columns.find(col.Physical().index) == update_columns.end()) {
			continue;
		}
		index_update = true;
		break;
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class UpdateGlobalState : public GlobalSinkState {
public:
	explicit UpdateGlobalState(ClientContext &context, const vector<LogicalType> &return_types,
	                           RowIdHandling row_id_handling)
	    : updated_count(0), return_collection(context, return_types) {
		if (row_id_handling != RowIdHandling::ASSUME_UNIQUE) {
			updated_rows = make_uniq<RowIdDeduplicator>(context, vector<LogicalType> {LogicalType::ROW_TYPE});
		}
	}

	mutex lock;
	atomic<idx_t> updated_count;
	unique_ptr<RowIdDeduplicator> updated_rows;
	ColumnDataCollection return_collection;
};

class UpdateLocalState : public LocalSinkState {
public:
	UpdateLocalState(ClientContext &context, const PhysicalUpdate &op)
	    : default_executor(context, op.bound_defaults), bound_constraints(op.bound_constraints) {
		auto &expressions = op.expressions;
		auto table_types = op.table.GetTypes();
		// Initialize the update chunk.
		auto &allocator = Allocator::Get(context);
		vector<LogicalType> update_types;
		update_types.reserve(expressions.size());
		for (auto &expr : expressions) {
			update_types.push_back(expr->GetReturnType());
		}
		update_chunk.Initialize(allocator, update_types);

		// Initialize the mock and delete chunk.
		mock_chunk.Initialize(allocator, table_types);
		delete_chunk.Initialize(allocator, table_types);

		ChunkLayoutBuilder table_builder;
		table_builder.AddColumns(table_types);
		table_layout = make_uniq<ChunkLayout>(table_builder.Build());

		if (op.return_chunk || op.update_is_del_and_insert) {
			ChunkLayoutBuilder update_builder;
			auto updates = update_builder.AddColumns(update_types);
			vector<idx_t> table_columns(table_types.size(), DConstants::INVALID_INDEX);
			for (idx_t i = 0; i < op.columns.size(); i++) {
				D_ASSERT(table_columns[op.columns[i].index] == DConstants::INVALID_INDEX);
				table_columns[op.columns[i].index] = i;
			}
			vector<ChunkColumn> new_columns;
			for (auto column : table_columns) {
				new_columns.push_back(updates.Column(column));
			}
			new_image_projection =
			    make_uniq<ChunkProjection>(update_builder.Build(), *table_layout, std::move(new_columns));
		}

		if (op.capture_old_rows) {
			ChunkLayoutBuilder return_builder;
			new_image_columns = make_uniq<ChunkColumnGroup>(return_builder.AddColumns(table_types));
			old_image_columns = make_uniq<ChunkColumnGroup>(return_builder.AddColumns(table_types));
			return_layout = make_uniq<ChunkLayout>(return_builder.Build());
			combined_chunk.Initialize(allocator, return_layout->GetTypes());
			old_image.InitializeEmpty(table_types);
		}
	}

	Vector &RowIds(DataChunk &input, const PhysicalUpdate &op) {
		if (!input_layout) {
			auto input_types = input.GetTypes();
			D_ASSERT(!input_types.empty() && input_types.back() == LogicalType::ROW_TYPE);
			ChunkLayoutBuilder builder;
			builder.AddColumns(vector<LogicalType>(input_types.begin(), input_types.end() - 1));
			row_id_column = make_uniq<ChunkColumn>(builder.AddColumn(LogicalType::ROW_TYPE));
			input_layout = make_uniq<ChunkLayout>(builder.Build());
			if (op.capture_old_rows) {
				auto input_columns = input_layout->AllColumns();
				vector<ChunkColumn> old_columns;
				for (auto column : op.old_row_columns) {
					old_columns.push_back(input_columns.Column(column));
				}
				old_image_projection = make_uniq<ChunkProjection>(*input_layout, *table_layout, std::move(old_columns));
			}
		}
		return input_layout->Column(input, *row_id_column);
	}

	void ArrangeNewImage(idx_t count) {
		new_image_projection->Reference(update_chunk, mock_chunk);
		mock_chunk.CheckCardinality(count);
	}

	ChunkColumnView NewImage() {
		return return_layout->Columns(combined_chunk, *new_image_columns);
	}

	ChunkColumnView OldImage() {
		return return_layout->Columns(combined_chunk, *old_image_columns);
	}

	void AppendReturnRows(ColumnDataCollection &collection, DataChunk &input, idx_t count,
	                      optional_ptr<const SelectionVector> sel) {
		old_image_projection->Reference(input, old_image);
		if (sel) {
			old_image.Slice(*sel, count);
		}
		combined_chunk.Reset();
		NewImage().ReferenceFrom(mock_chunk);
		OldImage().ReferenceFrom(old_image);
		combined_chunk.CheckCardinality(count);
		collection.Append(combined_chunk);
	}

private:
	unique_ptr<ChunkLayout> table_layout;
	unique_ptr<ChunkLayout> input_layout;
	unique_ptr<ChunkLayout> return_layout;
	unique_ptr<ChunkColumn> row_id_column;
	unique_ptr<ChunkColumnGroup> new_image_columns;
	unique_ptr<ChunkColumnGroup> old_image_columns;
	unique_ptr<ChunkProjection> new_image_projection;
	unique_ptr<ChunkProjection> old_image_projection;
	DataChunk old_image;

public:
	DataChunk update_chunk;
	DataChunk mock_chunk;
	DataChunk delete_chunk;
	DataChunk combined_chunk;
	ExpressionExecutor default_executor;
	unique_ptr<TableDeleteState> delete_state;
	unique_ptr<TableUpdateState> update_state;
	const vector<unique_ptr<BoundConstraint>> &bound_constraints;

	TableDeleteState &GetDeleteState(DataTable &table, TableCatalogEntry &tableref, ClientContext &context) {
		if (!delete_state) {
			delete_state = table.InitializeDelete(tableref, context, bound_constraints);
		}
		return *delete_state;
	}

	TableUpdateState &GetUpdateState(DataTable &table, TableCatalogEntry &tableref, ClientContext &context) {
		if (!update_state) {
			update_state = table.InitializeUpdate(tableref, context, bound_constraints);
		}
		return *update_state;
	}
};

SinkResultType PhysicalUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &g_state = input.global_state.Cast<UpdateGlobalState>();
	auto &l_state = input.local_state.Cast<UpdateLocalState>();

	chunk.Flatten();
	l_state.default_executor.SetChunk(chunk);

	DataChunk &update_chunk = l_state.update_chunk;
	update_chunk.Reset();

	for (idx_t i = 0; i < expressions.size(); i++) {
		// Default expression, set to the default value of the column.
		if (expressions[i]->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			l_state.default_executor.ExecuteExpression(columns[i].index, update_chunk.data[i]);
			continue;
		}

		D_ASSERT(expressions[i]->GetExpressionType() == ExpressionType::BOUND_REF);
		auto &binding = expressions[i]->Cast<BoundReferenceExpression>();
		update_chunk.data[i].Reference(chunk.data[binding.Index()]);
	}

	auto &row_ids = l_state.RowIds(chunk, *this);
	DataChunk &mock_chunk = l_state.mock_chunk;

	// Regular in-place update.
	if (!update_is_del_and_insert) {
		// Apply the duplicate row-id policy: ASSUME_UNIQUE keeps the lock-free path; KEEP_FIRST (UPDATE ... FROM)
		// deduplicates so each row is updated at most once, keeping RETURNING and transition tables predictable.
		idx_t update_count = update_chunk.size();
		SelectionVector sel;
		Vector update_row_ids(Vector::Ref(row_ids));
		const bool deduplicate = row_id_handling != RowIdHandling::ASSUME_UNIQUE;
		if (deduplicate) {
			sel.Initialize(update_chunk.size());
			lock_guard<mutex> glock(g_state.lock);
			D_ASSERT(g_state.updated_rows);
			update_count = g_state.updated_rows->Register(row_ids, update_chunk.size(), sel);
			if (row_id_handling == RowIdHandling::ERROR && update_count != update_chunk.size()) {
				throw InvalidInputException("UPDATE command cannot update the same row more than once");
			}
			if (update_count != update_chunk.size()) {
				update_chunk.Slice(sel, update_count);
				update_row_ids.Slice(row_ids, sel, update_count);
			}
		}

		if (return_chunk) {
			l_state.ArrangeNewImage(update_count);
		}
		auto &update_state = l_state.GetUpdateState(table, tableref, context.client);
		table.Update(update_state, context.client, tableref, update_row_ids, columns, update_chunk);

		if (return_chunk) {
			lock_guard<mutex> glock(g_state.lock);
			if (capture_old_rows) {
				// When we deduplicated, apply the selection vector to the OLD columns so they line up with NEW.
				l_state.AppendReturnRows(g_state.return_collection, chunk, update_count, deduplicate ? &sel : nullptr);
			} else {
				g_state.return_collection.Append(mock_chunk);
			}
		}
		g_state.updated_count += update_count;
		return SinkResultType::NEED_MORE_INPUT;
	}

	// We update an index or a complex type, so we need to split the UPDATE into DELETE + INSERT.

	// Apply the duplicate row-id policy. This path holds the lock for its whole body (the append is serialized),
	// so deduplication runs under it. ASSUME_UNIQUE (plain UPDATE and MERGE-driven updates) keeps every row;
	// KEEP_FIRST (UPDATE ... FROM) drops repeated row-ids so we never delete+insert the same row twice.
	SelectionVector sel(update_chunk.size());
	lock_guard<mutex> glock(g_state.lock);
	idx_t update_count = update_chunk.size();
	const bool deduplicate = row_id_handling != RowIdHandling::ASSUME_UNIQUE;
	if (deduplicate) {
		D_ASSERT(g_state.updated_rows);
		update_count = g_state.updated_rows->Register(row_ids, update_chunk.size(), sel);
		if (row_id_handling == RowIdHandling::ERROR && update_count != update_chunk.size()) {
			throw InvalidInputException("UPDATE command cannot update the same row more than once");
		}
	}

	// The update chunk now contains exactly those rows that we are deleting.
	Vector del_row_ids(Vector::Ref(row_ids));
	if (deduplicate && update_count != update_chunk.size()) {
		update_chunk.Slice(sel, update_count);
		del_row_ids.Slice(row_ids, sel, update_count);
	}

	auto &delete_chunk = index_update ? l_state.delete_chunk : l_state.mock_chunk;
	delete_chunk.Reset();

	if (index_update) {
		auto &transaction = DuckTransaction::Get(context.client, table.db);
		vector<StorageIndex> column_ids;
		for (idx_t i = 0; i < table.ColumnCount(); i++) {
			column_ids.emplace_back(i);
		};
		// Fetch the previous index keys for exactly the rows we delete. Use the deduplicated row-ids
		// (del_row_ids) so Fetch, Delete, and LocalAppend all operate on the same rows; flatten first because
		// deduplication slices del_row_ids into a dictionary vector, which Fetch cannot read.
		del_row_ids.Flatten();
		auto fetch_state = ColumnFetchState();
		table.Fetch(transaction, delete_chunk, column_ids, del_row_ids, update_count, fetch_state);
	}

	auto &delete_state = l_state.GetDeleteState(table, tableref, context.client);
	table.Delete(delete_state, context.client, tableref, del_row_ids, update_count);

	l_state.ArrangeNewImage(update_count);

	table.LocalAppend(tableref, context.client, mock_chunk, bound_constraints, del_row_ids, delete_chunk);
	if (return_chunk) {
		if (capture_old_rows) {
			// Apply the dedup selection vector to the OLD columns so they line up with the NEW image.
			l_state.AppendReturnRows(g_state.return_collection, chunk, update_count, deduplicate ? &sel : nullptr);
		} else {
			g_state.return_collection.Append(mock_chunk);
		}
	}

	g_state.updated_count += update_count;
	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalUpdate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<UpdateGlobalState>(context, GetTypes(), row_id_handling);
}

unique_ptr<LocalSinkState> PhysicalUpdate::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<UpdateLocalState>(context.client, *this);
}

SinkCombineResultType PhysicalUpdate::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(*this);
	client_profiler.Flush(context.thread.profiler);
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class UpdateSourceState : public GlobalSourceState {
public:
	explicit UpdateSourceState(const PhysicalUpdate &op) : total_rows(1), rows_scanned(0) {
		if (op.return_chunk) {
			D_ASSERT(op.sink_state);
			auto &g = op.sink_state->Cast<UpdateGlobalState>();
			g.return_collection.InitializeScan(scan_state);
			total_rows = g.return_collection.Count();
		}
	}

	ColumnDataScanState scan_state;
	idx_t total_rows;
	atomic<idx_t> rows_scanned;
};

unique_ptr<GlobalSourceState> PhysicalUpdate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<UpdateSourceState>(*this);
}

ProgressData PhysicalUpdate::GetProgress(ClientContext &context, GlobalSourceState &gstate) const {
	auto &state = gstate.Cast<UpdateSourceState>();
	ProgressData progress;
	progress.total = double(MaxValue<idx_t>(state.total_rows, 1));
	progress.done = state.total_rows == 0 ? 1.0 : double(state.rows_scanned.load(std::memory_order_relaxed));
	return progress;
}

SourceResultType PhysicalUpdate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<UpdateSourceState>();
	auto &g = sink_state->Cast<UpdateGlobalState>();
	if (!return_chunk) {
		chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(g.updated_count.load())));
		state.rows_scanned.store(1, std::memory_order_relaxed);
		return SourceResultType::FINISHED;
	}

	g.return_collection.Scan(state.scan_state, chunk);
	state.rows_scanned.fetch_add(chunk.size(), std::memory_order_relaxed);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
