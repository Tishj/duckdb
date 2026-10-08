#include "duckdb/common/types/chunk_layout.hpp"

namespace duckdb {

ChunkColumn ChunkColumnGroup::Column(idx_t index) const {
	D_ASSERT(index < count);
	return ChunkColumn(layout, offset + index);
}

Vector &ChunkColumnView::Column(idx_t index) const {
	D_ASSERT(index < count);
	return chunk.data[offset + index];
}

optional_ptr<Vector> ChunkColumnView::ContiguousVectors() const {
	return count ? &chunk.data[offset] : nullptr;
}

void ChunkColumnView::ReferenceInto(DataChunk &target) const {
	D_ASSERT(&target != &chunk);
	D_ASSERT(target.ColumnCount() == count);
	target.Reset();
	for (idx_t i = 0; i < count; i++) {
		D_ASSERT(target.data[i].GetType() == Column(i).GetType());
		target.data[i].Reference(Column(i));
	}
	target.SetCardinalityUnsafe(RowCount());
}

void ChunkColumnView::ReferenceFrom(const ChunkColumnView &source) const {
	D_ASSERT(ColumnCount() == source.ColumnCount());
	D_ASSERT(&chunk != &source.chunk);
	for (idx_t i = 0; i < count; i++) {
		D_ASSERT(Column(i).GetType() == source.Column(i).GetType());
		Column(i).Reference(source.Column(i));
	}
}

ChunkColumnView ChunkColumnView::Rebind(DataChunk &target) const {
	D_ASSERT(offset + count <= target.ColumnCount());
#ifdef DEBUG
	for (idx_t i = 0; i < count; i++) {
		D_ASSERT(Column(i).GetType() == target.data[offset + i].GetType());
	}
#endif
	return ChunkColumnView(target, offset, count);
}

ChunkColumnGroup ChunkLayout::AllColumns() const {
	return ChunkColumnGroup(data, 0, data->types.size());
}

idx_t ChunkLayout::GetColumnIndex(const ChunkColumn &column) const {
	D_ASSERT(column.layout == data);
	D_ASSERT(column.index < data->types.size());
	return column.index;
}

Vector &ChunkLayout::Column(DataChunk &chunk, const ChunkColumn &column) const {
	D_ASSERT(column.layout == data);
	D_ASSERT(column.index < data->types.size());
	D_ASSERT(chunk.ColumnCount() == data->types.size());
	D_ASSERT(chunk.data[column.index].GetType() == data->types[column.index]);
	return chunk.data[column.index];
}

ChunkColumnView ChunkLayout::Columns(DataChunk &chunk, const ChunkColumnGroup &group) const {
	D_ASSERT(group.layout == data);
	Verify(chunk);
	return ChunkColumnView(chunk, group.offset, group.count);
}

void ChunkLayout::Verify(const DataChunk &chunk) const {
	D_ASSERT(chunk.ColumnCount() == data->types.size());
#ifdef DEBUG
	for (idx_t i = 0; i < data->types.size(); i++) {
		D_ASSERT(chunk.data[i].GetType() == data->types[i]);
	}
#endif
}

ChunkLayoutBuilder::ChunkLayoutBuilder() : data(make_shared_ptr<ChunkLayoutData>()) {
}

ChunkColumn ChunkLayoutBuilder::AddColumn(const LogicalType &type) {
	return AddColumns({type}).Column(0);
}

ChunkColumnGroup ChunkLayoutBuilder::AddColumns(const vector<LogicalType> &types) {
	if (!data) {
		throw InternalException("Cannot modify a completed chunk layout");
	}
	auto offset = data->types.size();
	data->types.insert(data->types.end(), types.begin(), types.end());
	return ChunkColumnGroup(data, offset, types.size());
}

ChunkLayout ChunkLayoutBuilder::Build() {
	if (!data) {
		throw InternalException("Chunk layout has already been built");
	}
	return ChunkLayout(std::move(data));
}

ChunkLayout ChunkProjection::CreateTargetLayout(const ChunkLayout &source, const vector<ChunkColumn> &columns) {
	ChunkLayoutBuilder builder;
	for (auto &column : columns) {
		if (column.layout != source.data || column.index >= source.GetTypes().size()) {
			throw InternalException("Chunk projection column belongs to a different layout");
		}
		builder.AddColumn(source.GetTypes()[column.index]);
	}
	return builder.Build();
}

ChunkProjection::ChunkProjection(ChunkLayout source, const vector<ChunkColumn> &columns)
    : ChunkProjection(source, CreateTargetLayout(source, columns), columns) {
}

ChunkProjection::ChunkProjection(ChunkLayout source, ChunkLayout target, vector<ChunkColumn> columns_p)
    : source_layout(std::move(source)), target_layout(std::move(target)) {
	if (columns_p.size() != target_layout.GetTypes().size()) {
		throw InternalException("Chunk projection must populate every target column");
	}
	for (idx_t i = 0; i < columns_p.size(); i++) {
		auto &column = columns_p[i];
		if (column.layout != source_layout.data || column.index >= source_layout.GetTypes().size()) {
			throw InternalException("Chunk projection column belongs to a different layout");
		}
		if (source_layout.GetTypes()[column.index] != target_layout.GetTypes()[i]) {
			throw InternalException("Chunk projection column types do not match");
		}
		columns.push_back(column.index);
	}
}

void ChunkProjection::Reference(DataChunk &source, DataChunk &target) const {
	Reference(ChunkColumnView(source, 0, source.ColumnCount()), target);
}

void ChunkProjection::Reference(const ChunkColumnView &source, DataChunk &target) const {
	D_ASSERT(&source.chunk != &target);
	D_ASSERT(source.ColumnCount() == source_layout.GetTypes().size());
#ifdef DEBUG
	for (idx_t i = 0; i < source.ColumnCount(); i++) {
		D_ASSERT(source.Column(i).GetType() == source_layout.GetTypes()[i]);
	}
#endif
	target_layout.Verify(target);
	target.Reset();
	for (idx_t i = 0; i < columns.size(); i++) {
		target.data[i].Reference(source.Column(columns[i]));
	}
	target.SetCardinalityUnsafe(source.RowCount());
}

} // namespace duckdb
