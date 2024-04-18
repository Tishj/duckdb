#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb_python/numpy/numpy_result_conversion.hpp"

namespace duckdb {

NumpyResultConversion::NumpyResultConversion(vector<unique_ptr<NumpyResultConversion>> collections,
                                             const vector<LogicalType> &types) {
	D_ASSERT(py::gil_check());

	// Calculate the size of the resulting arrays
	count = 0;
	for (auto &collection : collections) {
		count += collection->Count();
	}
	capacity = count;

	auto concatenate_func = py::module_::import("numpy").attr("concatenate");

	D_ASSERT(owned_data.empty());
	D_ASSERT(!collections.empty());
	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		// Collect all the arrays of the collections for this column
		py::tuple arrays(collections.size());
		py::tuple masks(collections.size());

		bool requires_mask = false;
		for (idx_t i = 0; i < collections.size(); i++) {
			auto &collection = collections[i];

			// Check if the result array requires a mask
			auto &source = collection->owned_data[col_idx];
			requires_mask = requires_mask || source.requires_mask;

			// Shrink to fit
			source.Resize(source.data->count);

			arrays[i] = *source.data->array;
			masks[i] = *source.mask->array;
		}
		D_ASSERT(!arrays.empty());
		D_ASSERT(arrays.size() == masks.size());
		py::array result_array = concatenate_func(arrays);
		py::array result_mask = concatenate_func(masks);
		auto array_wrapper = make_uniq<RawArrayWrapper>(std::move(result_array), count, types[col_idx]);
		auto mask_wrapper = make_uniq<RawArrayWrapper>(std::move(result_mask), count, types[col_idx]);
		owned_data.emplace_back(std::move(array_wrapper), std::move(mask_wrapper), requires_mask);
	}

	// Delete the input arrays, we don't need them anymore
	for (auto &collection : collections) {
		collection->Reset();
	}
}

NumpyResultConversion::NumpyResultConversion(const vector<LogicalType> &types, idx_t initial_capacity,
                                             const ClientProperties &client_properties, bool pandas)
    : count(0), capacity(0) {
	owned_data.reserve(types.size());
	for (auto &type : types) {
		owned_data.emplace_back(type, client_properties, pandas);
	}
	Resize(initial_capacity);
}

void NumpyResultConversion::Resize(idx_t new_capacity) {
	if (capacity == 0) {
		for (auto &data : owned_data) {
			data.Initialize(new_capacity);
		}
	} else {
		for (auto &data : owned_data) {
			data.Resize(new_capacity);
		}
	}
	capacity = new_capacity;
}

void NumpyResultConversion::SetCategories() {
	for (idx_t col_idx = 0; col_idx < owned_data.size(); col_idx++) {
		auto &type = Type(col_idx);
		if (type.id() == LogicalTypeId::ENUM) {
			// It's an ENUM type, in addition to converting the codes we must convert the categories
			if (categories.find(col_idx) == categories.end()) {
				auto &categories_list = EnumType::GetValuesInsertOrder(type);
				auto categories_size = EnumType::GetSize(type);
				for (idx_t i = 0; i < categories_size; i++) {
					categories[col_idx].append(py::cast(categories_list.GetValue(i).ToString()));
				}
			}
		}
	}
}

void NumpyResultConversion::Append(DataChunk &chunk) {
	if (count + chunk.size() > capacity) {
		Resize(capacity * 2);
	}
	auto chunk_types = chunk.GetTypes();
	auto source_offset = 0;
	auto source_size = chunk.size();
	auto to_append = chunk.size();
	for (idx_t col_idx = 0; col_idx < owned_data.size(); col_idx++) {
		owned_data[col_idx].Append(count, chunk.data[col_idx], source_size, source_offset, to_append);
	}
	count += to_append;
#ifdef DEBUG
	for (auto &data : owned_data) {
		D_ASSERT(data.data->count == count);
		D_ASSERT(data.mask->count == count);
	}
#endif
}

} // namespace duckdb
