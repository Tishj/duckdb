#include "duckdb/storage/compression/roaring.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/common/fast_mem.hpp"

namespace duckdb {

namespace roaring {

//! The amount of values that are encoded per container
static constexpr idx_t ROARING_CONTAINER_SIZE = 2048;
static constexpr bool NULLS = true;
static constexpr bool NON_NULLS = false;
static constexpr uint16_t MAX_RUN_IDX = 63;
static constexpr uint16_t MAX_ARRAY_IDX = 127;

struct ContainerMetadata {
public:
	ContainerMetadata() {
	}
	ContainerMetadata(bool is_inverted, bool is_run, uint16_t value) {
		data.run_container.is_inverted = is_inverted;
		if (is_run) {
			data.run_container.is_run = 1;
			data.run_container.number_of_runs = value;
			data.run_container.unused = 0;
		} else {
			data.non_run_container.is_run = 0;
			data.non_run_container.cardinality = value;
			data.non_run_container.unused = 0;
		}
	}

public:
	bool IsRun() const {
		return data.run_container.is_run;
	}

	bool IsUncompressed() const {
		return !data.non_run_container.is_run && data.non_run_container.cardinality == MAX_ARRAY_IDX + 1;
	}

	bool IsInverted() const {
		return data.run_container.is_inverted;
	}

	uint16_t NumberOfRuns() const {
		return data.run_container.number_of_runs;
	}

	uint16_t Cardinality() const {
		return data.non_run_container.cardinality;
	}

public:
	union {
		struct {
			uint16_t unused : 1;          //! Currently unused bits
			uint16_t is_inverted : 1;     //! Indicate if this maps nulls or non-nulls
			uint16_t is_run : 1;          //! Indicate if this is a run container
			uint16_t unused2 : 2;         //! Currently unused bits
			uint16_t number_of_runs : 11; //! The number of runs
		} run_container;

		struct {
			uint16_t unused : 1;       //! Currently unused bits
			uint16_t is_inverted : 1;  //! Indicate if this maps nulls or non-nulls
			uint16_t is_run : 1;       //! Indicate if this is a run container
			uint16_t cardinality : 13; //! How many values are set (4096)
		} non_run_container;
	} data;
};

struct RunContainerRLEPair {
	uint16_t start;
	uint16_t length;
};

enum class ContainerType : uint8_t { RUN_CONTAINER, ARRAY_CONTAINER, BITSET_CONTAINER };

struct ContainerCompressionState {
public:
	struct Result {
	public:
		static Result RunContainer(idx_t runs, bool nulls) {
			auto res = Result();
			res.container_type = ContainerType::RUN_CONTAINER;
			res.nulls = nulls;
			res.count = runs;
			return res;
		}

		static Result ArrayContainer(idx_t array_size, bool nulls) {
			auto res = Result();
			res.container_type = ContainerType::ARRAY_CONTAINER;
			res.nulls = nulls;
			res.count = array_size;
			return res;
		}

		static Result BitsetContainer(idx_t container_size) {
			auto res = Result();
			res.container_type = ContainerType::BITSET_CONTAINER;
			res.nulls = true;
			res.count = container_size;
			return res;
		}

	public:
		ContainerMetadata GetMetadata() const {
			return ContainerMetadata(nulls, container_type == ContainerType::RUN_CONTAINER,
			                         container_type == ContainerType::BITSET_CONTAINER ? MAX_ARRAY_IDX + 1 : count);
		}
		idx_t GetByteSize() const {
			idx_t res = 0;
			res += sizeof(ContainerMetadata);
			switch (container_type) {
			case ContainerType::BITSET_CONTAINER:
				res += AlignValue<idx_t, 8>(count) / 8;
				break;
			case ContainerType::RUN_CONTAINER:
				res += count * sizeof(RunContainerRLEPair);
				break;
			case ContainerType::ARRAY_CONTAINER:
				res += count * sizeof(uint16_t);
				break;
			}
			return res;
		}

	public:
		ContainerType container_type;
		//! Whether nulls are being encoded or non-nulls
		bool nulls;
		//! The amount (meaning depends on container_type)
		idx_t count;

	private:
		Result() {
		}
	};

public:
	ContainerCompressionState() {
		Reset();
	}

public:
	void Append(bool null, idx_t amount = 1) {
		if (uncompressed) {
			if (null) {
				ValidityMask mask(uncompressed);
				for (idx_t i = 0; i < amount; i++) {
					mask.SetInvalidUnsafe(count + i);
				}
			}
			count += amount;
			return;
		}

		// Adjust the runs
		auto &current_run_idx = run_idx[null];
		auto &last_run_idx = run_idx[last_is_null];

		if (count && null != last_is_null && last_run_idx < MAX_RUN_IDX) {
			auto &last_run = runs[last_is_null][last_run_idx];
			// End the last run
			last_run.length = (count - last_run.start) - 1;
			last_run_idx++;
		}
		if (!count || (null != last_is_null && current_run_idx < MAX_RUN_IDX)) {
			auto &current_run = runs[null][current_run_idx];
			// Initialize the new run
			current_run.start = count;
		}

		// Add to the array
		auto &current_array_idx = array_idx[null];
		if (current_array_idx < MAX_ARRAY_IDX) {
			if (current_array_idx + amount <= MAX_ARRAY_IDX) {
				for (idx_t i = 0; i < amount; i++) {
					arrays[null][current_array_idx + i] = count + i;
				}
			}
			current_array_idx += amount;
		}

		last_is_null = null;
		null_count += null * amount;
		count += amount;
	}

	bool IsFull() const {
		return count == ROARING_CONTAINER_SIZE;
	}

	void OverrideArray(data_ptr_t destination, bool nulls) {
		arrays[nulls] = reinterpret_cast<uint16_t *>(destination);
	}
	void OverrideRun(data_ptr_t destination, bool nulls) {
		runs[nulls] = reinterpret_cast<RunContainerRLEPair *>(destination);
	}
	void OverrideUncompressed(data_ptr_t destination) {
		uncompressed = reinterpret_cast<validity_t *>(destination);
	}

	void Finalize() {
		D_ASSERT(!finalized);
		auto &last_run_idx = run_idx[last_is_null];
		if (count && last_run_idx < MAX_RUN_IDX) {
			auto &last_run = runs[last_is_null][last_run_idx];
			// End the last run
			last_run.length = (count - last_run.start);
			last_run_idx++;
		}
		finalized = true;
	}

	Result GetResult() {
		D_ASSERT(finalized);
		const bool can_use_null_array = array_idx[NON_NULLS] < MAX_ARRAY_IDX;
		const bool can_use_non_null_array = array_idx[NULLS] < MAX_ARRAY_IDX;

		const bool can_use_null_run = run_idx[NON_NULLS] < MAX_RUN_IDX;
		const bool can_use_non_null_run = run_idx[NULLS] < MAX_RUN_IDX;

		const bool can_use_array = can_use_null_array || can_use_non_null_array;
		const bool can_use_run = can_use_null_run || can_use_non_null_run;
		if (!can_use_array && !can_use_run) {
			// Can not efficiently encode at all, write it uncompressed
			return Result::BitsetContainer(count);
		}
		uint16_t lowest_array_cost = MinValue<uint16_t>(array_idx[NON_NULLS], array_idx[NULLS]);
		uint16_t lowest_run_cost = MinValue<uint16_t>(run_idx[NON_NULLS], run_idx[NULLS]) * 2;
		uint16_t uncompressed_cost =
		    (AlignValue<uint16_t, ValidityMask::BITS_PER_VALUE>(count) / ValidityMask::BITS_PER_VALUE) *
		    sizeof(validity_t);
		if (MinValue<uint16_t>(lowest_array_cost, lowest_run_cost) > uncompressed_cost) {
			// The amount of values is too small, better off using uncompressed
			// we can detect this at decompression because we know how many values are left
			return Result::BitsetContainer(count);
		}

		if (lowest_array_cost <= lowest_run_cost) {
			if (array_idx[NULLS] <= array_idx[NON_NULLS]) {
				return Result::ArrayContainer(array_idx[NULLS], NULLS);
			} else {
				return Result::ArrayContainer(array_idx[NON_NULLS], NON_NULLS);
			}
		} else {
			if (run_idx[NULLS] <= run_idx[NON_NULLS]) {
				return Result::RunContainer(run_idx[NULLS], NULLS);
			} else {
				return Result::RunContainer(run_idx[NON_NULLS], NON_NULLS);
			}
		}
	}

	void Reset() {
		count = 0;
		null_count = 0;
		run_idx[NON_NULLS] = 0;
		run_idx[NULLS] = 0;
		array_idx[NON_NULLS] = 0;
		array_idx[NULLS] = 0;
		finalized = false;
		last_is_null = false;

		// Reset the arrays + runs
		arrays[NULLS] = base_arrays[NULLS];
		arrays[NON_NULLS] = base_arrays[NON_NULLS];

		runs[NULLS] = base_runs[NULLS];
		runs[NON_NULLS] = base_runs[NON_NULLS];

		uncompressed = nullptr;
	}

public:
	//! Total amount of values covered by the container
	idx_t count = 0;
	//! How many of the total are null
	idx_t null_count = 0;
	bool last_is_null = false;

	RunContainerRLEPair *runs[2];
	uint16_t *arrays[2];

	//! The runs (for sequential nulls | sequential non-nulls)
	RunContainerRLEPair base_runs[2][MAX_RUN_IDX];
	//! The indices (for nulls | non-nulls)
	uint16_t base_arrays[2][MAX_ARRAY_IDX];

	uint16_t run_idx[2];
	uint16_t array_idx[2];

	validity_t *uncompressed = nullptr;
	//! Whether the state has been finalized
	bool finalized = false;
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct RoaringAnalyzeState : public AnalyzeState {
public:
	explicit RoaringAnalyzeState(const CompressionInfo &info) : AnalyzeState(info) {};

public:
	bool HasEnoughSpaceInSegment(idx_t required_space) {
		D_ASSERT(space_used <= info.GetBlockSize());
		idx_t remaining_space = info.GetBlockSize() - space_used;
		if (required_space > remaining_space) {
			return false;
		}
		return true;
	}

	void FlushSegment() {
		if (!current_count) {
			D_ASSERT(!space_used);
			return;
		}
		total_size += space_used;
		space_used = 0;
		current_count = 0;
		segment_count++;
	}

	void FlushContainer() {
		if (!container_state.count) {
			return;
		}
		container_state.Finalize();
		auto res = container_state.GetResult();
		container_metadata.push_back(res.GetMetadata());
		auto required_space = res.GetByteSize();
		if (!HasEnoughSpaceInSegment(required_space)) {
			FlushSegment();
		}
		space_used += required_space;
		current_count += container_state.count;
		container_state.Reset();
	}

	void Analyze(Vector &input, idx_t count) {
		UnifiedVectorFormat unified;
		input.ToUnifiedFormat(count, unified);
		auto &validity = unified.validity;

		if (validity.AllValid()) {
			idx_t appended = 0;
			while (appended < count) {
				idx_t to_append = MinValue<idx_t>(ROARING_CONTAINER_SIZE - container_state.count, count - appended);
				container_state.Append(false, to_append);
				if (container_state.IsFull()) {
					FlushContainer();
				}
				appended += to_append;
			}
		} else {
			idx_t appended = 0;
			while (appended < count) {
				idx_t to_append = MinValue<idx_t>(ROARING_CONTAINER_SIZE - container_state.count, count - appended);
				for (idx_t i = 0; i < to_append; i++) {
					auto is_null = validity.RowIsValidUnsafe(appended + i);
					container_state.Append(!is_null);
				}
				if (container_state.IsFull()) {
					FlushContainer();
				}
				appended += to_append;
			}
		}
		this->count += count;
	}

public:
	ContainerCompressionState container_state;
	//! The space used by the current segment
	idx_t space_used = 0;
	//! The total amount of segments to write
	idx_t segment_count = 0;
	//! The amount of values in the current segment;
	idx_t current_count = 0;
	//! The total amount of data to serialize
	idx_t count = 0;

	//! The total amount of bytes used to compress the whole segment
	idx_t total_size = 0;
	//! The container metadata, determining the type of each container to use during compression
	vector<ContainerMetadata> container_metadata;
};

unique_ptr<AnalyzeState> RoaringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	CompressionInfo info(col_data.GetBlockManager().GetBlockSize());
	auto state = make_uniq<RoaringAnalyzeState>(info);

	return std::move(state);
}

bool RoaringAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &analyze_state = state.Cast<RoaringAnalyzeState>();
	analyze_state.Analyze(input, count);
	return true;
}

idx_t RoaringFinalAnalyze(AnalyzeState &state) {
	auto &roaring_state = state.Cast<RoaringAnalyzeState>();
	roaring_state.FlushContainer();
	roaring_state.FlushSegment();
	return roaring_state.total_size;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct RoaringCompressState : public CompressionState {
public:
	explicit RoaringCompressState(ColumnDataCheckpointer &checkpointer, unique_ptr<AnalyzeState> analyze_state_p)
	    : CompressionState(analyze_state_p->info), owned_analyze_state(std::move(analyze_state_p)),
	      analyze_state(owned_analyze_state->Cast<RoaringAnalyzeState>()),
	      container_state(analyze_state.container_state), container_metadata(analyze_state.container_metadata),
	      checkpointer(checkpointer),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_ROARING)) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);
		count = 0;
		InitializeContainer();
	}

public:
	inline idx_t GetContainerIndex() {
		idx_t index = count / ROARING_CONTAINER_SIZE;
		return index;
	}

	idx_t GetRemainingSpace() {
		return metadata_ptr - data_ptr;
	}

	bool CanStore(idx_t container_size, const ContainerMetadata &metadata) {
		idx_t required_space = sizeof(ContainerMetadata);
		if (metadata.IsUncompressed()) {
			// Account for the alignment we might need for this container
			required_space += (AlignValue<idx_t>((idx_t)data_ptr)) - (idx_t)data_ptr;
			required_space += (container_size / ValidityMask::BITS_PER_VALUE) * sizeof(validity_t);
		} else if (metadata.IsRun()) {
			required_space += sizeof(RunContainerRLEPair) * metadata.NumberOfRuns();
		} else {
			required_space += sizeof(uint16_t) * metadata.Cardinality();
		}

		if (required_space > GetRemainingSpace()) {
			return false;
		}
		return true;
	}

	void InitializeContainer() {
		if (count == analyze_state.count) {
			// No more vectors left
			return;
		}
		auto container_index = GetContainerIndex();
		D_ASSERT(container_index < container_metadata.size());
		auto &metadata = container_metadata[container_index];

		idx_t container_size = AlignValue<idx_t, ValidityMask::BITS_PER_VALUE>(
		    MinValue<idx_t>(analyze_state.count - count, ROARING_CONTAINER_SIZE));
		if (!CanStore(container_size, metadata)) {
			idx_t row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
		}

		metadata_ptr -= sizeof(ContainerMetadata);
		Store<ContainerMetadata>(metadata, metadata_ptr);

		// Override the pointer to write directly into the block
		if (metadata.IsUncompressed()) {
			data_ptr = (data_ptr_t)(AlignValue<idx_t>((idx_t)(data_ptr)));
			FastMemset(data_ptr, (uint32_t)-1, sizeof(validity_t) * (container_size / ValidityMask::BITS_PER_VALUE));
			container_state.OverrideUncompressed(data_ptr);
			data_ptr += (container_size / ValidityMask::BITS_PER_VALUE) * sizeof(validity_t);
		} else if (metadata.IsRun()) {
			container_state.OverrideRun(data_ptr, metadata.IsInverted());
			data_ptr += sizeof(RunContainerRLEPair) * metadata.NumberOfRuns();
		} else {
			container_state.OverrideArray(data_ptr, metadata.IsInverted());
			data_ptr += sizeof(uint16_t) * metadata.Cardinality();
		}
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();

		auto compressed_segment =
		    ColumnSegment::CreateTransientSegment(db, type, row_start, info.GetBlockSize(), info.GetBlockSize());
		compressed_segment->function = function;
		current_segment = std::move(compressed_segment);

		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);
		data_ptr = handle.Ptr();
		data_ptr += sizeof(idx_t);
		metadata_ptr = handle.Ptr() + info.GetBlockSize();
	}

	void FlushSegment() {
		auto &state = checkpointer.GetCheckpointState();
		auto base_ptr = handle.Ptr();
		// +======================================+
		// |x|ddddddddddddddd|                |mmm|
		// +======================================+

		// x: metadata_offset (to the "right" of it)
		// d: data of the containers
		// m: metadata of the containers

		// This is after 'x'
		base_ptr += sizeof(idx_t);

		// Size of the 'd' segment
		idx_t data_size = NumericCast<idx_t>(data_ptr - base_ptr);
		data_size = AlignValue(data_size);
		// Size of the 'm' segment
		idx_t metadata_size = NumericCast<idx_t>(handle.Ptr() + info.GetBlockSize() - metadata_ptr);
		idx_t total_segment_size;

		if (current_segment->count.load() == 0) {
			return;
		}

		auto gap = metadata_ptr - data_ptr;
		double percentage_of_block = (double)gap / ((double)info.GetBlockSize() / 100.0);
		if (percentage_of_block > 0.25) {
			// Move the metadata, to close the gap between the data and the metadata
			std::memmove(base_ptr + data_size, metadata_ptr, metadata_size);
			metadata_ptr = base_ptr + data_size;
			total_segment_size = sizeof(idx_t) + data_size + metadata_size;
		} else {
			total_segment_size = info.GetBlockSize();
		}
		auto metadata_start = (metadata_ptr - base_ptr) + metadata_size;
		Store<idx_t>(metadata_start, handle.Ptr());
		state.FlushSegment(std::move(current_segment), std::move(handle), total_segment_size);
	}

	void Finalize() {
		FlushContainer();
		FlushSegment();
		current_segment.reset();
	}

	void FlushContainer() {
		if (!container_state.count) {
			return;
		}
		count += container_state.count;
		bool has_nulls = container_state.null_count != 0;
		bool has_non_nulls = container_state.null_count != container_state.count;
		if (has_nulls || container_state.uncompressed) {
			current_segment->stats.statistics.SetHasNullFast();
		}
		if (has_non_nulls || container_state.uncompressed) {
			current_segment->stats.statistics.SetHasNoNullFast();
		}
		current_segment->count += container_state.count;
		container_state.Reset();
	}

	void NextContainer() {
		FlushContainer();
		InitializeContainer();
	}

	void Compress(Vector &input, idx_t count) {
		UnifiedVectorFormat unified;
		input.ToUnifiedFormat(count, unified);
		auto &validity = unified.validity;

		if (validity.AllValid()) {
			idx_t appended = 0;
			while (appended < count) {
				idx_t to_append = MinValue<idx_t>(ROARING_CONTAINER_SIZE - container_state.count, count - appended);
				container_state.Append(false, to_append);
				if (container_state.IsFull()) {
					NextContainer();
				}
				appended += to_append;
			}
		} else {
			idx_t appended = 0;
			while (appended < count) {
				idx_t to_append = MinValue<idx_t>(ROARING_CONTAINER_SIZE - container_state.count, count - appended);
				for (idx_t i = 0; i < to_append; i++) {
					auto idx = unified.sel->get_index(appended + i);
					auto is_null = validity.RowIsValidUnsafe(idx);
					container_state.Append(!is_null);
				}
				if (container_state.IsFull()) {
					NextContainer();
				}
				appended += to_append;
			}
		}
	}

public:
	unique_ptr<AnalyzeState> owned_analyze_state;
	RoaringAnalyzeState &analyze_state;

	ContainerCompressionState &container_state;
	vector<ContainerMetadata> &container_metadata;

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;

	// Ptr to next free spot in segment;
	data_ptr_t data_ptr;
	// Ptr to next free spot for storing
	data_ptr_t metadata_ptr;
	//! The amount of values already compressed
	idx_t count = 0;
};

unique_ptr<CompressionState> RoaringInitCompression(ColumnDataCheckpointer &checkpointer,
                                                    unique_ptr<AnalyzeState> state) {
	return make_uniq<RoaringCompressState>(checkpointer, std::move(state));
}

void RoaringCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = state_p.Cast<RoaringCompressState>();
	state.Compress(scan_vector, count);
}

void RoaringFinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<RoaringCompressState>();
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//

struct ContainerScanState {
public:
	ContainerScanState(idx_t container_index_p, idx_t container_size)
	    : container_index(container_index_p), container_size(container_size) {
	}
	virtual ~ContainerScanState() {
	}

public:
	virtual void ScanPartial(Vector &result, idx_t result_offset, idx_t to_scan) = 0;
	virtual void Skip(idx_t count) = 0;
	virtual void Verify() const = 0;

public:
	//! The index of the container
	idx_t container_index;
	//! The size of the container (how many values does it hold)
	idx_t container_size;
	//! How much of the container is already consumed
	idx_t scanned_count = 0;
};

template <bool INVERTED>
struct RunContainerScanState : public ContainerScanState {
public:
	RunContainerScanState(idx_t container_index, idx_t container_size, RunContainerRLEPair *runs, idx_t count)
	    : ContainerScanState(container_index, container_size), runs(runs), count(count) {
	}

public:
	void ScanPartial(Vector &result, idx_t result_offset, idx_t to_scan) override {
		auto &result_mask = FlatVector::Validity(result);

		// This method assumes that the validity mask starts off as having all bits set for the entries that are being
		// scanned.

		if (INVERTED) {
			do {
				if (run_index >= count || runs[run_index].start > scanned_count + to_scan) {
					// The run does not cover these entries, no action required
					break;
				}
				idx_t result_idx = 0;
				while (run_index < count && result_idx < to_scan) {
					auto run = runs[run_index];
					// Either we are already inside a run, then 'valid_pos' will be scanned_count
					// or we're skipping values until the run begins
					auto valid_pos = MaxValue<idx_t>(MinValue<idx_t>(run.start, scanned_count + to_scan),
					                                 scanned_count + result_idx);
					idx_t valid_start = valid_pos - scanned_count;

					result_idx = valid_pos - scanned_count;

					// How much of the run are we covering?
					auto run_end = MinValue<idx_t>(run.start + 1 + run.length, scanned_count + to_scan);

					// Process the run
					// FIXME: optimize this
					for (idx_t i = 0; valid_pos + i < run_end; i++) {
						result_mask.SetInvalid(result_offset + result_idx + i);
					}
					result_idx += run_end - valid_pos;
					if (scanned_count + result_idx == run.start + 1 + run.length) {
						// Fully processed the current run
						run_index++;
					}
				}
			} while (false);
			scanned_count += to_scan;
		} else {
			do {
				if (run_index >= count || runs[run_index].start > scanned_count + to_scan) {
					// The run does not cover these entries
					// set all the bits to 0
					// FIXME: optimize this
					for (idx_t i = 0; i < to_scan; i++) {
						result_mask.SetInvalid(result_offset + i);
					}
					break;
				}
				if (run_index < count && scanned_count >= runs[run_index].start &&
				    runs[run_index].start + runs[run_index].length + 1 >= scanned_count + to_scan) {
					// The current run covers the entire scan range, meaning all these bits are set
					// no action required
					if (runs[run_index].start + runs[run_index].length + 1 == scanned_count + to_scan) {
						run_index++;
					}
					break;
				}

				idx_t i = 0;
				while (i < to_scan) {
					// Determine the next valid position within the scan range, if available
					idx_t valid_pos = (run_index < count) ? runs[run_index].start : scanned_count + to_scan;
					valid_pos = MaxValue<idx_t>(valid_pos, scanned_count);
					idx_t valid_start = MinValue<idx_t>(valid_pos - scanned_count, to_scan);

					if (i < valid_start) {
						// FIXME: optimize this to group the SetInvalid calls
						// These bits are all set to 0
						for (idx_t j = i; j < valid_start; j++) {
							result_mask.SetInvalid(result_offset + j);
						}
						i = valid_start;
					}

					auto end_of_run = run_index < count ? runs[run_index].start + 1 + runs[run_index].length
					                                    : scanned_count + to_scan;
					auto end_of_run_or_scan = MinValue<idx_t>(scanned_count + to_scan, end_of_run);
					i = end_of_run_or_scan - scanned_count;
					if (run_index < count && scanned_count + i == end_of_run) {
						run_index++;
					}
				}
			} while (false);
			scanned_count += to_scan;
		}
	}

	void Skip(idx_t to_skip) override {
		idx_t end = scanned_count + to_skip;
		while (scanned_count < end && run_index < count) {
			idx_t run_end = runs[run_index].start + 1 + runs[run_index].length;
			scanned_count = MinValue<idx_t>(run_end, end);
			if (scanned_count == run_end) {
				run_index++;
			}
		}
		// In case run_index has already reached count
		scanned_count = end;
	}

	void Verify() const override {
#ifdef DEBUG
		idx_t index = 0;
		for (idx_t i = 0; i < count; i++) {
			D_ASSERT(runs[i].start >= index);
			index = runs[i].start + 1 + runs[i].length;
		}
#endif
	}

public:
	RunContainerRLEPair *runs;
	idx_t count;
	idx_t run_index = 0;
};

template <bool INVERTED>
struct ArrayContainerScanState : public ContainerScanState {
public:
	ArrayContainerScanState(idx_t container_index, idx_t container_size, uint16_t *array, idx_t count)
	    : ContainerScanState(container_index, container_size), array(array), count(count) {
	}

public:
	void ScanPartial(Vector &result, idx_t result_offset, idx_t to_scan) override {
		auto &result_mask = FlatVector::Validity(result);

		// This method assumes that the validity mask starts off as having all bits set for the entries that are being
		// scanned.

		if (INVERTED) {
			// We are mapping nulls, not non-nulls
			do {
				if (array_index >= count || scanned_count + to_scan < array[array_index]) {
					// None of the bits we're scanning are 0, no action required
					break;
				}

#if false
				// FIXME: optimize this
				if (array_index + to_scan < count && array[array_index] == scanned_count && array[array_index + to_scan] == scanned_count + to_scan) {
					// All entries are present in the array, can set all bits to 0 directly
					for (idx_t i = 0; i < to_scan; i++) {
						result_mask.SetInvalid(result_offset + i);
					}
					break;
				}
#endif

				// At least one of the entries to scan is set
				for (; array_index < count; array_index++) {
					if (array[array_index] >= scanned_count + to_scan) {
						break;
					}
					if (array[array_index] < scanned_count) {
						continue;
					}
					auto index = array[array_index] - scanned_count;
					result_mask.SetInvalid(result_offset + index);
				}
			} while (false);
			scanned_count += to_scan;
		} else {
			// We are mapping non-nulls
			do {
				if (array_index >= count || scanned_count + to_scan < array[array_index]) {
					// None of the bits we're scanning are set, set everything to 0 directly
					// FIXME: optimize this
					for (idx_t i = 0; i < to_scan; i++) {
						result_mask.SetInvalid(result_offset + i);
					}
					break;
				}

				if (array_index + to_scan < count && array[array_index] == scanned_count &&
				    array[array_index + to_scan] == scanned_count + to_scan) {
					// All bits are set, no action required
					break;
				}

				idx_t i = 0;
				while (i < to_scan) {
					// Determine the next valid position within the scan range, if available
					idx_t valid_pos = (array_index < count) ? array[array_index] : scanned_count + to_scan;
					idx_t valid_start = valid_pos - scanned_count;

					if (i < valid_start) {
						// FIXME: optimize this
						// These bits are all set to 0
						idx_t invalid_end = MinValue<idx_t>(valid_start, to_scan);
						for (idx_t j = i; j < invalid_end; j++) {
							result_mask.SetInvalid(result_offset + j);
						}
						i = invalid_end;
					}

					if (i == valid_start && i < to_scan && array_index < count) {
						// This bit is already set, no action required
						i++;
						array_index++;
					}
				}
			} while (false);
			scanned_count += to_scan;
		}
	}

	void Skip(idx_t to_skip) override {
		idx_t end = scanned_count + to_skip;
		while (array_index < count && array[array_index] < end) {
			array_index++;
		}
		// In case array_index has already reached count
		scanned_count = end;
	}

	void Verify() const override {
#ifdef DEBUG
		idx_t index = 0;
		for (idx_t i = 0; i < count; i++) {
			D_ASSERT(!i || array[i] > index);
			index = array[i];
		}
#endif
	}

public:
	uint16_t *array;
	const idx_t count;
	idx_t array_index = 0;
};

struct BitsetContainerScanState : public ContainerScanState {
public:
	BitsetContainerScanState(idx_t container_index, idx_t count, validity_t *bitset)
	    : ContainerScanState(container_index, count), bitset(bitset) {
	}

public:
	void ScanPartial(Vector &result, idx_t result_offset, idx_t to_scan) override {
		if ((to_scan % ValidityMask::BITS_PER_VALUE) == 0 && (scanned_count % ValidityMask::BITS_PER_VALUE) == 0) {
			ValidityUncompressed::AlignedScan((data_ptr_t)bitset, scanned_count, result, to_scan);
		} else {
			ValidityUncompressed::UnalignedScan((data_ptr_t)bitset, scanned_count, result, result_offset, to_scan);
		}
		scanned_count += to_scan;
	}

	void Skip(idx_t to_skip) override {
		// NO OP: we only need to forward scanned_count
		scanned_count += to_skip;
	}

	void Verify() const override {
		// uncompressed, nothing to verify
		return;
	}

public:
	validity_t *bitset;
};

struct RoaringScanState : public SegmentScanState {
public:
	explicit RoaringScanState(ColumnSegment &segment) : segment(segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);
		auto base_ptr = handle.Ptr() + segment.GetBlockOffset();
		data_ptr = base_ptr + sizeof(idx_t);

		// Deserialize the container metadata for this segment
		auto metadata_offset = Load<idx_t>(base_ptr);
		auto metadata_ptr = data_ptr + metadata_offset - sizeof(ContainerMetadata);

		auto segment_count = segment.count.load();
		auto container_count = segment_count / ROARING_CONTAINER_SIZE;
		if (segment_count % ROARING_CONTAINER_SIZE != 0) {
			container_count++;
		}
		container_metadata.reserve(container_count);
		data_start_position.reserve(container_count);
		idx_t position = 0;
		for (idx_t i = 0; i < container_count; i++) {
			auto metadata = Load<ContainerMetadata>(metadata_ptr);
			container_metadata.push_back(metadata);
			metadata_ptr -= sizeof(ContainerMetadata);
			if (metadata.IsUncompressed()) {
				position = AlignValue<idx_t>(position);
			}
			data_start_position.push_back(position);
			position += SkipVector(metadata);
		}
	}

public:
	idx_t SkipVector(const ContainerMetadata &metadata) {
		if (metadata.IsRun()) {
			return sizeof(RunContainerRLEPair) * metadata.NumberOfRuns();
		}
		if (metadata.IsUncompressed()) {
			// NOTE: this doesn't care about smaller containers, since only the last container can be smaller
			return (ROARING_CONTAINER_SIZE / ValidityMask::BITS_PER_VALUE) * sizeof(validity_t);
		}
		return sizeof(uint16_t) * metadata.Cardinality();
	}

	bool UseContainerStateCache(idx_t container_index, idx_t internal_offset) {
		if (!current_container) {
			// No container loaded yet
			return false;
		}
		if (current_container->container_index != container_index) {
			// Not the same container
			return false;
		}
		if (current_container->scanned_count != internal_offset) {
			// Not the same scan offset
			return false;
		}
		return true;
	}

	ContainerMetadata GetContainerMetadata(idx_t container_index) {
		return container_metadata[container_index];
	}

	data_ptr_t GetStartOfContainerData(idx_t container_index) {
		return data_ptr + data_start_position[container_index];
	}

	ContainerScanState &LoadContainer(idx_t container_index, idx_t internal_offset) {
		if (UseContainerStateCache(container_index, internal_offset)) {
			return *current_container;
		}
		auto metadata = GetContainerMetadata(container_index);
		auto data_ptr = GetStartOfContainerData(container_index);

		auto segment_count = segment.count.load();
		auto start_of_container = container_index * ROARING_CONTAINER_SIZE;
		auto container_size = MinValue<idx_t>(segment_count - start_of_container, ROARING_CONTAINER_SIZE);
		if (metadata.IsUncompressed()) {
			current_container = make_uniq<BitsetContainerScanState>(container_index, container_size,
			                                                        reinterpret_cast<validity_t *>(data_ptr));
		} else if (metadata.IsRun()) {
			if (metadata.IsInverted()) {
				current_container = make_uniq<RunContainerScanState<NULLS>>(
				    container_index, container_size, reinterpret_cast<RunContainerRLEPair *>(data_ptr),
				    metadata.NumberOfRuns());
			} else {
				current_container = make_uniq<RunContainerScanState<NON_NULLS>>(
				    container_index, container_size, reinterpret_cast<RunContainerRLEPair *>(data_ptr),
				    metadata.NumberOfRuns());
			}
		} else {
			if (metadata.IsInverted()) {
				current_container = make_uniq<ArrayContainerScanState<NULLS>>(
				    container_index, container_size, reinterpret_cast<uint16_t *>(data_ptr), metadata.Cardinality());
			} else {
				current_container = make_uniq<ArrayContainerScanState<NON_NULLS>>(
				    container_index, container_size, reinterpret_cast<uint16_t *>(data_ptr), metadata.Cardinality());
			}
		}

		current_container->Verify();

		auto &scan_state = *current_container;
		if (internal_offset) {
			Skip(scan_state, internal_offset);
		}
		return *current_container;
	}

	void ScanInternal(ContainerScanState &scan_state, idx_t to_scan, Vector &result, idx_t offset) {
		scan_state.ScanPartial(result, offset, to_scan);
	}

	idx_t GetContainerIndex(idx_t start_index, idx_t &offset) {
		idx_t container_index = start_index / ROARING_CONTAINER_SIZE;
		offset = start_index % ROARING_CONTAINER_SIZE;
		return container_index;
	}

	void ScanPartial(idx_t start_idx, Vector &result, idx_t offset, idx_t count) {
		result.Flatten(count);
		idx_t remaining = count;
		idx_t scanned = 0;
		while (remaining) {
			idx_t internal_offset;
			idx_t vector_idx = GetContainerIndex(start_idx + scanned, internal_offset);
			auto &scan_state = LoadContainer(vector_idx, internal_offset);
			idx_t remaining_in_container = scan_state.container_size - scan_state.scanned_count;
			idx_t to_scan = MinValue<idx_t>(remaining, remaining_in_container);
			ScanInternal(scan_state, to_scan, result, offset + scanned);
			remaining -= to_scan;
			scanned += to_scan;
		}
		D_ASSERT(scanned == count);
	}

	void Skip(ContainerScanState &scan_state, idx_t skip_count) {
		D_ASSERT(scan_state.scanned_count + skip_count <= scan_state.container_size);
		if (scan_state.scanned_count + skip_count == scan_state.container_size) {
			scan_state.scanned_count = scan_state.container_size;
			// This skips all remaining values covered by this container
			return;
		}
		scan_state.Skip(skip_count);
	}

public:
	BufferHandle handle;
	ColumnSegment &segment;
	unique_ptr<ContainerScanState> current_container;
	data_ptr_t data_ptr;
	vector<ContainerMetadata> container_metadata;
	vector<idx_t> data_start_position;
};

unique_ptr<SegmentScanState> RoaringInitScan(ColumnSegment &segment) {
	auto result = make_uniq<RoaringScanState>(segment);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void RoaringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                        idx_t result_offset) {
	auto &scan_state = state.scan_state->Cast<RoaringScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);

	scan_state.ScanPartial(start, result, result_offset, scan_count);
}

void RoaringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	RoaringScanPartial(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void RoaringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	RoaringScanState scan_state(segment);

	idx_t internal_offset;
	idx_t vector_idx = scan_state.GetContainerIndex(row_id, internal_offset);
	auto &container_state = scan_state.LoadContainer(vector_idx, internal_offset);

	scan_state.ScanInternal(container_state, 1, result, result_idx);
}

void RoaringSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	// NO OP
	// We skip inside scan instead, if the container boundary gets crossed we can avoid a bunch of work anyways
	return;
}

} // namespace roaring

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction GetCompressionFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_ROARING, data_type, roaring::RoaringInitAnalyze,
	                           roaring::RoaringAnalyze, roaring::RoaringFinalAnalyze, roaring::RoaringInitCompression,
	                           roaring::RoaringCompress, roaring::RoaringFinalizeCompress, roaring::RoaringInitScan,
	                           roaring::RoaringScan, roaring::RoaringScanPartial, roaring::RoaringFetchRow,
	                           roaring::RoaringSkip);
}

CompressionFunction RoaringCompressionFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return GetCompressionFunction(type);
	default:
		throw InternalException("Unsupported type for Roaring");
	}
}

bool RoaringCompressionFun::TypeIsSupported(const PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::BIT:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb