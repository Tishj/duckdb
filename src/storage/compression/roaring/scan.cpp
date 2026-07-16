#include "duckdb/storage/compression/roaring/roaring.hpp"

#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"

namespace duckdb {

namespace roaring {

static idx_t GetSegmentValueCount(data_ptr_t segments) {
	idx_t total = 0;
	for (idx_t i = 0; i < COMPRESSED_SEGMENT_COUNT; i++) {
		total += segments[i];
	}
	return total;
}

static uint16_t GetCompressedValue(data_ptr_t segments, data_ptr_t values, idx_t value_index,
                                   bool allow_container_end) {
	idx_t segment_index = 0;
	idx_t values_before_segment = 0;
	while (segment_index < COMPRESSED_SEGMENT_COUNT && value_index >= values_before_segment + segments[segment_index]) {
		values_before_segment += segments[segment_index++];
	}
	if (segment_index > COMPRESSED_SEGMENT_COUNT ||
	    (segment_index == COMPRESSED_SEGMENT_COUNT &&
	     (!allow_container_end || value_index != values_before_segment || values[value_index] != 0))) {
		throw IOException("Corrupted Roaring segment: compressed value has no segment");
	}
	return UnsafeNumericCast<uint16_t>(segment_index * COMPRESSED_SEGMENT_SIZE + values[value_index]);
}

static void ValidateContainerData(const ContainerMetadata &metadata, data_ptr_t data, idx_t container_size) {
	if (metadata.IsUncompressed()) {
		return;
	}
	if (metadata.IsArray()) {
		auto cardinality = metadata.Cardinality();
		if (cardinality >= COMPRESSED_ARRAY_THRESHOLD && GetSegmentValueCount(data) != cardinality) {
			throw IOException("Corrupted Roaring segment: compressed segment counts do not match cardinality");
		}
		uint16_t previous = 0;
		for (idx_t i = 0; i < cardinality; i++) {
			uint16_t value;
			if (cardinality >= COMPRESSED_ARRAY_THRESHOLD) {
				value = GetCompressedValue(data, data + COMPRESSED_SEGMENT_COUNT, i, false);
			} else {
				value = Load<uint16_t>(data + i * sizeof(uint16_t));
			}
			if ((i > 0 && value <= previous) || value >= container_size) {
				throw IOException("Corrupted Roaring segment: array value is out of range or unordered");
			}
			previous = value;
		}
		return;
	}

	auto run_count = metadata.NumberOfRuns();
	uint16_t previous_end = 0;
	if (run_count >= COMPRESSED_RUN_THRESHOLD) {
		auto segment_value_count = GetSegmentValueCount(data);
		if (segment_value_count != run_count * 2 && segment_value_count + 1 != run_count * 2) {
			throw IOException("Corrupted Roaring segment: compressed segment counts do not match run count");
		}
	}
	for (idx_t i = 0; i < run_count; i++) {
		idx_t start;
		idx_t end;
		if (run_count >= COMPRESSED_RUN_THRESHOLD) {
			auto values = data + COMPRESSED_SEGMENT_COUNT;
			start = GetCompressedValue(data, values, i * 2, false);
			end = GetCompressedValue(data, values, i * 2 + 1, i + 1 == run_count);
		} else {
			auto pair = Load<RunContainerRLEPair>(data + i * sizeof(RunContainerRLEPair));
			start = pair.start;
			end = start + 1 + pair.length;
		}
		auto terminal_run_end = i + 1 == run_count && end == container_size + 1;
		if ((i > 0 && start < previous_end) || end <= start || (end > container_size && !terminal_run_end)) {
			throw IOException("Corrupted Roaring segment: run is out of range or overlapping");
		}
		previous_end = UnsafeNumericCast<uint16_t>(end);
	}
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//

ContainerSegmentScan::ContainerSegmentScan(data_ptr_t data)
    : segments(reinterpret_cast<uint8_t *>(data)), index(0), count(0) {
}

// Returns the base of the current segment, forwarding the index if the segment is depleted of values
uint16_t ContainerSegmentScan::operator++(int) {
	while (index < COMPRESSED_SEGMENT_COUNT && count >= segments[index]) {
		count = 0;
		index++;
	}
	count++;

	// index == COMPRESSED_SEGMENT_COUNT is allowed for runs, as the last run could end at ROARING_CONTAINER_SIZE
	D_ASSERT(index <= COMPRESSED_SEGMENT_COUNT);
	if (index < COMPRESSED_SEGMENT_COUNT) {
		D_ASSERT(segments[index] != 0);
	}
	uint16_t base = static_cast<uint16_t>(index) * COMPRESSED_SEGMENT_SIZE;
	return base;
}

//===--------------------------------------------------------------------===//
// ContainerScanState
//===--------------------------------------------------------------------===//

//! RunContainer

RunContainerScanState::RunContainerScanState(idx_t container_index, idx_t container_size, idx_t count,
                                             data_ptr_t data_p)
    : ContainerScanState(container_index, container_size), count(count), data(data_p) {
}

void RunContainerScanState::ScanPartial(ValidityMask &result_mask, idx_t result_offset, idx_t to_scan) {
	// This method assumes that the validity mask starts off as having all bits set for the entries that are being
	// scanned.

	idx_t result_idx = 0;
	if (!run_index) {
		LoadNextRun();
	}
	while (!finished && result_idx < to_scan) {
		// Either we are already inside a run, then 'start_of_run' will be scanned_count
		// or we're skipping values until the run begins
		auto start_of_run =
		    MaxValue<idx_t>(MinValue<idx_t>(run.start, scanned_count + to_scan), scanned_count + result_idx);
		result_idx = start_of_run - scanned_count;

		// How much of the run are we covering?
		idx_t run_end = run.start + 1 + run.length;
		auto run_or_scan_end = MinValue<idx_t>(run_end, scanned_count + to_scan);

		// Process the run
		D_ASSERT(run_or_scan_end >= start_of_run);
		if (run_or_scan_end > start_of_run) {
			idx_t amount = run_or_scan_end - start_of_run;
			idx_t start = result_offset + result_idx;
			idx_t end = start + amount;
			SetInvalidRange(result_mask, start, end);
		}

		result_idx += run_or_scan_end - start_of_run;
		if (scanned_count + result_idx == run_end) {
			// Fully processed the current run
			LoadNextRun();
		}
	}
	scanned_count += to_scan;
}

void RunContainerScanState::Skip(idx_t to_skip) {
	idx_t end = scanned_count + to_skip;
	if (!run_index) {
		LoadNextRun();
	}
	while (scanned_count < end && !finished) {
		idx_t run_end = run.start + 1 + run.length;
		scanned_count = MinValue<idx_t>(run_end, end);
		if (scanned_count == run_end) {
			LoadNextRun();
		}
	}
	// In case run_index has already reached count
	scanned_count = end;
}

void RunContainerScanState::Verify() const {
#ifdef DEBUG
	uint16_t index = 0;
	for (idx_t i = 0; i < count; i++) {
		auto run = reinterpret_cast<RunContainerRLEPair *>(data)[i];
		D_ASSERT(run.start >= index);
		index = run.start + 1 + run.length;
	}
#endif
}

void RunContainerScanState::LoadNextRun() {
	if (run_index >= count) {
		finished = true;
		return;
	}
	run = reinterpret_cast<RunContainerRLEPair *>(data)[run_index];
	run_index++;
}

CompressedRunContainerScanState::CompressedRunContainerScanState(idx_t container_index, idx_t container_size,
                                                                 idx_t count, data_ptr_t segments, data_ptr_t data)
    : RunContainerScanState(container_index, container_size, count, data), segments(segments), segment(segments) {
	D_ASSERT(count >= COMPRESSED_RUN_THRESHOLD);
	//! Used by Verify, have to use it to avoid a compiler warning/error
	(void)this->segments;
}

void CompressedRunContainerScanState::LoadNextRun() {
	if (run_index >= count) {
		finished = true;
		return;
	}
	uint16_t start = segment++;
	start += reinterpret_cast<uint8_t *>(data)[(run_index * 2) + 0];

	uint16_t end = segment++;
	end += reinterpret_cast<uint8_t *>(data)[(run_index * 2) + 1];

	D_ASSERT(end > start);
	run = RunContainerRLEPair {start, static_cast<uint16_t>(end - 1 - start)};
	run_index++;
}

void CompressedRunContainerScanState::Verify() const {
#ifdef DEBUG
	uint16_t index = 0;
	ContainerSegmentScan verify_segment(segments);
	for (idx_t i = 0; i < count; i++) {
		// Get the start index of the run
		uint16_t start = verify_segment++;
		start += reinterpret_cast<uint8_t *>(data)[(i * 2) + 0];

		// Get the end index of the run
		uint16_t end = verify_segment++;
		end += reinterpret_cast<uint8_t *>(data)[(i * 2) + 1];

		D_ASSERT(!i || start >= index);
		D_ASSERT(end > start);
		index = end;
	}
#endif
}

//! BitsetContainer

BitsetContainerScanState::BitsetContainerScanState(idx_t container_index, idx_t count, validity_t *bitset)
    : ContainerScanState(container_index, count), bitset(bitset) {
}

void BitsetContainerScanState::ScanPartial(ValidityMask &result_mask, idx_t result_offset, idx_t to_scan) {
	if (!result_offset && (to_scan % ValidityMask::BITS_PER_VALUE) == 0 &&
	    (scanned_count % ValidityMask::BITS_PER_VALUE) == 0) {
		ValidityUncompressed::AlignedScan(reinterpret_cast<data_ptr_t>(bitset), scanned_count, result_mask, to_scan);
	} else {
		ValidityUncompressed::UnalignedScan(reinterpret_cast<data_ptr_t>(bitset), container_size, scanned_count,
		                                    result_mask, result_offset, to_scan);
	}
	scanned_count += to_scan;
}

void BitsetContainerScanState::Skip(idx_t to_skip) {
	// NO OP: we only need to forward scanned_count
	scanned_count += to_skip;
}

void BitsetContainerScanState::Verify() const {
	// uncompressed, nothing to verify
	return;
}

RoaringScanState::RoaringScanState(ColumnSegment &segment)
    : segment(segment), reader(nullptr, 0, "Roaring segment"), data_reader(nullptr, 0, "Roaring container data") {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	handle = buffer_manager.Pin(segment.GetBlockHandle());
	reader = CompressionSegmentReader(handle, segment, "Roaring segment");

	// Deserialize the container metadata for this segment
	auto metadata_offset = reader.Read<idx_t>();
	if (metadata_offset > reader.Remaining()) {
		throw IOException("Corrupted Roaring segment: metadata offset is out of range");
	}
	data_reader = reader.ReadSubReader(metadata_offset, "Roaring container data");
	auto metadata_reader = reader.SubReader(reader.Position(), reader.Remaining(), "Roaring metadata");

	auto segment_count = segment.count.load();
	auto container_count = segment_count / ROARING_CONTAINER_SIZE;
	if (segment_count % ROARING_CONTAINER_SIZE != 0) {
		container_count++;
	}
	metadata_collection.Deserialize(metadata_reader, container_count);
	ContainerMetadataCollectionScanner scanner(metadata_collection);
	data_start_position.reserve(container_count);
	idx_t position = 0;
	for (idx_t i = 0; i < container_count; i++) {
		auto metadata = scanner.GetNext();
		if ((metadata.IsRun() && (metadata.NumberOfRuns() == 0 || metadata.NumberOfRuns() >= MAX_RUN_IDX)) ||
		    (metadata.IsArray() && metadata.Cardinality() > MAX_ARRAY_IDX)) {
			throw IOException("Corrupted Roaring segment: invalid container cardinality");
		}
		container_metadata.push_back(metadata);
		auto layout = data_reader;
		layout.SetPosition(position);
		if (metadata.IsUncompressed()) {
			layout.Align(8);
		} else if (metadata.IsArray() && metadata.Cardinality() < COMPRESSED_ARRAY_THRESHOLD) {
			layout.Align(sizeof(uint16_t));
		} else if (metadata.IsRun() && metadata.NumberOfRuns() < COMPRESSED_RUN_THRESHOLD) {
			layout.Align(alignof(RunContainerRLEPair));
		}
		position = layout.Position();
		data_start_position.push_back(position);
		auto start_of_container = i * ROARING_CONTAINER_SIZE;
		auto container_size = MinValue<idx_t>(segment_count - start_of_container, ROARING_CONTAINER_SIZE);
		auto data_size = metadata.GetDataSizeInBytes(container_size);
		auto container_data = data_reader.GetSpan(position, data_size);
		ValidateContainerData(metadata, container_data, container_size);
		container_data_size.push_back(data_size);
		position += data_size;
	}
}

idx_t RoaringScanState::SkipVector(const ContainerMetadata &metadata) {
	// NOTE: this doesn't care about smaller containers, since only the last container can be smaller
	return metadata.GetDataSizeInBytes(ROARING_CONTAINER_SIZE);
}

bool RoaringScanState::UseContainerStateCache(idx_t container_index, idx_t internal_offset) {
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

ContainerMetadata RoaringScanState::GetContainerMetadata(idx_t container_index) {
	return container_metadata[container_index];
}

data_ptr_t RoaringScanState::GetStartOfContainerData(idx_t container_index) {
	return data_reader.GetSpan(data_start_position[container_index], container_data_size[container_index]);
}

ContainerScanState &RoaringScanState::LoadContainer(idx_t container_index, idx_t internal_offset) {
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
		D_ASSERT(metadata.IsInverted());
		auto number_of_runs = metadata.NumberOfRuns();
		if (number_of_runs >= COMPRESSED_RUN_THRESHOLD) {
			auto segments = data_ptr;
			data_ptr = segments + COMPRESSED_SEGMENT_COUNT;
			current_container = make_uniq<CompressedRunContainerScanState>(container_index, container_size,
			                                                               number_of_runs, segments, data_ptr);
		} else {
			D_ASSERT(AlignPointer<sizeof(RunContainerRLEPair)>(data_ptr) == data_ptr);
			current_container =
			    make_uniq<RunContainerScanState>(container_index, container_size, number_of_runs, data_ptr);
		}
	} else {
		auto cardinality = metadata.Cardinality();
		if (cardinality >= COMPRESSED_ARRAY_THRESHOLD) {
			auto segments = data_ptr;
			data_ptr = segments + COMPRESSED_SEGMENT_COUNT;
			if (metadata.IsInverted()) {
				current_container = make_uniq<CompressedArrayContainerScanState<NULLS>>(
				    container_index, container_size, cardinality, segments, data_ptr);
			} else {
				current_container = make_uniq<CompressedArrayContainerScanState<NON_NULLS>>(
				    container_index, container_size, cardinality, segments, data_ptr);
			}
		} else {
			D_ASSERT(AlignPointer<sizeof(uint16_t)>(data_ptr) == data_ptr);
			if (metadata.IsInverted()) {
				current_container =
				    make_uniq<ArrayContainerScanState<NULLS>>(container_index, container_size, cardinality, data_ptr);
			} else {
				current_container = make_uniq<ArrayContainerScanState<NON_NULLS>>(container_index, container_size,
				                                                                  cardinality, data_ptr);
			}
		}
	}

	current_container->Verify();

	auto &scan_state = *current_container;
	if (internal_offset) {
		Skip(scan_state, internal_offset);
	}
	return *current_container;
}

void RoaringScanState::ScanInternal(ContainerScanState &scan_state, idx_t to_scan, ValidityMask &result, idx_t offset) {
	scan_state.ScanPartial(result, offset, to_scan);
}

idx_t RoaringScanState::GetContainerIndex(idx_t start_index, idx_t &offset) {
	idx_t container_index = start_index / ROARING_CONTAINER_SIZE;
	offset = start_index % ROARING_CONTAINER_SIZE;
	return container_index;
}

void RoaringScanState::ScanPartial(idx_t start_idx, ValidityMask &result, idx_t offset, idx_t count) {
	idx_t remaining = count;
	idx_t scanned = 0;
	while (remaining) {
		idx_t internal_offset;
		idx_t container_idx = GetContainerIndex(start_idx + scanned, internal_offset);
		auto &scan_state = LoadContainer(container_idx, internal_offset);
		idx_t remaining_in_container = scan_state.container_size - scan_state.scanned_count;
		idx_t to_scan = MinValue<idx_t>(remaining, remaining_in_container);
		ScanInternal(scan_state, to_scan, result, offset + scanned);
		remaining -= to_scan;
		scanned += to_scan;
	}
	D_ASSERT(scanned == count);
}

void RoaringScanState::Skip(ContainerScanState &scan_state, idx_t skip_count) {
	D_ASSERT(scan_state.scanned_count + skip_count <= scan_state.container_size);
	if (scan_state.scanned_count + skip_count == scan_state.container_size) {
		scan_state.scanned_count = scan_state.container_size;
		// This skips all remaining values covered by this container
		return;
	}
	scan_state.Skip(skip_count);
}

} // namespace roaring

} // namespace duckdb
