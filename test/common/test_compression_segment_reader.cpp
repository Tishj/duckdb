#include "catch.hpp"
#include "duckdb/storage/compression/compression_segment_reader.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("CompressionSegmentReader checks every byte range", "[compression][storage]") {
	alignas(uint64_t) data_t data[16];
	for (idx_t i = 0; i < sizeof(data); i++) {
		data[i] = UnsafeNumericCast<data_t>(i);
	}

	CompressionSegmentReader reader(data, sizeof(data), "test segment");
	CHECK(reader.Read<uint16_t>() == 0x0100);
	CHECK(reader.Position() == sizeof(uint16_t));
	CHECK(reader.ReadAt<uint16_t>(14) == 0x0f0e);
	CHECK(reader.GetSpan(16, 0) == data + 16);
	CHECK_THROWS_AS(reader.GetSpan(16, 1), IOException);
	CHECK_THROWS_AS(reader.GetSpan(NumericLimits<idx_t>::Maximum(), 2), IOException);

	reader.SetPosition(sizeof(data));
	CHECK(reader.ReadBackward<uint32_t>() == 0x0f0e0d0c);
	CHECK(reader.Position() == 12);
	CHECK(reader.ReadBackwardSpan(12) == data);
	CHECK_THROWS_AS(reader.ReadBackward<uint8_t>(), IOException);
}

TEST_CASE("CompressionSegmentReader checks positioning and slices", "[compression][storage]") {
	alignas(uint64_t) data_t data[32] = {};
	CompressionSegmentReader reader(data, sizeof(data), "test segment");

	reader.Skip(3);
	reader.Align(8);
	CHECK(reader.Position() == 8);
	reader.Rewind(3);
	CHECK(reader.Position() == 5);
	CHECK_THROWS_AS(reader.Rewind(6), IOException);
	CHECK_THROWS_AS(reader.SetPosition(33), IOException);
	CHECK_THROWS_AS(reader.Align(0), IOException);

	auto slice = reader.SubReader(8, 8, "test slice");
	CHECK(slice.Size() == 8);
	CHECK(slice.GetSpan(8, 0) == data + 16);
	CHECK_THROWS_AS(slice.GetSpan(8, 1), IOException);
	CHECK_THROWS_AS(reader.SubReader(24, 9, "escaping slice"), IOException);
}

TEST_CASE("CompressionSegmentReader checks typed array sizes", "[compression][storage]") {
	alignas(uint64_t) data_t data[32] = {};
	CompressionSegmentReader reader(data, sizeof(data), "test segment");

	CHECK(reader.ReadArray<uint64_t>(4) == reinterpret_cast<const uint64_t *>(data));
	CHECK(reader.Finished());
	reader.SetPosition(0);
	CHECK_THROWS_AS(reader.ReadArray<uint64_t>(5), IOException);
	CHECK_THROWS_AS(reader.GetArray<uint64_t>(0, NumericLimits<idx_t>::Maximum()), IOException);
	CHECK_THROWS_AS(reader.GetArray<uint32_t>(1, 1), IOException);
}
