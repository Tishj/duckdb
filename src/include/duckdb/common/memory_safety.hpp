#pragma once

namespace duckdb {

template <bool IS_ENABLED>
struct MemorySafety {
#ifdef DEBUG
	// In DEBUG mode safety is always on
	static constexpr bool ENABLED = true;
#else
	static constexpr bool ENABLED = IS_ENABLED;
#endif
};

template <bool SAFE, class ErrorMessageProvider>
struct MemorySafetyHandler {};

template <typename ErrorMessageProvider>
struct MemorySafetyHandler<true, ErrorMessageProvider> {
	// Safety is enabled, do a nullptr check
	static void OP(const bool null) {
#if defined(DUCKDB_DEBUG_NO_SAFETY) || defined(DUCKDB_CLANG_TIDY)
		return;
#else
		if (DUCKDB_UNLIKELY(null)) {
			throw duckdb::InternalException(ErrorMessageProvider::MESSAGE);
		}
#endif
	}
};

template <typename ErrorMessageProvider>
struct MemorySafetyHandler<false, ErrorMessageProvider> {
	static void OP(const bool null) {
	}
};

} // namespace duckdb
