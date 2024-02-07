//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/thread.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"

#include <thread>
#include <utility>

namespace duckdb {

class thread {
private:
	std::thread internal;
#ifdef DUCKDB_DEBUG_THREADS
	static std::atomic<int> thread_count;
#endif

	void IncrementIfJoinable() {
#ifdef DUCKDB_DEBUG_THREADS
		if (internal.joinable()) {
			++thread_count;
			Printer::Print("thread_count " + std::to_string(thread_count.load()));
		}
#endif
	}

public:
	template <class... Args>
	explicit thread(Args &&...args) : internal(std::forward<Args>(args)...) {
		IncrementIfJoinable();
	}

	~thread() {
#ifdef DUCKDB_DEBUG_THREADS
		if (internal.joinable()) {
			--thread_count;
		}
#endif
	}

	// Move constructor
	thread(thread &&other) noexcept : internal(std::move(other.internal)) {
	}

	// Move assignment operator
	thread &operator=(thread &&other) noexcept {
		if (this != &other) {
			internal = std::move(other.internal);
		}
		return *this;
	}

	// Member function wrappers
	template <class Function, class... Args>
	explicit thread(Function &&f, Args &&...args) : internal(std::forward<Function>(f), std::forward<Args>(args)...) {
		IncrementIfJoinable();
	}

	void swap(thread &other) noexcept {
		internal.swap(other.internal);
	}

	std::thread::id get_id() const noexcept {
		return internal.get_id();
	}

	bool joinable() const noexcept {
		return internal.joinable();
	}

	void join() {
#ifdef DUCKDB_DEBUG_THREADS
		if (internal.joinable()) {
			--thread_count;
		}
#endif
		internal.join();
	}

	void detach() {
#ifdef DUCKDB_DEBUG_THREADS
		if (internal.joinable()) {
			--thread_count;
		}
#endif
		internal.detach();
	}

	static unsigned int hardware_concurrency() noexcept {
		return std::thread::hardware_concurrency();
	}

#ifdef DUCKDB_DEBUG_THREADS
	static int ThreadCount() noexcept {
		return thread_count.load();
	}
#endif

	std::thread::native_handle_type native_handle() {
		return internal.native_handle();
	}
};

} // namespace duckdb
