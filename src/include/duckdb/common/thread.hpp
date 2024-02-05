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
	static std::atomic<int> thread_count;

	void IncrementIfJoinable() {
		if (internal.joinable()) {
			thread_count++;
			Printer::Print("thread_count " + std::to_string(thread_count.load()));
		}
	}

public:
	template <class... Args>
	explicit thread(Args &&...args) : internal(std::forward<Args>(args)...) {
		IncrementIfJoinable();
	}

	~thread() {
		if (internal.joinable()) {
			thread_count--;
		}
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
		if (internal.joinable()) {
			thread_count--;
		}
		internal.join();
	}

	void detach() {
		if (internal.joinable()) {
			thread_count--;
		}
		internal.detach();
	}

	static unsigned int hardware_concurrency() noexcept {
		return std::thread::hardware_concurrency();
	}

	static int ThreadCount() noexcept {
		return thread_count;
	}

	std::thread::native_handle_type native_handle() {
		return internal.native_handle();
	}
};

} // namespace duckdb
