//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/thread.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <thread>
#include <utility>

namespace duckdb {

class thread {
private:
	std::thread internal;

public:
	template <class... Args>
	explicit thread(Args &&...args) : internal(std::forward<Args>(args)...) {
	}

	~thread() {
		if (internal.joinable()) {
			internal.join();
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
		internal.join();
	}

	void detach() {
		internal.detach();
	}

	static unsigned int hardware_concurrency() noexcept {
		return std::thread::hardware_concurrency();
	}

	std::thread::native_handle_type native_handle() {
		return internal.native_handle();
	}

	// Other methods from std::thread can be added similarly
};

} // namespace duckdb
