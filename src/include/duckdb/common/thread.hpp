//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/thread.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

#include <thread>
#include <utility>

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

	// Copy constructor
	thread(const thread &other) : internal(other.internal) {
	}

	// Move constructor
	thread(thread &&other) noexcept : internal(std::move(other.internal)) {
	}

	// Copy assignment operator
	thread &operator=(const thread &other) {
		if (this != &other) {
			internal = other.internal;
		}
		return *this;
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

// Non-member swap function for thread objects
void swap(thread &t1, thread &t2) noexcept {
	t1.swap(t2);
}

} // namespace duckdb
