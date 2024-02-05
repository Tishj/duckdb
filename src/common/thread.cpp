#include "duckdb/common/thread.hpp"

namespace duckdb {

std::atomic<int> thread::thread_count {0};

} // namespace duckdb
