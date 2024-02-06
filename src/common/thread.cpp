#include "duckdb/common/thread.hpp"

namespace duckdb {

#ifdef DUCKDB_DEBUG_THREADS
std::atomic<int> thread::thread_count {0};
#endif

} // namespace duckdb
