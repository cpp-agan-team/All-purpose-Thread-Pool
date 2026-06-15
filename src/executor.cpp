#include "universal_thread_pool/thread_pool_core.hpp"

namespace universal_thread_pool {

#if UNIVERSAL_THREAD_POOL_HAS_COROUTINE
schedule_awaiter executor::schedule() const noexcept {
    return schedule_awaiter{*this};
}
#endif

std::future<void> executor::when_all(std::vector<std::future<void>> futures) const {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->when_all(std::move(futures));
}

std::future<when_any_result<void>> executor::when_any(
    std::vector<std::future<void>> futures) const {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->when_any(std::move(futures));
}

bool executor::is_worker_thread() const noexcept {
    return pool_ && pool_->is_worker_thread();
}

} // namespace universal_thread_pool
