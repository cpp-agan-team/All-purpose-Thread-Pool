#include "universal_thread_pool/task_group.hpp"

namespace universal_thread_pool {

namespace {

void remember_first_error(std::exception_ptr& first_error) {
    if (!first_error) {
        first_error = std::current_exception();
    }
}

void restore_futures(
    std::vector<std::future<void>>& target,
    std::vector<std::future<void>>& source) {
    target.reserve(target.size() + source.size());
    for (auto& future : source) {
        if (future.valid()) {
            target.push_back(std::move(future));
        }
    }
}

void get_all_with_executor_help(
    executor& ex,
    std::vector<std::future<void>>& futures) {
    std::exception_ptr first_error;
    for (auto& future : futures) {
        if (!future.valid()) {
            continue;
        }

        try {
            ex.wait_with_help(future);
            future.get();
        } catch (...) {
            remember_first_error(first_error);
        }
    }

    if (first_error) {
        std::rethrow_exception(first_error);
    }
}

} // namespace

task_group::task_group(executor ex)
    : executor_(std::move(ex)) {}

void task_group::wait() {
    std::vector<std::future<void>> local;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        local.swap(futures_);
    }

    get_all_with_executor_help(executor_, local);
}

bool task_group::wait_for(std::chrono::nanoseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::vector<std::future<void>> local;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        local.swap(futures_);
    }

    for (auto& future : local) {
        if (!future.valid()) {
            continue;
        }

        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline ||
            future.wait_for(deadline - now) != std::future_status::ready) {
            std::lock_guard<std::mutex> lock(mutex_);
            restore_futures(futures_, local);
            return false;
        }
    }

    get_all_with_executor_help(executor_, local);

    return true;
}

std::size_t task_group::clear_ready() {
    std::vector<std::future<void>> ready;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = futures_.begin();
        while (it != futures_.end()) {
            if (it->valid() &&
                it->wait_for(std::chrono::nanoseconds{0}) == std::future_status::ready) {
                ready.push_back(std::move(*it));
                it = futures_.erase(it);
            } else {
                ++it;
            }
        }
    }

    get_all_with_executor_help(executor_, ready);

    return ready.size();
}

std::size_t task_group::pending() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return futures_.size();
}

cancellation_token task_group::token() const noexcept {
    return cancellation_.token();
}

bool task_group::stop_requested() const noexcept {
    return cancellation_.stop_requested();
}

void task_group::cancel() noexcept {
    cancellation_.request_stop();
}

} // namespace universal_thread_pool
