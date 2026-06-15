#include "universal_thread_pool/runtime.hpp"

namespace universal_thread_pool {

thread_pool_runtime::thread_pool_runtime(thread_pool_runtime_options options)
    : cpu_pool_(std::move(options.cpu)),
      io_pool_(std::move(options.io)),
      background_pool_(std::move(options.background)) {}

thread_pool_runtime& global_runtime() {
    static thread_pool_runtime runtime;
    return runtime;
}

thread_pool& thread_pool_runtime::cpu_pool() noexcept {
    return cpu_pool_;
}

thread_pool& thread_pool_runtime::io_pool() noexcept {
    return io_pool_;
}

thread_pool& thread_pool_runtime::background_pool() noexcept {
    return background_pool_;
}

const thread_pool& thread_pool_runtime::cpu_pool() const noexcept {
    return cpu_pool_;
}

const thread_pool& thread_pool_runtime::io_pool() const noexcept {
    return io_pool_;
}

const thread_pool& thread_pool_runtime::background_pool() const noexcept {
    return background_pool_;
}

executor thread_pool_runtime::cpu_executor(task_priority priority) noexcept {
    return cpu_pool_.get_executor(priority);
}

executor thread_pool_runtime::io_executor(task_priority priority) noexcept {
    return io_pool_.get_executor(priority);
}

executor thread_pool_runtime::background_executor(task_priority priority) noexcept {
    return background_pool_.get_executor(priority);
}

executor thread_pool_runtime::executor_for(task_kind kind) noexcept {
    switch (kind) {
    case task_kind::blocking_io:
        return io_executor(task_priority::normal);
    case task_kind::background:
        return background_executor(task_priority::low);
    case task_kind::latency_sensitive:
        return cpu_executor(task_priority::high);
    case task_kind::cpu_bound:
    default:
        return cpu_executor(task_priority::normal);
    }
}

runtime_executors thread_pool_runtime::executors() noexcept {
    return runtime_executors{
        cpu_executor(),
        io_executor(),
        background_executor(),
        cpu_executor(task_priority::high)};
}

void thread_pool_runtime::pause_all() {
    cpu_pool_.pause();
    io_pool_.pause();
    background_pool_.pause();
}

void thread_pool_runtime::resume_all() {
    cpu_pool_.resume();
    io_pool_.resume();
    background_pool_.resume();
}

void thread_pool_runtime::shutdown(shutdown_policy policy) {
    cpu_pool_.shutdown(policy);
    io_pool_.shutdown(policy);
    background_pool_.shutdown(policy);
}

bool thread_pool_runtime::wait_for_idle(std::chrono::nanoseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    auto remaining = [&deadline] {
        const auto now = std::chrono::steady_clock::now();
        return now >= deadline
            ? std::chrono::nanoseconds{0}
            : std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now);
    };

    if (!cpu_pool_.wait_for_idle(remaining())) {
        return false;
    }
    if (!io_pool_.wait_for_idle(remaining())) {
        return false;
    }
    return background_pool_.wait_for_idle(remaining());
}

void thread_pool_runtime::join() noexcept {
    cpu_pool_.join();
    io_pool_.join();
    background_pool_.join();
}

runtime_metrics thread_pool_runtime::metrics() const {
    return runtime_metrics{
        cpu_pool_.metrics(),
        io_pool_.metrics(),
        background_pool_.metrics()};
}

task_priority thread_pool_runtime::priority_for(task_kind kind) noexcept {
    switch (kind) {
    case task_kind::background:
        return task_priority::low;
    case task_kind::latency_sensitive:
        return task_priority::high;
    case task_kind::blocking_io:
    case task_kind::cpu_bound:
    default:
        return task_priority::normal;
    }
}

thread_pool& thread_pool_runtime::pool_for(task_kind kind) noexcept {
    switch (kind) {
    case task_kind::blocking_io:
        return io_pool_;
    case task_kind::background:
        return background_pool_;
    case task_kind::latency_sensitive:
    case task_kind::cpu_bound:
    default:
        return cpu_pool_;
    }
}

} // namespace universal_thread_pool
