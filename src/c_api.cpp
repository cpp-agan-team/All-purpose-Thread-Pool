#include "universal_thread_pool/c_api.h"
#include "universal_thread_pool/thread_pool.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <exception>
#include <memory>
#include <string>
#include <utility>

struct utp_pool {
    explicit utp_pool(universal_thread_pool::thread_pool_options options)
        : pool(std::move(options)) {}

    universal_thread_pool::thread_pool pool;
};

namespace {

universal_thread_pool::shutdown_policy to_cpp_shutdown(utp_shutdown_policy policy) {
    switch (policy) {
    case UTP_SHUTDOWN_CANCEL_PENDING:
        return universal_thread_pool::shutdown_policy::cancel_pending;
    case UTP_SHUTDOWN_STOP_IMMEDIATELY:
        return universal_thread_pool::shutdown_policy::stop_immediately;
    case UTP_SHUTDOWN_DRAIN:
    default:
        return universal_thread_pool::shutdown_policy::drain;
    }
}

universal_thread_pool::thread_pool_options to_cpp_options(const utp_pool_options* input) {
    const auto options_value = input ? *input : utp_default_options();
    universal_thread_pool::thread_pool_options options;
    options.initial_threads = std::max<std::size_t>(1, options_value.threads);
    options.max_threads = options.initial_threads;
    options.worker_batch_size = std::max<std::size_t>(1, options_value.worker_batch_size);
    options.enable_work_stealing = options_value.enable_work_stealing != 0;
    options.enable_priority = options_value.enable_priority != 0;
    options.scheduler = options.enable_work_stealing
        ? universal_thread_pool::schedule_policy::work_stealing
        : (options.enable_priority
            ? universal_thread_pool::schedule_policy::priority
            : universal_thread_pool::schedule_policy::fifo);
    options.mode = options.enable_work_stealing
        ? universal_thread_pool::pool_mode::work_stealing
        : universal_thread_pool::pool_mode::fixed;

    if (options_value.bounded_queue) {
        options.queue = universal_thread_pool::queue_policy::bounded_reject;
        options.max_queue_size = std::max<std::size_t>(1, options_value.max_queue_size);
    }

    return options;
}

void report_error(utp_error_fn on_error, void* user_data, const char* message) noexcept {
    if (!on_error) {
        return;
    }
    try {
        on_error(message, user_data);
    } catch (...) {
    }
}

} // namespace

extern "C" {

utp_pool_options utp_default_options(void) {
    utp_pool_options options{};
    options.threads = universal_thread_pool::default_thread_count();
    options.max_queue_size = options.threads * 4096;
    options.worker_batch_size = 4;
    options.bounded_queue = 0;
    options.enable_work_stealing = 0;
    options.enable_priority = 0;
    return options;
}

utp_pool* utp_create(const utp_pool_options* options) {
    try {
        return new utp_pool(to_cpp_options(options));
    } catch (...) {
        return nullptr;
    }
}

void utp_destroy(utp_pool* pool) {
    try {
        delete pool;
    } catch (...) {
    }
}

int utp_detach(utp_pool* pool, utp_task_fn task, void* user_data) {
    return utp_detach_checked(pool, task, user_data, nullptr, nullptr);
}

int utp_detach_checked(
    utp_pool* pool,
    utp_task_fn task,
    void* user_data,
    utp_error_fn on_error,
    void* error_user_data) {
    if (!pool || !task) {
        report_error(on_error, error_user_data, "invalid utp_detach arguments");
        return 0;
    }

    try {
        return pool->pool.detach([task, user_data, on_error, error_user_data] {
            try {
                task(user_data);
            } catch (const std::exception& error) {
                report_error(on_error, error_user_data, error.what());
            } catch (...) {
                report_error(on_error, error_user_data, "unknown C task exception");
            }
        }) ? 1 : 0;
    } catch (const std::exception& error) {
        report_error(on_error, error_user_data, error.what());
        return 0;
    } catch (...) {
        report_error(on_error, error_user_data, "unknown enqueue exception");
        return 0;
    }
}

void utp_shutdown(utp_pool* pool, utp_shutdown_policy policy) {
    if (!pool) {
        return;
    }
    try {
        pool->pool.shutdown(to_cpp_shutdown(policy));
    } catch (...) {
    }
}

int utp_wait_for_idle_ms(utp_pool* pool, uint64_t timeout_ms) {
    if (!pool) {
        return 0;
    }
    try {
        return pool->pool.wait_for_idle(
            std::chrono::milliseconds(timeout_ms)) ? 1 : 0;
    } catch (...) {
        return 0;
    }
}

utp_metrics utp_get_metrics(utp_pool* pool) {
    utp_metrics output{};
    if (!pool) {
        return output;
    }

    try {
        const auto metrics = pool->pool.metrics();
        output.submitted_tasks_total = metrics.submitted_tasks_total;
        output.completed_tasks_total = metrics.completed_tasks_total;
        output.failed_tasks_total = metrics.failed_tasks_total;
        output.cancelled_tasks_total = metrics.cancelled_tasks_total;
        output.rejected_tasks_total = metrics.rejected_tasks_total;
        output.steal_success_total = metrics.steal_success_total;
        output.steal_fail_total = metrics.steal_fail_total;
        output.queued_tasks = metrics.queued_tasks;
        output.running_tasks = metrics.running_tasks;
        output.total_threads = metrics.total_threads;
        output.active_tasks = metrics.active_tasks;
        output.blocked_workers = metrics.blocked_workers;
    } catch (...) {
    }
    return output;
}

size_t utp_metrics_json(utp_pool* pool, char* buffer, size_t buffer_size) {
    if (!pool) {
        return 0;
    }

    try {
        const auto json = universal_thread_pool::to_json(pool->pool.metrics(), "c_api_pool");
        const auto required = json.size() + 1;
        if (!buffer || buffer_size == 0) {
            return required;
        }

        const auto to_copy = std::min(buffer_size - 1, json.size());
        std::memcpy(buffer, json.data(), to_copy);
        buffer[to_copy] = '\0';
        return required;
    } catch (...) {
        if (buffer && buffer_size > 0) {
            buffer[0] = '\0';
        }
        return 0;
    }
}

} // extern "C"
