#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <future>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#if ((defined(__cplusplus) && __cplusplus >= 202002L) || \
     (defined(_MSVC_LANG) && _MSVC_LANG >= 202002L))
#if defined(__has_include)
#if __has_include(<coroutine>)
#include <coroutine>
#define UNIVERSAL_THREAD_POOL_HAS_COROUTINE 1
#endif
#endif
#endif

#if ((defined(__cplusplus) && __cplusplus >= 202002L) || \
     (defined(_MSVC_LANG) && _MSVC_LANG >= 202002L))
#if defined(__has_include)
#if __has_include(<stop_token>)
#include <stop_token>
#define UNIVERSAL_THREAD_POOL_HAS_STOP_TOKEN 1
#endif
#endif
#endif

#ifndef UNIVERSAL_THREAD_POOL_HAS_COROUTINE
#define UNIVERSAL_THREAD_POOL_HAS_COROUTINE 0
#endif

#ifndef UNIVERSAL_THREAD_POOL_HAS_STOP_TOKEN
#define UNIVERSAL_THREAD_POOL_HAS_STOP_TOKEN 0
#endif

#ifndef UNIVERSAL_THREAD_POOL_VERSION_MAJOR
#define UNIVERSAL_THREAD_POOL_VERSION_MAJOR 0
#endif

#ifndef UNIVERSAL_THREAD_POOL_VERSION_MINOR
#define UNIVERSAL_THREAD_POOL_VERSION_MINOR 1
#endif

#ifndef UNIVERSAL_THREAD_POOL_VERSION_PATCH
#define UNIVERSAL_THREAD_POOL_VERSION_PATCH 0
#endif

#ifndef UNIVERSAL_THREAD_POOL_VERSION_STRING
#define UNIVERSAL_THREAD_POOL_VERSION_STRING "0.1.0-dev"
#endif

namespace universal_thread_pool {

inline constexpr int version_major = UNIVERSAL_THREAD_POOL_VERSION_MAJOR;
inline constexpr int version_minor = UNIVERSAL_THREAD_POOL_VERSION_MINOR;
inline constexpr int version_patch = UNIVERSAL_THREAD_POOL_VERSION_PATCH;
inline constexpr const char* version_string = UNIVERSAL_THREAD_POOL_VERSION_STRING;
inline constexpr std::size_t cache_line_size = 64;
inline constexpr std::size_t metric_latency_bucket_count = 13;
inline constexpr std::array<std::uint64_t, metric_latency_bucket_count - 1>
    metric_latency_bucket_upper_bounds_ns{
        1000ULL,
        10000ULL,
        100000ULL,
        1000000ULL,
        5000000ULL,
        10000000ULL,
        50000000ULL,
        100000000ULL,
        500000000ULL,
        1000000000ULL,
        5000000000ULL,
        10000000000ULL};

using latency_bucket_array = std::array<std::uint64_t, metric_latency_bucket_count>;

double estimate_latency_percentile_ns(
    const latency_bucket_array& buckets,
    std::uint64_t sample_count,
    double percentile) noexcept;

namespace detail {

template <class T, class F, bool IsVoid = std::is_void<T>::value>
struct continuation_result;

template <class T, class F>
struct continuation_result<T, F, false> {
    using type = std::invoke_result_t<F, T>;
};

template <class T, class F>
struct continuation_result<T, F, true> {
    using type = std::invoke_result_t<F>;
};

template <class T, class F>
using continuation_result_t = typename continuation_result<T, F>::type;

template <class T, class F, bool IsVoid = std::is_void<T>::value>
struct dataflow_result;

template <class T, class F>
struct dataflow_result<T, F, false> {
    using type = std::invoke_result_t<F, std::vector<T>>;
};

template <class T, class F>
struct dataflow_result<T, F, true> {
    using type = std::invoke_result_t<F>;
};

template <class T, class F>
using dataflow_result_t = typename dataflow_result<T, F>::type;

template <class F, class... Args>
using bound_result_t = std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>;

template <class Result, class F, class... Args>
class bound_task {
public:
    bound_task(F&& f, Args&&... args)
        : func_(std::forward<F>(f)),
          args_(std::forward<Args>(args)...) {}

    Result operator()() {
        if constexpr (std::is_void<Result>::value) {
            std::apply(std::move(func_), std::move(args_));
        } else {
            return std::apply(std::move(func_), std::move(args_));
        }
    }

private:
    std::decay_t<F> func_;
    std::tuple<std::decay_t<Args>...> args_;
};

template <class FutureRange>
void get_all_futures_or_rethrow(FutureRange& futures) {
    std::exception_ptr first_error;
    for (auto& future : futures) {
        try {
            future.get();
        } catch (...) {
            if (!first_error) {
                first_error = std::current_exception();
            }
        }
    }

    if (first_error) {
        std::rethrow_exception(first_error);
    }
}

} // namespace detail

std::size_t default_thread_count() noexcept;

enum class pool_mode {
    fixed,
    cached,
    work_stealing
};

enum class queue_policy {
    unbounded,
    bounded_block,
    bounded_reject,
    bounded_caller_runs,
    bounded_drop_oldest
};

enum class shutdown_policy {
    drain,
    cancel_pending,
    stop_immediately
};

enum class schedule_policy {
    fifo,
    priority,
    work_stealing
};

enum class task_priority : int {
    low = 0,
    normal = 1,
    high = 2,
    critical = 3
};

enum class loop_schedule {
    static_blocks,
    dynamic_blocks,
    guided_blocks
};

enum class task_kind {
    cpu_bound,
    blocking_io,
    background,
    latency_sensitive
};

enum class thread_pool_state {
    running,
    paused,
    stopping,
    stopped
};

struct loop_options {
    std::size_t block_size = 0;
    loop_schedule schedule = loop_schedule::static_blocks;
    task_priority priority = task_priority::normal;
};

struct task_source_location {
    const char* file = "";
    int line = 0;
    const char* function = "";
};

#ifndef UNIVERSAL_THREAD_POOL_SOURCE_LOCATION
#define UNIVERSAL_THREAD_POOL_SOURCE_LOCATION \
    ::universal_thread_pool::task_source_location{__FILE__, __LINE__, __func__}
#endif

struct task_metadata {
    std::uint64_t id = 0;
    std::uint64_t sequence = 0;
    task_priority priority = task_priority::normal;
    task_kind kind = task_kind::cpu_bound;
    std::size_t numa_node = static_cast<std::size_t>(-1);
    std::string name;
    task_source_location source;
    std::chrono::steady_clock::time_point enqueue_time{};
    std::chrono::steady_clock::time_point start_time{};
    std::chrono::steady_clock::time_point finish_time{};
    std::size_t worker_id = static_cast<std::size_t>(-1);
};

using task_hook = std::function<void(const task_metadata&)>;
using task_error_hook = std::function<void(const task_metadata&, std::exception_ptr)>;

struct thread_pool_options {
    std::string name = "thread_pool";
    std::string thread_name_prefix;

    std::size_t min_threads = 0;
    std::size_t initial_threads = default_thread_count();
    std::size_t max_threads = default_thread_count();

    pool_mode mode = pool_mode::fixed;
    schedule_policy scheduler = schedule_policy::fifo;
    queue_policy queue = queue_policy::unbounded;

    std::size_t max_queue_size = 0;
    std::size_t queue_expand_threshold = 1024;
    std::size_t worker_batch_size = 4;
    std::size_t continuation_threads = 1;
    std::size_t numa_nodes = 1;
    std::size_t priority_fairness_interval = 32;
    std::vector<std::size_t> thread_affinity_cpu_ids;
    std::chrono::milliseconds idle_timeout{30000};

    bool enable_priority = false;
    bool enable_work_stealing = false;
    bool enable_metrics = true;
    bool enable_deadlock_check = true;
    bool enable_managed_blocking = true;
    bool enable_thread_affinity = false;

    std::function<void(std::size_t)> on_thread_start;
    std::function<void(std::size_t, std::size_t)> on_thread_affinity;
    std::function<void(std::size_t)> on_thread_stop;
    std::function<void(std::exception_ptr)> on_unhandled_exception;

    task_hook on_task_queued;
    task_hook on_task_start;
    task_hook on_task_finish;
    task_hook on_task_cancel;
    task_error_hook on_task_error;
};

thread_pool_options make_cpu_pool_options(
    std::size_t threads = default_thread_count());

thread_pool_options make_io_pool_options(
    std::size_t threads = default_thread_count() * 2);

thread_pool_options make_background_pool_options(
    std::size_t threads = std::max<std::size_t>(1, default_thread_count() / 2));

thread_pool_options make_cached_pool_options(
    std::size_t min_threads = 0,
    std::size_t max_threads = default_thread_count() * 4);

struct worker_metrics {
    std::size_t id = static_cast<std::size_t>(-1);
    std::size_t numa_node = static_cast<std::size_t>(-1);
    std::uint64_t completed_tasks_total = 0;
    std::uint64_t failed_tasks_total = 0;
    std::uint64_t cancelled_tasks_total = 0;
    std::size_t local_queue_size = 0;
    bool finished = false;
};

struct thread_pool_metrics {
    std::uint64_t submitted_tasks_total = 0;
    std::uint64_t completed_tasks_total = 0;
    std::uint64_t failed_tasks_total = 0;
    std::uint64_t cancelled_tasks_total = 0;
    std::uint64_t rejected_tasks_total = 0;
    std::uint64_t caller_runs_total = 0;
    std::uint64_t scheduled_tasks_total = 0;
    std::uint64_t scheduled_tasks_cancelled_total = 0;
    std::uint64_t local_tasks_total = 0;
    std::uint64_t steal_success_total = 0;
    std::uint64_t steal_fail_total = 0;
    std::uint64_t priority_fairness_picks_total = 0;
    std::uint64_t managed_blocking_total = 0;
    std::uint64_t managed_blocking_compensations_total = 0;
    std::uint64_t thread_affinity_applied_total = 0;
    std::uint64_t thread_affinity_failed_total = 0;
    std::uint64_t worker_wakeup_total = 0;
    std::uint64_t worker_idle_timeout_total = 0;
    std::uint64_t worker_retired_total = 0;
    std::uint64_t task_wait_time_ns_total = 0;
    std::uint64_t task_run_time_ns_total = 0;
    latency_bucket_array task_wait_time_buckets{};
    latency_bucket_array task_run_time_buckets{};
    std::uint64_t max_queue_size_seen = 0;

    std::size_t queued_tasks = 0;
    std::size_t delayed_tasks = 0;
    std::size_t running_tasks = 0;
    std::size_t total_threads = 0;
    std::size_t idle_threads = 0;
    std::size_t active_tasks = 0;
    std::size_t blocked_workers = 0;

    std::vector<worker_metrics> workers;

    bool accepting = false;
    bool paused = false;
    bool stopping = false;
    thread_pool_state state = thread_pool_state::stopped;

    std::uint64_t finished_tasks_total() const noexcept {
        return completed_tasks_total + failed_tasks_total + cancelled_tasks_total;
    }

    std::size_t pending_tasks_total() const noexcept {
        return queued_tasks + delayed_tasks + active_tasks;
    }

    std::uint64_t wait_time_sample_count() const noexcept {
        return std::accumulate(
            task_wait_time_buckets.begin(),
            task_wait_time_buckets.end(),
            std::uint64_t{0});
    }

    std::uint64_t run_time_sample_count() const noexcept {
        return std::accumulate(
            task_run_time_buckets.begin(),
            task_run_time_buckets.end(),
            std::uint64_t{0});
    }

    double average_wait_time_ns() const noexcept {
        const auto samples = wait_time_sample_count();
        return samples == 0 ? 0.0 :
            static_cast<double>(task_wait_time_ns_total) / static_cast<double>(samples);
    }

    double average_run_time_ns() const noexcept {
        const auto samples = run_time_sample_count();
        return samples == 0 ? 0.0 :
            static_cast<double>(task_run_time_ns_total) / static_cast<double>(samples);
    }

    double wait_time_percentile_estimate_ns(double percentile) const noexcept {
        return estimate_latency_percentile_ns(
            task_wait_time_buckets,
            wait_time_sample_count(),
            percentile);
    }

    double run_time_percentile_estimate_ns(double percentile) const noexcept {
        return estimate_latency_percentile_ns(
            task_run_time_buckets,
            run_time_sample_count(),
            percentile);
    }

    bool healthy() const noexcept {
        return state == thread_pool_state::running || state == thread_pool_state::paused;
    }
};

std::string to_prometheus(
    const thread_pool_metrics& metrics,
    const std::string& pool_name = "thread_pool");

std::string to_json(
    const thread_pool_metrics& metrics,
    const std::string& pool_name = "thread_pool");

std::string to_opentelemetry_json(
    const thread_pool_metrics& metrics,
    const std::string& pool_name = "thread_pool");

struct hardware_topology {
    std::size_t logical_cpus = default_thread_count();
    std::size_t numa_nodes = 1;
    std::vector<std::size_t> cpu_ids;
};

hardware_topology detect_hardware_topology();

thread_pool_options make_affinity_pool_options(
    std::size_t threads = default_thread_count(),
    std::vector<std::size_t> cpu_ids = {});

class task_rejected : public std::runtime_error {
public:
    explicit task_rejected(const std::string& message)
        : std::runtime_error(message) {}
};

class thread_pool_closed : public std::runtime_error {
public:
    explicit thread_pool_closed(const std::string& message)
        : std::runtime_error(message) {}
};

class task_cancelled : public std::runtime_error {
public:
    explicit task_cancelled(const std::string& message)
        : std::runtime_error(message) {}
};

class cancellation_state {
public:
    void request_stop() noexcept {
        stopped_.store(true, std::memory_order_release);
    }

    bool stop_requested() const noexcept {
        return stopped_.load(std::memory_order_acquire);
    }

private:
    std::atomic<bool> stopped_{false};
};

class cancellation_token {
public:
    cancellation_token() = default;

    bool stop_requested() const noexcept {
        const auto state = state_.lock();
        return state && state->stop_requested();
    }

    explicit operator bool() const noexcept {
        return !state_.expired();
    }

private:
    friend class cancellation_source;

    explicit cancellation_token(std::weak_ptr<cancellation_state> state)
        : state_(std::move(state)) {}

    std::weak_ptr<cancellation_state> state_;
};

class cancellation_source {
public:
    cancellation_source()
        : state_(std::make_shared<cancellation_state>()) {}

    cancellation_token token() const noexcept {
        return cancellation_token{state_};
    }

    void request_stop() const noexcept {
        state_->request_stop();
    }

    bool stop_requested() const noexcept {
        return state_->stop_requested();
    }

private:
    std::shared_ptr<cancellation_state> state_;
};

struct task_options {
    std::optional<task_priority> priority;
    task_kind kind = task_kind::cpu_bound;
    std::optional<std::size_t> numa_node;
    std::string name;
    task_source_location source;
    cancellation_token cancellation;
    std::optional<std::chrono::steady_clock::time_point> deadline;
};

struct retry_options {
    std::size_t max_attempts = 3;
    std::chrono::nanoseconds initial_backoff{0};
    std::chrono::nanoseconds max_backoff{std::chrono::milliseconds{1000}};
    double backoff_multiplier = 2.0;
    bool retry_on_task_cancelled = false;
    std::function<bool(std::exception_ptr, std::size_t)> should_retry;
};

struct periodic_options {
    std::chrono::nanoseconds interval{std::chrono::seconds{1}};
    std::chrono::nanoseconds initial_delay{0};
    std::size_t max_runs = 0;
    bool run_immediately = false;
    bool continue_on_error = true;
};

class scheduled_task_state {
public:
    bool request_cancel() noexcept {
        bool expected = false;
        return cancelled_.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
    }

    bool cancelled() const noexcept {
        return cancelled_.load(std::memory_order_acquire);
    }

private:
    std::atomic<bool> cancelled_{false};
};

class scheduled_task_handle {
public:
    scheduled_task_handle() = default;

    bool cancel() noexcept {
        const auto state = state_.lock();
        return state && state->request_cancel();
    }

    bool cancelled() const noexcept {
        const auto state = state_.lock();
        return state && state->cancelled();
    }

    explicit operator bool() const noexcept {
        return !state_.expired();
    }

private:
    friend class thread_pool;

    explicit scheduled_task_handle(std::weak_ptr<scheduled_task_state> state)
        : state_(std::move(state)) {}

    std::weak_ptr<scheduled_task_state> state_;
};

template <class T>
struct scheduled_future {
    std::future<T> future;
    scheduled_task_handle handle;

    bool cancel() noexcept {
        return handle.cancel();
    }

    bool cancelled() const noexcept {
        return handle.cancelled();
    }

    explicit operator bool() const noexcept {
        return future.valid() && static_cast<bool>(handle);
    }
};

class periodic_task_state {
public:
    bool request_cancel() noexcept {
        bool expected = false;
        const auto changed =
            cancelled_.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.cancel();
        return changed;
    }

    bool cancelled() const noexcept {
        return cancelled_.load(std::memory_order_acquire);
    }

    std::uint64_t record_run() noexcept {
        return runs_.fetch_add(1, std::memory_order_acq_rel) + 1;
    }

    std::uint64_t runs() const noexcept {
        return runs_.load(std::memory_order_acquire);
    }

    void set_pending(scheduled_task_handle handle) noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_ = std::move(handle);
        if (cancelled()) {
            pending_.cancel();
        }
    }

private:
    std::atomic<bool> cancelled_{false};
    std::atomic<std::uint64_t> runs_{0};
    mutable std::mutex mutex_;
    scheduled_task_handle pending_;
};

class periodic_task_handle {
public:
    periodic_task_handle() = default;

    bool cancel() noexcept {
        const auto state = state_.lock();
        return state && state->request_cancel();
    }

    bool cancelled() const noexcept {
        const auto state = state_.lock();
        return !state || state->cancelled();
    }

    std::uint64_t runs() const noexcept {
        const auto state = state_.lock();
        return state ? state->runs() : 0;
    }

    explicit operator bool() const noexcept {
        return !state_.expired();
    }

private:
    friend class thread_pool;

    explicit periodic_task_handle(std::weak_ptr<periodic_task_state> state)
        : state_(std::move(state)) {}

    std::weak_ptr<periodic_task_state> state_;
};

template <class T>
struct when_any_result {
    std::size_t index = 0;
    T value;
};

template <>
struct when_any_result<void> {
    std::size_t index = 0;
};

class thread_pool;

#if UNIVERSAL_THREAD_POOL_HAS_COROUTINE
class schedule_awaiter;

template <class T>
class future_awaiter;

template <class Callable, class Result = std::invoke_result_t<Callable&>>
class submit_awaiter;
#endif

} // namespace universal_thread_pool
