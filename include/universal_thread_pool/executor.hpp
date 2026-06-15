#pragma once

#include "universal_thread_pool/common.hpp"

namespace universal_thread_pool {

class executor {
public:
    executor() = default;

    template <class F, class... Args>
    auto submit(F&& f, Args&&... args) const
        -> std::future<std::invoke_result_t<F, Args...>>;

    template <class F, class... Args>
    auto submit_with_options(task_options options, F&& f, Args&&... args) const
        -> std::future<std::invoke_result_t<F, Args...>>;

    template <class F, class... Args>
    auto try_submit(F&& f, Args&&... args) const
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>>;

    template <class F, class... Args>
    auto try_submit_with_options(task_options options, F&& f, Args&&... args) const
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>>;

    template <class Rep, class Period, class F, class... Args>
    auto try_submit_for(std::chrono::duration<Rep, Period> timeout, F&& f, Args&&... args) const
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>>;

    template <class Rep, class Period, class F, class... Args>
    auto try_submit_with_options_for(
        task_options options,
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args) const
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>>;

    template <class Clock, class Duration, class F, class... Args>
    auto try_submit_until(
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args) const
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>>;

    template <class Clock, class Duration, class F, class... Args>
    auto try_submit_with_options_until(
        task_options options,
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args) const
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>>;

    template <class F, class... Args>
    auto submit_cancellable(cancellation_token token, F&& f, Args&&... args) const
        -> std::future<std::invoke_result_t<F, Args...>>;

    template <class F, class... Args>
    auto submit_retry(retry_options retry, F&& f, Args&&... args) const
        -> std::future<std::invoke_result_t<std::decay_t<F>&, std::decay_t<Args>&...>>;

    template <class F, class... Args>
    auto submit_retry_with_options(
        retry_options retry,
        task_options options,
        F&& f,
        Args&&... args) const
        -> std::future<std::invoke_result_t<std::decay_t<F>&, std::decay_t<Args>&...>>;

#if UNIVERSAL_THREAD_POOL_HAS_STOP_TOKEN
    template <class F, class... Args>
    auto submit_stop_token(std::stop_token token, F&& f, Args&&... args) const
        -> std::future<std::invoke_result_t<F, Args...>>;
#endif

    template <class F>
    auto bulk_submit(std::size_t count, F&& f) const
        -> std::vector<std::future<std::invoke_result_t<std::decay_t<F>&, std::size_t>>>;

    // Copies task_options for each item. A shared cancellation token cancels
    // the whole batch by design.
    template <class F>
    auto bulk_submit_with_options(task_options options, std::size_t count, F&& f) const
        -> std::vector<std::future<std::invoke_result_t<std::decay_t<F>&, std::size_t>>>;

    template <class Rep, class Period, class F, class... Args>
    auto schedule_submit_after(
        std::chrono::duration<Rep, Period> delay,
        F&& f,
        Args&&... args) const
        -> scheduled_future<std::invoke_result_t<F, Args...>>;

    template <class Clock, class Duration, class F, class... Args>
    auto schedule_submit_at(
        std::chrono::time_point<Clock, Duration> time_point,
        F&& f,
        Args&&... args) const
        -> scheduled_future<std::invoke_result_t<F, Args...>>;

    template <class Rep, class Period, class F, class... Args>
    auto schedule_submit_after_with_options(
        task_options options,
        std::chrono::duration<Rep, Period> delay,
        F&& f,
        Args&&... args) const
        -> scheduled_future<std::invoke_result_t<F, Args...>>;

    template <class Clock, class Duration, class F, class... Args>
    auto schedule_submit_at_with_options(
        task_options options,
        std::chrono::time_point<Clock, Duration> time_point,
        F&& f,
        Args&&... args) const
        -> scheduled_future<std::invoke_result_t<F, Args...>>;

    template <class Rep, class Period, class F, class... Args>
    periodic_task_handle detach_periodic(
        std::chrono::duration<Rep, Period> interval,
        F&& f,
        Args&&... args) const;

    template <class F, class... Args>
    periodic_task_handle detach_periodic_with_options(
        periodic_options periodic,
        task_options options,
        F&& f,
        Args&&... args) const;

    template <class T, class F>
    auto submit_then(std::future<T> predecessor, F&& continuation) const
        -> std::future<detail::continuation_result_t<T, F>>;

    template <class T, class F>
    auto continue_with(std::future<T> predecessor, F&& continuation) const
        -> std::future<detail::continuation_result_t<T, F>>;

    template <class T, class F>
    auto continue_on(executor completion, std::future<T> predecessor, F&& continuation) const
        -> std::future<detail::continuation_result_t<T, F>>;

    template <class T, class F>
    auto dataflow(std::vector<std::future<T>> predecessors, F&& continuation) const
        -> std::future<detail::dataflow_result_t<T, F>>;

    template <class T, class F>
    auto dataflow_on(
        executor completion,
        std::vector<std::future<T>> predecessors,
        F&& continuation) const
        -> std::future<detail::dataflow_result_t<T, F>>;

    template <class T>
    auto when_all(std::vector<std::future<T>> futures) const
        -> std::future<std::vector<T>>;

    std::future<void> when_all(std::vector<std::future<void>> futures) const;

    // Uses the pool continuation scheduler and polls with 1ms granularity.
    template <class T>
    auto when_any(std::vector<std::future<T>> futures) const
        -> std::future<when_any_result<T>>;

    // Uses the pool continuation scheduler and polls with 1ms granularity.
    std::future<when_any_result<void>> when_any(std::vector<std::future<void>> futures) const;

#if UNIVERSAL_THREAD_POOL_HAS_COROUTINE
    schedule_awaiter schedule() const noexcept;

    template <class F, class... Args>
    auto submit_awaitable(F&& f, Args&&... args) const
        -> submit_awaiter<
            detail::bound_task<detail::bound_result_t<F, Args...>, F, Args...>>;

    template <class T>
    future_awaiter<T> await_future(std::future<T> future) const;
#endif

    template <class F, class... Args>
    bool detach(F&& f, Args&&... args) const;

    template <class F, class... Args>
    bool detach_with_options(task_options options, F&& f, Args&&... args) const;

    template <class F, class... Args>
    bool try_detach(F&& f, Args&&... args) const;

    template <class F, class... Args>
    bool try_detach_with_options(task_options options, F&& f, Args&&... args) const;

    template <class Rep, class Period, class F, class... Args>
    bool try_detach_for(std::chrono::duration<Rep, Period> timeout, F&& f, Args&&... args) const;

    template <class Rep, class Period, class F, class... Args>
    bool try_detach_with_options_for(
        task_options options,
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args) const;

    template <class Clock, class Duration, class F, class... Args>
    bool try_detach_until(
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args) const;

    template <class Clock, class Duration, class F, class... Args>
    bool try_detach_with_options_until(
        task_options options,
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args) const;

    template <class F, class... Args>
    bool detach_cancellable(cancellation_token token, F&& f, Args&&... args) const;

#if UNIVERSAL_THREAD_POOL_HAS_STOP_TOKEN
    template <class F, class... Args>
    bool detach_stop_token(std::stop_token token, F&& f, Args&&... args) const;
#endif

    template <class F>
    std::size_t bulk_detach(std::size_t count, F&& f) const;

    // Copies task_options for each item. A shared cancellation token cancels
    // the whole batch by design.
    template <class F>
    std::size_t bulk_detach_with_options(task_options options, std::size_t count, F&& f) const;

    template <class F>
    bool post(F&& f) const;

    template <class F>
    bool defer(F&& f) const;

    // May run inline on a worker from the same pool; that fast path skips
    // lifecycle hooks and latency histograms while preserving task counters.
    template <class F>
    bool dispatch(F&& f) const;

    template <class F>
    auto run_managed_blocking(F&& f) const -> std::invoke_result_t<F&&>;

    bool is_worker_thread() const noexcept;

    template <class T>
    void wait_with_help(std::future<T>& future) const;

    template <class T>
    T get_with_help(std::future<T> future) const;

    explicit operator bool() const noexcept {
        return pool_ != nullptr;
    }

private:
    friend class thread_pool;

    executor(thread_pool* pool, task_priority priority) noexcept
        : pool_(pool), priority_(priority) {}

    static void apply_default_priority(task_options& options, task_priority priority) {
        if (!options.priority) {
            options.priority = priority;
        }
    }

    thread_pool* pool_ = nullptr;
    task_priority priority_ = task_priority::normal;
};

} // namespace universal_thread_pool
