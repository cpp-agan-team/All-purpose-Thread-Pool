#pragma once

#include "universal_thread_pool/task_graph.hpp"

namespace universal_thread_pool {

struct thread_pool_runtime_options {
    thread_pool_options cpu = make_cpu_pool_options();
    thread_pool_options io = make_io_pool_options();
    thread_pool_options background = make_background_pool_options();
};

struct runtime_executors {
    executor cpu;
    executor io;
    executor background;
    executor latency_sensitive;
};

struct runtime_metrics {
    thread_pool_metrics cpu;
    thread_pool_metrics io;
    thread_pool_metrics background;

    std::uint64_t submitted_tasks_total() const noexcept {
        return cpu.submitted_tasks_total +
            io.submitted_tasks_total +
            background.submitted_tasks_total;
    }

    std::uint64_t completed_tasks_total() const noexcept {
        return cpu.completed_tasks_total +
            io.completed_tasks_total +
            background.completed_tasks_total;
    }

    std::uint64_t failed_tasks_total() const noexcept {
        return cpu.failed_tasks_total +
            io.failed_tasks_total +
            background.failed_tasks_total;
    }

    std::uint64_t rejected_tasks_total() const noexcept {
        return cpu.rejected_tasks_total +
            io.rejected_tasks_total +
            background.rejected_tasks_total;
    }

    std::size_t queued_tasks() const noexcept {
        return cpu.queued_tasks + io.queued_tasks + background.queued_tasks;
    }

    std::size_t active_tasks() const noexcept {
        return cpu.active_tasks + io.active_tasks + background.active_tasks;
    }

    std::size_t total_threads() const noexcept {
        return cpu.total_threads + io.total_threads + background.total_threads;
    }

    bool healthy() const noexcept {
        return cpu.healthy() && io.healthy() && background.healthy();
    }

    std::string to_prometheus(const std::string& runtime_name = "thread_pool_runtime") const {
        return universal_thread_pool::to_prometheus(cpu, runtime_name + "_cpu") +
            universal_thread_pool::to_prometheus(io, runtime_name + "_io") +
            universal_thread_pool::to_prometheus(background, runtime_name + "_background");
    }

    std::string to_json(const std::string& runtime_name = "thread_pool_runtime") const {
        return std::string("{\"runtime\":\"") + runtime_name + "\",\"cpu\":" +
            universal_thread_pool::to_json(cpu, runtime_name + "_cpu") +
            ",\"io\":" +
            universal_thread_pool::to_json(io, runtime_name + "_io") +
            ",\"background\":" +
            universal_thread_pool::to_json(background, runtime_name + "_background") +
            "}";
    }

    std::string to_opentelemetry_json(
        const std::string& runtime_name = "thread_pool_runtime") const {
        return std::string("{\"runtime\":\"") + runtime_name + "\",\"pools\":[" +
            universal_thread_pool::to_opentelemetry_json(cpu, runtime_name + "_cpu") +
            "," +
            universal_thread_pool::to_opentelemetry_json(io, runtime_name + "_io") +
            "," +
            universal_thread_pool::to_opentelemetry_json(
                background,
                runtime_name + "_background") +
            "]}";
    }
};

class thread_pool_runtime {
public:
    explicit thread_pool_runtime(thread_pool_runtime_options options = {});

    thread_pool& cpu_pool() noexcept;
    thread_pool& io_pool() noexcept;
    thread_pool& background_pool() noexcept;

    const thread_pool& cpu_pool() const noexcept;
    const thread_pool& io_pool() const noexcept;
    const thread_pool& background_pool() const noexcept;

    executor cpu_executor(task_priority priority = task_priority::normal) noexcept;
    executor io_executor(task_priority priority = task_priority::normal) noexcept;
    executor background_executor(task_priority priority = task_priority::low) noexcept;
    executor executor_for(task_kind kind) noexcept;
    runtime_executors executors() noexcept;

    template <class F, class... Args>
    auto submit(task_kind kind, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        return pool_for(kind).submit_with_priority(
            priority_for(kind),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto submit_with_options(task_options options, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        const auto kind = options.kind;
        apply_default_priority(options, kind);
        return pool_for(kind).submit_with_options(
            std::move(options),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto submit_retry(task_kind kind, retry_options retry, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<std::decay_t<F>&, std::decay_t<Args>&...>> {
        return pool_for(kind).submit_retry_with_priority(
            std::move(retry),
            priority_for(kind),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto submit_retry_with_options(
        retry_options retry,
        task_options options,
        F&& f,
        Args&&... args)
        -> std::future<std::invoke_result_t<std::decay_t<F>&, std::decay_t<Args>&...>> {
        const auto kind = options.kind;
        apply_default_priority(options, kind);
        return pool_for(kind).submit_retry_with_options(
            std::move(retry),
            std::move(options),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto try_submit(task_kind kind, F&& f, Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return pool_for(kind).try_submit_with_priority(
            priority_for(kind),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto try_submit_with_options(task_options options, F&& f, Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        const auto kind = options.kind;
        apply_default_priority(options, kind);
        return pool_for(kind).try_submit_with_options(
            std::move(options),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    auto try_submit_for(
        task_kind kind,
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return pool_for(kind).try_submit_with_priority_for(
            priority_for(kind),
            timeout,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    auto try_submit_with_options_for(
        task_options options,
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        const auto kind = options.kind;
        apply_default_priority(options, kind);
        return pool_for(kind).try_submit_with_options_for(
            std::move(options),
            timeout,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    auto try_submit_until(
        task_kind kind,
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return pool_for(kind).try_submit_with_priority_until(
            priority_for(kind),
            deadline,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    auto try_submit_with_options_until(
        task_options options,
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        const auto kind = options.kind;
        apply_default_priority(options, kind);
        return pool_for(kind).try_submit_with_options_until(
            std::move(options),
            deadline,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto submit_cpu(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        return submit(
            task_kind::cpu_bound,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto submit_retry_cpu(retry_options retry, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<std::decay_t<F>&, std::decay_t<Args>&...>> {
        return submit_retry(
            task_kind::cpu_bound,
            std::move(retry),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto try_submit_cpu(F&& f, Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return try_submit(
            task_kind::cpu_bound,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    auto try_submit_cpu_for(
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return try_submit_for(
            task_kind::cpu_bound,
            timeout,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto submit_io(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        return submit(
            task_kind::blocking_io,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto try_submit_io(F&& f, Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return try_submit(
            task_kind::blocking_io,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    auto try_submit_io_for(
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return try_submit_for(
            task_kind::blocking_io,
            timeout,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto submit_background(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        return submit(
            task_kind::background,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto try_submit_background(F&& f, Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return try_submit(
            task_kind::background,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    auto try_submit_background_for(
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return try_submit_for(
            task_kind::background,
            timeout,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    bool detach(task_kind kind, F&& f, Args&&... args) {
        return pool_for(kind).detach_with_priority(
            priority_for(kind),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    bool detach_with_options(task_options options, F&& f, Args&&... args) {
        const auto kind = options.kind;
        apply_default_priority(options, kind);
        return pool_for(kind).detach_with_options(
            std::move(options),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    bool try_detach(task_kind kind, F&& f, Args&&... args) {
        return pool_for(kind).try_detach_with_priority(
            priority_for(kind),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    bool try_detach_with_options(task_options options, F&& f, Args&&... args) {
        const auto kind = options.kind;
        apply_default_priority(options, kind);
        return pool_for(kind).try_detach_with_options(
            std::move(options),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    bool try_detach_for(
        task_kind kind,
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args) {
        return pool_for(kind).try_detach_with_priority_for(
            priority_for(kind),
            timeout,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    bool try_detach_until(
        task_kind kind,
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args) {
        return pool_for(kind).try_detach_with_priority_until(
            priority_for(kind),
            deadline,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    bool try_detach_with_options_for(
        task_options options,
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args) {
        const auto kind = options.kind;
        apply_default_priority(options, kind);
        return pool_for(kind).try_detach_with_options_for(
            std::move(options),
            timeout,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    bool try_detach_with_options_until(
        task_options options,
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args) {
        const auto kind = options.kind;
        apply_default_priority(options, kind);
        return pool_for(kind).try_detach_with_options_until(
            std::move(options),
            deadline,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    bool detach_cpu(F&& f, Args&&... args) {
        return detach(
            task_kind::cpu_bound,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    bool try_detach_cpu(F&& f, Args&&... args) {
        return try_detach(
            task_kind::cpu_bound,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    bool try_detach_cpu_for(
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args) {
        return try_detach_for(
            task_kind::cpu_bound,
            timeout,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    bool try_detach_cpu_until(
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args) {
        return try_detach_until(
            task_kind::cpu_bound,
            deadline,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    bool detach_io(F&& f, Args&&... args) {
        return detach(
            task_kind::blocking_io,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    bool try_detach_io(F&& f, Args&&... args) {
        return try_detach(
            task_kind::blocking_io,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    bool try_detach_io_for(
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args) {
        return try_detach_for(
            task_kind::blocking_io,
            timeout,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    bool try_detach_io_until(
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args) {
        return try_detach_until(
            task_kind::blocking_io,
            deadline,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    bool detach_background(F&& f, Args&&... args) {
        return detach(
            task_kind::background,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    bool try_detach_background(F&& f, Args&&... args) {
        return try_detach(
            task_kind::background,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    auto schedule_submit_after(
        task_kind kind,
        std::chrono::duration<Rep, Period> delay,
        F&& f,
        Args&&... args)
        -> scheduled_future<std::invoke_result_t<F, Args...>> {
        return pool_for(kind).schedule_submit_after_with_priority(
            priority_for(kind),
            delay,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    periodic_task_handle detach_periodic(
        task_kind kind,
        std::chrono::duration<Rep, Period> interval,
        F&& f,
        Args&&... args) {
        periodic_options periodic;
        periodic.interval = std::chrono::duration_cast<std::chrono::nanoseconds>(interval);
        task_options options;
        options.kind = kind;
        apply_default_priority(options, kind);
        return pool_for(kind).detach_periodic_with_options(
            std::move(periodic),
            std::move(options),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    periodic_task_handle detach_periodic_with_options(
        periodic_options periodic,
        task_options options,
        F&& f,
        Args&&... args) {
        const auto kind = options.kind;
        apply_default_priority(options, kind);
        return pool_for(kind).detach_periodic_with_options(
            std::move(periodic),
            std::move(options),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    auto schedule_submit_at(
        task_kind kind,
        std::chrono::time_point<Clock, Duration> time_point,
        F&& f,
        Args&&... args)
        -> scheduled_future<std::invoke_result_t<F, Args...>> {
        return pool_for(kind).schedule_submit_at_with_priority(
            priority_for(kind),
            time_point,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    auto schedule_submit_cpu_after(
        std::chrono::duration<Rep, Period> delay,
        F&& f,
        Args&&... args)
        -> scheduled_future<std::invoke_result_t<F, Args...>> {
        return schedule_submit_after(
            task_kind::cpu_bound,
            delay,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    auto schedule_submit_cpu_at(
        std::chrono::time_point<Clock, Duration> time_point,
        F&& f,
        Args&&... args)
        -> scheduled_future<std::invoke_result_t<F, Args...>> {
        return schedule_submit_at(
            task_kind::cpu_bound,
            time_point,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    auto schedule_submit_io_after(
        std::chrono::duration<Rep, Period> delay,
        F&& f,
        Args&&... args)
        -> scheduled_future<std::invoke_result_t<F, Args...>> {
        return schedule_submit_after(
            task_kind::blocking_io,
            delay,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    auto schedule_submit_io_at(
        std::chrono::time_point<Clock, Duration> time_point,
        F&& f,
        Args&&... args)
        -> scheduled_future<std::invoke_result_t<F, Args...>> {
        return schedule_submit_at(
            task_kind::blocking_io,
            time_point,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    auto schedule_submit_background_after(
        std::chrono::duration<Rep, Period> delay,
        F&& f,
        Args&&... args)
        -> scheduled_future<std::invoke_result_t<F, Args...>> {
        return schedule_submit_after(
            task_kind::background,
            delay,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    auto schedule_submit_background_at(
        std::chrono::time_point<Clock, Duration> time_point,
        F&& f,
        Args&&... args)
        -> scheduled_future<std::invoke_result_t<F, Args...>> {
        return schedule_submit_at(
            task_kind::background,
            time_point,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    auto schedule_submit_after_with_options(
        task_options options,
        std::chrono::duration<Rep, Period> delay,
        F&& f,
        Args&&... args)
        -> scheduled_future<std::invoke_result_t<F, Args...>> {
        const auto kind = options.kind;
        apply_default_priority(options, kind);
        return pool_for(kind).schedule_submit_after_with_options(
            std::move(options),
            delay,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    auto schedule_submit_at_with_options(
        task_options options,
        std::chrono::time_point<Clock, Duration> time_point,
        F&& f,
        Args&&... args)
        -> scheduled_future<std::invoke_result_t<F, Args...>> {
        const auto kind = options.kind;
        apply_default_priority(options, kind);
        return pool_for(kind).schedule_submit_at_with_options(
            std::move(options),
            time_point,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    bool try_detach_background_for(
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args) {
        return try_detach_for(
            task_kind::background,
            timeout,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    bool try_detach_background_until(
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args) {
        return try_detach_until(
            task_kind::background,
            deadline,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class T, class F>
    auto continue_with(task_kind kind, std::future<T> predecessor, F&& continuation)
        -> std::future<detail::continuation_result_t<T, F>> {
        return pool_for(kind).continue_with_priority(
            priority_for(kind),
            std::move(predecessor),
            std::forward<F>(continuation));
    }

    template <class T, class F>
    auto continue_cpu(std::future<T> predecessor, F&& continuation)
        -> std::future<detail::continuation_result_t<T, F>> {
        return continue_with(
            task_kind::cpu_bound,
            std::move(predecessor),
            std::forward<F>(continuation));
    }

    template <class T, class F>
    auto continue_io(std::future<T> predecessor, F&& continuation)
        -> std::future<detail::continuation_result_t<T, F>> {
        return continue_with(
            task_kind::blocking_io,
            std::move(predecessor),
            std::forward<F>(continuation));
    }

    template <class T, class F>
    auto continue_background(std::future<T> predecessor, F&& continuation)
        -> std::future<detail::continuation_result_t<T, F>> {
        return continue_with(
            task_kind::background,
            std::move(predecessor),
            std::forward<F>(continuation));
    }

    template <class T, class F>
    auto dataflow(task_kind kind, std::vector<std::future<T>> predecessors, F&& continuation)
        -> std::future<detail::dataflow_result_t<T, F>> {
        return pool_for(kind).dataflow_with_priority(
            priority_for(kind),
            std::move(predecessors),
            std::forward<F>(continuation));
    }

    template <class T, class F>
    auto dataflow_cpu(std::vector<std::future<T>> predecessors, F&& continuation)
        -> std::future<detail::dataflow_result_t<T, F>> {
        return dataflow(
            task_kind::cpu_bound,
            std::move(predecessors),
            std::forward<F>(continuation));
    }

    template <class T, class F>
    auto dataflow_io(std::vector<std::future<T>> predecessors, F&& continuation)
        -> std::future<detail::dataflow_result_t<T, F>> {
        return dataflow(
            task_kind::blocking_io,
            std::move(predecessors),
            std::forward<F>(continuation));
    }

    template <class T, class F>
    auto dataflow_background(std::vector<std::future<T>> predecessors, F&& continuation)
        -> std::future<detail::dataflow_result_t<T, F>> {
        return dataflow(
            task_kind::background,
            std::move(predecessors),
            std::forward<F>(continuation));
    }

    template <class T, class F>
    auto continue_on(
        task_kind waiter_kind,
        task_kind completion_kind,
        std::future<T> predecessor,
        F&& continuation)
        -> std::future<detail::continuation_result_t<T, F>> {
        return pool_for(waiter_kind).continue_on(
            executor_for(completion_kind),
            std::move(predecessor),
            std::forward<F>(continuation));
    }

    template <class T, class F>
    auto dataflow_on(
        task_kind waiter_kind,
        task_kind completion_kind,
        std::vector<std::future<T>> predecessors,
        F&& continuation)
        -> std::future<detail::dataflow_result_t<T, F>> {
        return pool_for(waiter_kind).dataflow_on(
            executor_for(completion_kind),
            std::move(predecessors),
            std::forward<F>(continuation));
    }

    template <class F>
    auto bulk_submit(task_kind kind, std::size_t count, F&& f)
        -> std::vector<std::future<std::invoke_result_t<std::decay_t<F>&, std::size_t>>> {
        return pool_for(kind).bulk_submit_with_priority(
            priority_for(kind),
            count,
            std::forward<F>(f));
    }

    // Copies task_options for each routed item. The cancellation token remains
    // shared, so cancelling it intentionally cancels the whole routed batch.
    template <class F>
    auto bulk_submit_with_options(task_options options, std::size_t count, F&& f)
        -> std::vector<std::future<std::invoke_result_t<std::decay_t<F>&, std::size_t>>> {
        const auto kind = options.kind;
        apply_default_priority(options, kind);
        return pool_for(kind).bulk_submit_with_options(
            std::move(options),
            count,
            std::forward<F>(f));
    }

    template <class F>
    auto bulk_submit_cpu(std::size_t count, F&& f)
        -> std::vector<std::future<std::invoke_result_t<std::decay_t<F>&, std::size_t>>> {
        return bulk_submit(
            task_kind::cpu_bound,
            count,
            std::forward<F>(f));
    }

    template <class F>
    auto bulk_submit_cpu_with_options(task_options options, std::size_t count, F&& f)
        -> std::vector<std::future<std::invoke_result_t<std::decay_t<F>&, std::size_t>>> {
        options.kind = task_kind::cpu_bound;
        return bulk_submit_with_options(
            std::move(options),
            count,
            std::forward<F>(f));
    }

    template <class F>
    std::size_t bulk_detach(task_kind kind, std::size_t count, F&& f) {
        return pool_for(kind).bulk_detach_with_priority(
            priority_for(kind),
            count,
            std::forward<F>(f));
    }

    // Copies task_options for each routed item. The cancellation token remains
    // shared, so cancelling it intentionally cancels the whole routed batch.
    template <class F>
    std::size_t bulk_detach_with_options(task_options options, std::size_t count, F&& f) {
        const auto kind = options.kind;
        apply_default_priority(options, kind);
        return pool_for(kind).bulk_detach_with_options(
            std::move(options),
            count,
            std::forward<F>(f));
    }

    template <class F>
    std::size_t bulk_detach_cpu(std::size_t count, F&& f) {
        return bulk_detach(
            task_kind::cpu_bound,
            count,
            std::forward<F>(f));
    }

    template <class F>
    std::size_t bulk_detach_cpu_with_options(task_options options, std::size_t count, F&& f) {
        options.kind = task_kind::cpu_bound;
        return bulk_detach_with_options(
            std::move(options),
            count,
            std::forward<F>(f));
    }

    template <class Index, class F>
    void parallel_for(Index first, Index last, F&& f, loop_options options = {}) {
        cpu_pool_.parallel_for(first, last, std::forward<F>(f), options);
    }

    template <class Iterator, class F>
    void parallel_for_each(Iterator first, Iterator last, F&& f, loop_options options = {}) {
        cpu_pool_.parallel_for_each(first, last, std::forward<F>(f), options);
    }

    template <class InputIterator, class OutputIterator, class F>
    void parallel_transform(
        InputIterator first,
        InputIterator last,
        OutputIterator output,
        F&& f,
        loop_options options = {}) {
        cpu_pool_.parallel_transform(
            first,
            last,
            output,
            std::forward<F>(f),
            options);
    }

    template <class Index, class F>
    void parallel_for_2d(
        Index x_first,
        Index x_last,
        Index y_first,
        Index y_last,
        F&& f,
        loop_options options = {}) {
        cpu_pool_.parallel_for_2d(
            x_first,
            x_last,
            y_first,
            y_last,
            std::forward<F>(f),
            options);
    }

    template <class Index, class F>
    void parallel_for_3d(
        Index x_first,
        Index x_last,
        Index y_first,
        Index y_last,
        Index z_first,
        Index z_last,
        F&& f,
        loop_options options = {}) {
        cpu_pool_.parallel_for_3d(
            x_first,
            x_last,
            y_first,
            y_last,
            z_first,
            z_last,
            std::forward<F>(f),
            options);
    }

    template <class Index, std::size_t Dimensions, class F>
    void parallel_for_nd(
        const std::array<Index, Dimensions>& first,
        const std::array<Index, Dimensions>& last,
        F&& f,
        loop_options options = {}) {
        cpu_pool_.parallel_for_nd<Index, Dimensions>(
            first,
            last,
            std::forward<F>(f),
            options);
    }

    template <class Index, class Value, class Reduce>
    Value parallel_reduce(
        Index first,
        Index last,
        Value init,
        Reduce&& reduce,
        loop_options options = {}) {
        return cpu_pool_.parallel_reduce(
            first,
            last,
            std::move(init),
            std::forward<Reduce>(reduce),
            options);
    }

    template <class Index, class Value, class Map, class Reduce>
    Value parallel_transform_reduce(
        Index first,
        Index last,
        Value init,
        Map&& map,
        Reduce&& reduce,
        loop_options options = {}) {
        return cpu_pool_.parallel_transform_reduce(
            first,
            last,
            std::move(init),
            std::forward<Map>(map),
            std::forward<Reduce>(reduce),
            options);
    }

    void pause_all();
    void resume_all();
    void shutdown(shutdown_policy policy = shutdown_policy::drain);
    // Waits for ready/running work in each pool to drain. Delayed tasks
    // scheduled for the future are not busy until their timer promotes them.
    bool wait_for_idle(std::chrono::nanoseconds timeout);

    template <class Rep, class Period>
    bool wait_for_idle_for(std::chrono::duration<Rep, Period> timeout) {
        return wait_for_idle(std::chrono::duration_cast<std::chrono::nanoseconds>(timeout));
    }

    template <class Clock, class Duration>
    bool wait_for_idle_until(std::chrono::time_point<Clock, Duration> deadline) {
        const auto now = Clock::now();
        if (deadline <= now) {
            return wait_for_idle(std::chrono::nanoseconds{0});
        }
        return wait_for_idle(
            std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now));
    }

    void join() noexcept;
    runtime_metrics metrics() const;

private:
    static task_priority priority_for(task_kind kind) noexcept;
    static void apply_default_priority(task_options& options, task_kind kind) {
        if (!options.priority) {
            options.priority = priority_for(kind);
        }
    }

    thread_pool& pool_for(task_kind kind) noexcept;

    thread_pool cpu_pool_;
    thread_pool io_pool_;
    thread_pool background_pool_;
};

thread_pool_runtime& global_runtime();

} // namespace universal_thread_pool
