#include "universal_thread_pool/thread_pool_core.hpp"

#if defined(__linux__) || defined(__APPLE__)
#include <pthread.h>
#endif

#if defined(__linux__)
#include <sched.h>
#endif

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace universal_thread_pool {

thread_local thread_pool* thread_pool::current_pool_ = nullptr;
thread_local std::size_t thread_pool::current_worker_id_ = static_cast<std::size_t>(-1);
thread_local thread_pool::worker_control* thread_pool::current_worker_ = nullptr;

thread_pool& global_thread_pool() {
    static thread_pool pool;
    return pool;
}

thread_pool::thread_pool(thread_pool_options options)
    : options_(normalize_options(std::move(options))) {
    const auto initial_threads = options_.initial_threads;
    target_threads_ =
        options_.mode == pool_mode::cached ? options_.min_threads : initial_threads;
    for (std::size_t i = 0; i < initial_threads; ++i) {
        start_worker_unlocked();
    }
    start_continuation_workers_unlocked();

    timer_thread_ = std::thread([this] {
        set_current_thread_name(make_thread_name("timer", 0, options_));
        timer_loop();
    });
}

thread_pool::~thread_pool() {
    shutdown(shutdown_policy::drain);
    join();
}

std::future<void> thread_pool::when_all(std::vector<std::future<void>> futures) {
    auto promise = std::make_shared<std::promise<void>>();
    auto result = promise->get_future();

    start_continuation_waiter(
        [futures = std::move(futures), promise]() mutable {
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
                set_promise_exception<void>(promise, first_error);
                return;
            }

            try {
                promise->set_value();
            } catch (...) {
                set_promise_exception<void>(promise, std::current_exception());
            }
        });

    return result;
}

std::future<when_any_result<void>> thread_pool::when_any(std::vector<std::future<void>> futures) {
    if (futures.empty()) {
        throw std::invalid_argument("when_any requires at least one future");
    }

    auto promise = std::make_shared<std::promise<when_any_result<void>>>();
    auto result = promise->get_future();

    start_continuation_waiter(
        [futures = std::move(futures), promise]() mutable {
            try {
                while (true) {
                    bool has_valid_future = false;
                    for (std::size_t i = 0; i < futures.size(); ++i) {
                        if (!futures[i].valid()) {
                            continue;
                        }

                        has_valid_future = true;
                        if (futures[i].wait_for(std::chrono::nanoseconds{0}) ==
                            std::future_status::ready) {
                            futures[i].get();
                            promise->set_value(when_any_result<void>{i});
                            return;
                        }
                    }

                    if (!has_valid_future) {
                        throw std::invalid_argument("when_any requires at least one valid future");
                    }

                    std::this_thread::sleep_for(std::chrono::milliseconds{1});
                }
            } catch (...) {
                set_promise_exception<when_any_result<void>>(
                    promise,
                    std::current_exception());
            }
        });

    return result;
}

executor thread_pool::get_executor(task_priority priority) noexcept {
    return executor{this, priority};
}

bool thread_pool::is_worker_thread() const noexcept {
    return current_pool_ == this;
}

std::size_t thread_pool::current_worker_id() noexcept {
    return current_worker_id_;
}

thread_pool::managed_blocking_scope::managed_blocking_scope(thread_pool& pool) noexcept
    : pool_(pool.begin_managed_blocking() ? &pool : nullptr) {}

thread_pool::managed_blocking_scope::~managed_blocking_scope() {
    if (pool_) {
        pool_->end_managed_blocking();
    }
}

thread_pool::managed_blocking_scope::managed_blocking_scope(
    managed_blocking_scope&& other) noexcept
    : pool_(other.pool_) {
    other.pool_ = nullptr;
}

thread_pool::managed_blocking_scope& thread_pool::managed_blocking_scope::operator=(
    managed_blocking_scope&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    if (pool_) {
        pool_->end_managed_blocking();
    }
    pool_ = other.pool_;
    other.pool_ = nullptr;
    return *this;
}

thread_pool::managed_blocking_scope thread_pool::managed_blocking() noexcept {
    return managed_blocking_scope(*this);
}

void thread_pool::pause() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            return;
        }
        paused_ = true;
    }
}

void thread_pool::resume() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            return;
        }
        paused_ = false;
    }
    task_cv_.notify_all();
}

void thread_pool::resize(std::size_t new_thread_count) {
    if (new_thread_count == 0) {
        new_thread_count = 1;
    }

    new_thread_count = std::min(new_thread_count, options_.max_threads);
    new_thread_count = std::max(new_thread_count, options_.min_threads);

    bool should_notify = false;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        target_threads_ = new_thread_count;
        const auto current = total_threads_.load(std::memory_order_acquire);
        const auto reserved = std::min(current, resize_retirements_ + idle_retirements_);
        const auto effective_threads = current - reserved;
        retire_requests_ =
            effective_threads > new_thread_count ? effective_threads - new_thread_count : 0;
        should_notify = retire_requests_ > 0;
    }

    {
        std::lock_guard<std::mutex> workers_lock(workers_mutex_);
        join_finished_workers_unlocked();

        auto live_threads = total_threads_.load(std::memory_order_acquire);
        while (live_threads < new_thread_count && live_threads < options_.max_threads) {
            start_worker_unlocked();
            ++live_threads;
            should_notify = true;
        }
    }

    if (should_notify) {
        task_cv_.notify_all();
    }
}

void thread_pool::shutdown(shutdown_policy policy) {
    std::size_t cancelled_delayed_tasks = 0;
    std::vector<task_metadata> cancelled_metadata;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            return;
        }

        accepting_ = false;
        stopping_ = true;
        paused_ = false;

        if (policy == shutdown_policy::cancel_pending ||
            policy == shutdown_policy::stop_immediately) {
            clear_queues_locked(cancelled_metadata);
        }

        if (policy == shutdown_policy::stop_immediately) {
            stop_immediately_ = true;
        }
    }

    {
        std::lock_guard<std::mutex> delay_lock(delay_mutex_);
        timer_stopping_ = true;
        cancelled_delayed_tasks = delayed_tasks_.size();
        for (auto& entry : delayed_tasks_) {
            cancel_pending_item(entry.second.item, cancelled_metadata);
        }
        delayed_tasks_.clear();
    }

    if (cancelled_delayed_tasks > 0) {
        scheduled_tasks_cancelled_total_.fetch_add(cancelled_delayed_tasks, std::memory_order_relaxed);
    }

    for (const auto& metadata : cancelled_metadata) {
        notify_task_cancel(metadata);
    }

    task_cv_.notify_all();
    queue_space_cv_.notify_all();
    idle_cv_.notify_all();
    delay_cv_.notify_all();
}

bool thread_pool::wait_for_idle(std::chrono::nanoseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    return idle_cv_.wait_for(lock, timeout, [this] {
        return queued_size_ == 0 && active_tasks_ == 0;
    });
}

void thread_pool::join() noexcept {
    bool needs_shutdown = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        needs_shutdown = !stopping_;
    }

    if (needs_shutdown) {
        shutdown(shutdown_policy::drain);
    }

    if (timer_thread_.joinable()) {
        if (timer_thread_.get_id() == std::this_thread::get_id()) {
            timer_thread_.detach();
        } else {
            timer_thread_.join();
        }
    }

    std::vector<std::unique_ptr<worker_control>> workers;
    {
        std::lock_guard<std::mutex> workers_lock(workers_mutex_);
        workers.swap(workers_);
    }

    for (auto& worker : workers) {
        if (worker->thread.joinable()) {
            if (worker->thread.get_id() == std::this_thread::get_id()) {
                worker->thread.detach();
            } else {
                worker->thread.join();
            }
        }
    }

    stop_continuation_workers();

    std::lock_guard<std::mutex> lock(mutex_);
    accepting_ = false;
    stopping_ = true;
}

thread_pool_metrics thread_pool::metrics() const {
    thread_pool_metrics snapshot;
    snapshot.submitted_tasks_total = submitted_tasks_total_.load(std::memory_order_relaxed);
    snapshot.completed_tasks_total = completed_tasks_total_.load(std::memory_order_relaxed);
    snapshot.failed_tasks_total = failed_tasks_total_.load(std::memory_order_relaxed);
    snapshot.cancelled_tasks_total = cancelled_tasks_total_.load(std::memory_order_relaxed);
    snapshot.rejected_tasks_total = rejected_tasks_total_.load(std::memory_order_relaxed);
    snapshot.caller_runs_total = caller_runs_total_.load(std::memory_order_relaxed);
    snapshot.scheduled_tasks_total = scheduled_tasks_total_.load(std::memory_order_relaxed);
    snapshot.scheduled_tasks_cancelled_total =
        scheduled_tasks_cancelled_total_.load(std::memory_order_relaxed);
    snapshot.local_tasks_total = local_tasks_total_.load(std::memory_order_relaxed);
    snapshot.steal_success_total = steal_success_total_.load(std::memory_order_relaxed);
    snapshot.steal_fail_total = steal_fail_total_.load(std::memory_order_relaxed);
    snapshot.priority_fairness_picks_total =
        priority_fairness_picks_total_.load(std::memory_order_relaxed);
    snapshot.managed_blocking_total =
        managed_blocking_total_.load(std::memory_order_relaxed);
    snapshot.managed_blocking_compensations_total =
        managed_blocking_compensations_total_.load(std::memory_order_relaxed);
    snapshot.thread_affinity_applied_total =
        thread_affinity_applied_total_.load(std::memory_order_relaxed);
    snapshot.thread_affinity_failed_total =
        thread_affinity_failed_total_.load(std::memory_order_relaxed);
    snapshot.worker_wakeup_total = worker_wakeup_total_.load(std::memory_order_relaxed);
    snapshot.worker_idle_timeout_total =
        worker_idle_timeout_total_.load(std::memory_order_relaxed);
    snapshot.worker_retired_total = worker_retired_total_.load(std::memory_order_relaxed);
    snapshot.task_wait_time_ns_total = task_wait_time_ns_total_.load(std::memory_order_relaxed);
    snapshot.task_run_time_ns_total = task_run_time_ns_total_.load(std::memory_order_relaxed);
    for (std::size_t i = 0; i < metric_latency_bucket_count; ++i) {
        snapshot.task_wait_time_buckets[i] =
            task_wait_time_buckets_[i].load(std::memory_order_relaxed);
        snapshot.task_run_time_buckets[i] =
            task_run_time_buckets_[i].load(std::memory_order_relaxed);
    }
    snapshot.max_queue_size_seen = max_queue_size_seen_.load(std::memory_order_relaxed);
    snapshot.running_tasks = running_tasks_.load(std::memory_order_relaxed);
    snapshot.blocked_workers = blocked_workers_.load(std::memory_order_relaxed);
    snapshot.total_threads = total_threads_.load(std::memory_order_relaxed);
    snapshot.idle_threads = snapshot.total_threads > snapshot.running_tasks
        ? snapshot.total_threads - snapshot.running_tasks
        : 0;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        snapshot.queued_tasks = queued_size_;
        snapshot.active_tasks = active_tasks_;
        snapshot.accepting = accepting_;
        snapshot.paused = paused_;
        snapshot.stopping = stopping_;
        if (stopping_ && total_threads_.load(std::memory_order_relaxed) == 0) {
            snapshot.state = thread_pool_state::stopped;
        } else if (stopping_) {
            snapshot.state = thread_pool_state::stopping;
        } else if (paused_) {
            snapshot.state = thread_pool_state::paused;
        } else {
            snapshot.state = thread_pool_state::running;
        }
    }

    {
        std::lock_guard<std::mutex> lock(delay_mutex_);
        snapshot.delayed_tasks = delayed_tasks_.size();
    }

    {
        std::lock_guard<std::mutex> workers_lock(workers_mutex_);
        snapshot.workers.reserve(workers_.size());
        for (const auto& worker : workers_) {
            worker_metrics metrics;
            metrics.id = worker->id;
            metrics.numa_node = worker->numa_node;
            metrics.completed_tasks_total =
                worker->completed_tasks.load(std::memory_order_relaxed);
            metrics.failed_tasks_total =
                worker->failed_tasks.load(std::memory_order_relaxed);
            metrics.cancelled_tasks_total =
                worker->cancelled_tasks.load(std::memory_order_relaxed);
            metrics.finished = worker->finished.load(std::memory_order_acquire);
            {
                std::lock_guard<std::mutex> local_lock(worker->local_mutex);
                metrics.local_queue_size = worker->local_queue.size();
            }
            snapshot.workers.push_back(metrics);
        }
    }

    return snapshot;
}

std::size_t thread_pool::thread_count() const noexcept {
    return total_threads_.load(std::memory_order_acquire);
}

thread_pool_options thread_pool::normalize_options(thread_pool_options options) {
    if (options.name.empty()) {
        options.name = "thread_pool";
    }
    if (options.mode == pool_mode::cached && options.initial_threads == 0) {
        options.initial_threads = options.min_threads;
    } else if (options.initial_threads == 0) {
        options.initial_threads = default_thread_count();
    }
    if (options.mode == pool_mode::cached && options.max_threads == 0) {
        options.max_threads = std::max<std::size_t>(1, default_thread_count());
    } else if (options.max_threads == 0) {
        options.max_threads = options.initial_threads;
    }
    if (options.min_threads > options.max_threads) {
        options.min_threads = options.max_threads;
    }
    if (options.initial_threads < options.min_threads) {
        options.initial_threads = options.min_threads;
    }
    if (options.initial_threads > options.max_threads) {
        options.max_threads = options.initial_threads;
    }
    if (options.queue != queue_policy::unbounded && options.max_queue_size == 0) {
        options.max_queue_size = 1;
    }
    if (options.worker_batch_size == 0) {
        options.worker_batch_size = 1;
    }
    if (options.continuation_threads == 0) {
        options.continuation_threads = 1;
    }
    if (options.numa_nodes == 0) {
        options.numa_nodes = 1;
    }
    if (options.scheduler == schedule_policy::priority) {
        options.enable_priority = true;
    }
    if (options.scheduler == schedule_policy::work_stealing ||
        options.mode == pool_mode::work_stealing) {
        options.enable_work_stealing = true;
        options.scheduler = schedule_policy::work_stealing;
        options.mode = pool_mode::work_stealing;
    }
    if (options.mode == pool_mode::cached && options.min_threads > options.initial_threads) {
        options.initial_threads = options.min_threads;
    }
    if (options.mode == pool_mode::cached && options.initial_threads > options.max_threads) {
        options.max_threads = options.initial_threads;
    }
    return options;
}

scheduled_task_handle thread_pool::schedule_task_at(
    clock_type::time_point due_time,
    task_item item,
    bool throw_on_failure) {
    auto state = std::make_shared<scheduled_task_state>();
    scheduled_task_handle handle{state};

    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!accepting_) {
            rejected_tasks_total_.fetch_add(1, std::memory_order_relaxed);
            if (throw_on_failure) {
                throw thread_pool_closed("thread pool is not accepting delayed tasks");
            }
            return scheduled_task_handle{};
        }

        assign_task_metadata_locked(item);

        {
            std::lock_guard<std::mutex> delay_lock(delay_mutex_);
            delayed_tasks_.emplace(
                due_time,
                delayed_task{std::move(item), state});
            scheduled_tasks_total_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    delay_cv_.notify_one();
    return handle;
}

void thread_pool::enqueue_or_throw(task_item item) {
    if (!enqueue_impl(std::move(item), true, true, std::nullopt)) {
        throw task_rejected("task was rejected by thread pool");
    }
}

bool thread_pool::enqueue_or_false(task_item item) {
    return enqueue_impl(std::move(item), false, true, std::nullopt);
}

bool thread_pool::try_enqueue(task_item item) {
    return enqueue_impl(std::move(item), false, false, std::nullopt);
}

bool thread_pool::try_enqueue_until(task_item item, clock_type::time_point deadline) {
    return enqueue_impl(std::move(item), false, true, deadline);
}

bool thread_pool::enqueue_impl(
    task_item item,
    bool throw_on_failure,
    bool allow_block,
    std::optional<clock_type::time_point> wait_deadline) {
    bool run_inline = false;
    bool enqueued = false;
    std::optional<task_metadata> queued_metadata;
    std::vector<task_metadata> cancelled_metadata;
    auto cancel_unqueued = [this, &item]() noexcept {
        try {
            if (item.on_cancel) {
                item.on_cancel();
            }
        } catch (...) {
            handle_unhandled_exception(std::current_exception());
        }
    };

    {
        std::unique_lock<std::mutex> lock(mutex_);

        if (!accepting_) {
            rejected_tasks_total_.fetch_add(1, std::memory_order_relaxed);
            cancel_unqueued();
            if (throw_on_failure) {
                throw thread_pool_closed("thread pool is not accepting new tasks");
            }
            return false;
        }

        const auto bounded = options_.queue != queue_policy::unbounded;
        if (bounded && options_.queue == queue_policy::bounded_block) {
            if (!allow_block && queued_size_ >= options_.max_queue_size) {
                rejected_tasks_total_.fetch_add(1, std::memory_order_relaxed);
                cancel_unqueued();
                return false;
            }

            if (wait_deadline) {
                const auto ready = queue_space_cv_.wait_until(lock, *wait_deadline, [this] {
                    return !accepting_ || queued_size_ < options_.max_queue_size;
                });
                if (!ready && queued_size_ >= options_.max_queue_size) {
                    rejected_tasks_total_.fetch_add(1, std::memory_order_relaxed);
                    cancel_unqueued();
                    return false;
                }
            } else {
                queue_space_cv_.wait(lock, [this] {
                    return !accepting_ || queued_size_ < options_.max_queue_size;
                });
            }

            if (!accepting_) {
                rejected_tasks_total_.fetch_add(1, std::memory_order_relaxed);
                cancel_unqueued();
                if (throw_on_failure) {
                    throw thread_pool_closed("thread pool stopped while waiting for queue space");
                }
                return false;
            }
        }

        if (bounded && queued_size_ >= options_.max_queue_size) {
            switch (options_.queue) {
            case queue_policy::bounded_reject:
                rejected_tasks_total_.fetch_add(1, std::memory_order_relaxed);
                cancel_unqueued();
                if (throw_on_failure) {
                    throw task_rejected("thread pool queue is full");
                }
                return false;
            case queue_policy::bounded_caller_runs:
                run_inline = true;
                caller_runs_total_.fetch_add(1, std::memory_order_relaxed);
                break;
            case queue_policy::bounded_drop_oldest:
                drop_oldest_locked(cancelled_metadata);
                break;
            case queue_policy::bounded_block:
            case queue_policy::unbounded:
                break;
            }
        }

        assign_task_metadata_locked(item);
        submitted_tasks_total_.fetch_add(1, std::memory_order_relaxed);

        if (!run_inline) {
            queued_metadata = item.metadata;
            if (should_use_local_queue_locked(item)) {
                push_local_task_locked(current_worker_, std::move(item));
            } else {
                push_task_locked(std::move(item));
            }
            maybe_grow_for_pending_task_locked();
            task_cv_.notify_one();
            enqueued = true;
        } else {
            ++active_tasks_;
        }
    }

    if (queued_metadata) {
        notify_task_queued(*queued_metadata);
    }
    for (const auto& metadata : cancelled_metadata) {
        notify_task_cancel(metadata);
    }
    if (enqueued) {
        return true;
    }

    execute_task(item);
    finish_active_task();
    return true;
}

void thread_pool::enqueue_many_or_throw(std::vector<task_item> items) {
    const auto requested = items.size();
    const auto accepted = enqueue_many_impl(std::move(items), true);
    if (accepted != requested) {
        throw task_rejected("one or more bulk tasks were rejected by thread pool");
    }
}

std::size_t thread_pool::enqueue_many_or_count(std::vector<task_item> items) {
    return enqueue_many_impl(std::move(items), false);
}

std::size_t thread_pool::enqueue_many_impl(
    std::vector<task_item> items,
    bool throw_on_failure) {
    if (items.empty()) {
        return 0;
    }

    std::size_t accepted = 0;
    std::size_t enqueued = 0;
    bool closed_failure = false;
    bool capacity_failure = false;
    std::vector<task_metadata> queued_metadata;
    std::vector<task_metadata> cancelled_metadata;

    auto cancel_unqueued = [this](task_item& item) noexcept {
        try {
            if (item.on_cancel) {
                item.on_cancel();
            }
        } catch (...) {
            handle_unhandled_exception(std::current_exception());
        }
    };

    {
        std::unique_lock<std::mutex> lock(mutex_);
        const auto reject_remaining = [&](std::size_t first) {
            for (std::size_t j = first; j < items.size(); ++j) {
                rejected_tasks_total_.fetch_add(1, std::memory_order_relaxed);
                cancel_unqueued(items[j]);
            }
        };

        bool abort_bulk = false;
        for (std::size_t i = 0; i < items.size() && !abort_bulk; ++i) {
            auto& item = items[i];
            if (!accepting_) {
                rejected_tasks_total_.fetch_add(1, std::memory_order_relaxed);
                cancel_unqueued(item);
                closed_failure = true;
                if (throw_on_failure) {
                    reject_remaining(i + 1);
                    break;
                }
                continue;
            }

            const auto bounded = options_.queue != queue_policy::unbounded;
            if (bounded && queued_size_ >= options_.max_queue_size) {
                switch (options_.queue) {
                case queue_policy::bounded_reject:
                    rejected_tasks_total_.fetch_add(1, std::memory_order_relaxed);
                    cancel_unqueued(item);
                    capacity_failure = true;
                    if (throw_on_failure) {
                        reject_remaining(i + 1);
                        abort_bulk = true;
                    }
                    continue;
                case queue_policy::bounded_drop_oldest:
                    drop_oldest_locked(cancelled_metadata);
                    break;
                case queue_policy::bounded_block:
                case queue_policy::bounded_caller_runs:
                    rejected_tasks_total_.fetch_add(1, std::memory_order_relaxed);
                    cancel_unqueued(item);
                    capacity_failure = true;
                    if (throw_on_failure) {
                        reject_remaining(i + 1);
                        abort_bulk = true;
                    }
                    continue;
                case queue_policy::unbounded:
                    break;
                }
            }

            assign_task_metadata_locked(item);
            submitted_tasks_total_.fetch_add(1, std::memory_order_relaxed);
            queued_metadata.push_back(item.metadata);
            if (should_use_local_queue_locked(item)) {
                push_local_task_locked(current_worker_, std::move(item));
            } else {
                push_task_locked(std::move(item));
            }
            ++accepted;
            ++enqueued;
            maybe_grow_for_pending_task_locked();
        }
    }

    for (const auto& metadata : queued_metadata) {
        notify_task_queued(metadata);
    }
    for (const auto& metadata : cancelled_metadata) {
        notify_task_cancel(metadata);
    }

    if (enqueued == 1) {
        task_cv_.notify_one();
    } else if (enqueued > 1) {
        task_cv_.notify_all();
    }

    if (throw_on_failure) {
        if (closed_failure) {
            throw thread_pool_closed("thread pool is not accepting new bulk tasks");
        }
        if (capacity_failure) {
            throw task_rejected("thread pool queue is full while bulk enqueuing");
        }
    }

    return accepted;
}

void thread_pool::push_task_locked(task_item item) {
    const auto bucket = priority_bucket(item.priority);
    queues_[bucket].push_back(std::move(item));
    ++queued_size_;
    record_queue_size_locked();
}

void thread_pool::push_local_task_locked(worker_control* worker, task_item item) {
    {
        std::lock_guard<std::mutex> local_lock(worker->local_mutex);
        worker->local_queue.push_front(std::move(item));
    }
    ++queued_size_;
    local_tasks_total_.fetch_add(1, std::memory_order_relaxed);
    record_queue_size_locked();
}

bool thread_pool::should_use_local_queue_locked(const task_item& item) const noexcept {
    if (item.preferred_numa_node &&
        (!current_worker_ || current_worker_->numa_node != *item.preferred_numa_node)) {
        return false;
    }

    return options_.enable_work_stealing &&
        !options_.enable_priority &&
        current_pool_ == this &&
        current_worker_ != nullptr;
}

void thread_pool::record_queue_size_locked() {
    auto current_max = max_queue_size_seen_.load(std::memory_order_relaxed);
    while (queued_size_ > current_max &&
           !max_queue_size_seen_.compare_exchange_weak(
               current_max,
               queued_size_,
               std::memory_order_relaxed)) {
    }
}

bool thread_pool::pop_task_locked(worker_control* worker, task_item& item) {
    if (queued_size_ == 0) {
        return false;
    }

    auto matches_worker = [worker](const task_item& candidate) {
        return !worker ||
            !candidate.preferred_numa_node ||
            *candidate.preferred_numa_node == worker->numa_node;
    };
    auto pop_matching = [&](std::deque<task_item>& queue) {
        for (auto it = queue.begin(); it != queue.end(); ++it) {
            if (!matches_worker(*it)) {
                continue;
            }
            item = std::move(*it);
            queue.erase(it);
            --queued_size_;
            queue_space_cv_.notify_one();
            return true;
        }
        return false;
    };
    auto pop_front_any = [&](std::deque<task_item>& queue) {
        if (queue.empty()) {
            return false;
        }
        item = std::move(queue.front());
        queue.pop_front();
        --queued_size_;
        queue_space_cv_.notify_one();
        return true;
    };
    auto pop_high_to_low_matching = [&] {
        for (int bucket = static_cast<int>(queues_.size()) - 1; bucket >= 0; --bucket) {
            if (pop_matching(queues_[static_cast<std::size_t>(bucket)])) {
                return true;
            }
        }
        return false;
    };
    auto pop_high_to_low_any = [&] {
        for (int bucket = static_cast<int>(queues_.size()) - 1; bucket >= 0; --bucket) {
            if (pop_front_any(queues_[static_cast<std::size_t>(bucket)])) {
                return true;
            }
        }
        return false;
    };
    auto pop_low_to_high_matching = [&] {
        for (auto& queue : queues_) {
            if (pop_matching(queue)) {
                return true;
            }
        }
        return false;
    };
    auto pop_low_to_high_any = [&] {
        for (auto& queue : queues_) {
            if (pop_front_any(queue)) {
                return true;
            }
        }
        return false;
    };
    auto record_strict_priority_pick = [&] {
        if (options_.priority_fairness_interval > 0 &&
            priority_strict_picks_ < options_.priority_fairness_interval) {
            ++priority_strict_picks_;
        }
    };

    if (options_.enable_priority) {
        const bool fairness_due =
            options_.priority_fairness_interval > 0 &&
            priority_strict_picks_ >= options_.priority_fairness_interval;
        if (fairness_due) {
            if (pop_low_to_high_matching() || pop_low_to_high_any()) {
                priority_strict_picks_ = 0;
                priority_fairness_picks_total_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
            priority_strict_picks_ = 0;
        }

        if (pop_high_to_low_matching() || pop_high_to_low_any()) {
            record_strict_priority_pick();
            return true;
        }
    }

    auto& normal_queue = queues_[priority_bucket(task_priority::normal)];
    if (pop_matching(normal_queue) || pop_front_any(normal_queue)) {
        return true;
    }

    for (auto& queue : queues_) {
        if (pop_matching(queue)) {
            return true;
        }
    }

    for (auto& queue : queues_) {
        if (pop_front_any(queue)) {
            return true;
        }
    }

    return false;
}

bool thread_pool::pop_local_task_locked(worker_control* worker, task_item& item) {
    if (!options_.enable_work_stealing || !worker) {
        return false;
    }

    std::lock_guard<std::mutex> local_lock(worker->local_mutex);
    if (worker->local_queue.empty()) {
        return false;
    }

    for (auto it = worker->local_queue.begin(); it != worker->local_queue.end(); ++it) {
        if (it->preferred_numa_node && *it->preferred_numa_node != worker->numa_node) {
            continue;
        }
        item = std::move(*it);
        worker->local_queue.erase(it);
        --queued_size_;
        queue_space_cv_.notify_one();
        return true;
    }

    return false;
}

bool thread_pool::steal_task_locked(worker_control* thief, task_item& item) {
    if (!options_.enable_work_stealing || !thief) {
        return false;
    }

    std::lock_guard<std::mutex> workers_lock(workers_mutex_);
    for (auto& victim_ptr : workers_) {
        auto* victim = victim_ptr.get();
        if (victim == thief || victim->finished.load(std::memory_order_acquire)) {
            continue;
        }

        std::lock_guard<std::mutex> local_lock(victim->local_mutex);
        if (victim->local_queue.empty()) {
            continue;
        }

        for (auto it = victim->local_queue.end(); it != victim->local_queue.begin();) {
            --it;
            if (it->preferred_numa_node && *it->preferred_numa_node != thief->numa_node) {
                continue;
            }
            item = std::move(*it);
            victim->local_queue.erase(it);
            --queued_size_;
            steal_success_total_.fetch_add(1, std::memory_order_relaxed);
            queue_space_cv_.notify_one();
            return true;
        }
    }

    steal_fail_total_.fetch_add(1, std::memory_order_relaxed);
    return false;
}

bool thread_pool::pop_next_task_locked(worker_control* worker, task_item& item) {
    if (options_.enable_work_stealing) {
        return pop_local_task_locked(worker, item) ||
            pop_task_locked(worker, item) ||
            steal_task_locked(worker, item);
    }

    return pop_task_locked(worker, item);
}

std::size_t thread_pool::pop_task_batch_locked(
    worker_control* worker,
    std::vector<task_item>& batch,
    std::size_t max_items) {
    if (max_items == 0) {
        return 0;
    }

    const auto original_size = batch.size();
    while (batch.size() - original_size < max_items) {
        task_item item;
        if (!pop_next_task_locked(worker, item)) {
            break;
        }
        batch.push_back(std::move(item));
    }

    return batch.size() - original_size;
}

bool thread_pool::drop_oldest_locked(std::vector<task_metadata>& cancelled_metadata) {
    for (auto& queue : queues_) {
        if (!queue.empty()) {
            auto item = std::move(queue.front());
            queue.pop_front();
            --queued_size_;
            cancel_pending_item(item, cancelled_metadata);
            return true;
        }
    }

    if (options_.enable_work_stealing) {
        std::lock_guard<std::mutex> workers_lock(workers_mutex_);
        for (auto& worker : workers_) {
            std::lock_guard<std::mutex> local_lock(worker->local_mutex);
            if (!worker->local_queue.empty()) {
                auto item = std::move(worker->local_queue.back());
                worker->local_queue.pop_back();
                --queued_size_;
                cancel_pending_item(item, cancelled_metadata);
                return true;
            }
        }
    }

    return false;
}

void thread_pool::clear_queues_locked(std::vector<task_metadata>& cancelled_metadata) {
    for (auto& queue : queues_) {
        for (auto& item : queue) {
            cancel_pending_item(item, cancelled_metadata);
        }
        queue.clear();
    }

    if (options_.enable_work_stealing) {
        std::lock_guard<std::mutex> workers_lock(workers_mutex_);
        for (auto& worker : workers_) {
            std::lock_guard<std::mutex> local_lock(worker->local_mutex);
            for (auto& item : worker->local_queue) {
                cancel_pending_item(item, cancelled_metadata);
            }
            worker->local_queue.clear();
        }
    }

    queued_size_ = 0;
    queue_space_cv_.notify_all();
    idle_cv_.notify_all();
}

void thread_pool::cancel_pending_item(
    task_item& item,
    std::vector<task_metadata>& cancelled_metadata) noexcept {
    try {
        if (item.on_cancel) {
            item.on_cancel();
        }
    } catch (...) {
        handle_unhandled_exception(std::current_exception());
    }

    item.metadata.finish_time = clock_type::now();
    cancelled_tasks_total_.fetch_add(1, std::memory_order_relaxed);
    cancelled_metadata.push_back(item.metadata);
}

void thread_pool::maybe_grow_for_pending_task_locked() {
    if (options_.mode != pool_mode::cached || stopping_ || paused_) {
        return;
    }

    const auto current_threads = total_threads_.load(std::memory_order_acquire);
    if (current_threads >= options_.max_threads) {
        return;
    }

    const auto all_workers_busy = active_tasks_ >= current_threads;
    const auto threshold = std::max<std::size_t>(1, options_.queue_expand_threshold);
    const auto queue_pressure = queued_size_ >= threshold;
    if (current_threads > 0 && !all_workers_busy && !queue_pressure) {
        return;
    }

    std::size_t desired_threads = current_threads + 1;
    if (queue_pressure) {
        const auto pressure_threads = current_threads + (queued_size_ / threshold);
        desired_threads = std::max(desired_threads, pressure_threads);
    }
    desired_threads = std::min(desired_threads, options_.max_threads);

    std::lock_guard<std::mutex> workers_lock(workers_mutex_);
    join_finished_workers_unlocked();

    auto live_threads = total_threads_.load(std::memory_order_acquire);
    while (live_threads < desired_threads) {
        start_worker_unlocked();
        ++live_threads;
    }
}

bool thread_pool::should_retire_idle_worker_locked() const noexcept {
    const auto current_threads = total_threads_.load(std::memory_order_acquire);
    const auto idle_floor = std::max(options_.min_threads, target_threads_);
    return options_.mode == pool_mode::cached &&
        queued_size_ == 0 &&
        !paused_ &&
        !stopping_ &&
        current_threads > idle_floor + idle_retirements_;
}

bool thread_pool::reserve_idle_retirement_locked() noexcept {
    if (!should_retire_idle_worker_locked()) {
        return false;
    }

    ++idle_retirements_;
    return true;
}

std::size_t thread_pool::priority_bucket(task_priority priority) noexcept {
    const auto value = static_cast<int>(priority);
    return static_cast<std::size_t>(std::max(0, std::min(3, value)));
}

std::size_t thread_pool::latency_bucket_index(std::uint64_t nanoseconds) noexcept {
    for (std::size_t i = 0; i < metric_latency_bucket_upper_bounds_ns.size(); ++i) {
        if (nanoseconds <= metric_latency_bucket_upper_bounds_ns[i]) {
            return i;
        }
    }
    return metric_latency_bucket_count - 1;
}

task_priority thread_pool::resolve_task_priority(
    const task_options& options,
    task_priority fallback) noexcept {
    return options.priority.value_or(fallback);
}

task_metadata thread_pool::make_task_metadata(
    const task_options& options,
    task_priority priority) {
    task_metadata metadata;
    metadata.priority = priority;
    metadata.kind = options.kind;
    metadata.numa_node = options.numa_node.value_or(static_cast<std::size_t>(-1));
    metadata.name = options.name;
    metadata.source = options.source;
    return metadata;
}

void thread_pool::record_latency_bucket(
    latency_atomic_buckets& buckets,
    std::uint64_t nanoseconds) noexcept {
    buckets[latency_bucket_index(nanoseconds)].fetch_add(1, std::memory_order_relaxed);
}

void thread_pool::assign_task_metadata_locked(task_item& item) {
    item.sequence = next_sequence_++;
    if (item.metadata.id == 0) {
        item.metadata.id = next_task_id_++;
    }
    item.metadata.sequence = item.sequence;
    item.metadata.priority = item.priority;
    item.metadata.enqueue_time = item.enqueue_time;
}

void thread_pool::notify_task_queued(const task_metadata& metadata) const noexcept {
    if (!options_.on_task_queued) {
        return;
    }

    try {
        options_.on_task_queued(metadata);
    } catch (...) {
        handle_unhandled_exception(std::current_exception());
    }
}

void thread_pool::notify_task_start(const task_metadata& metadata) const noexcept {
    if (!options_.on_task_start) {
        return;
    }

    try {
        options_.on_task_start(metadata);
    } catch (...) {
        handle_unhandled_exception(std::current_exception());
    }
}

void thread_pool::notify_task_finish(const task_metadata& metadata) const noexcept {
    if (!options_.on_task_finish) {
        return;
    }

    try {
        options_.on_task_finish(metadata);
    } catch (...) {
        handle_unhandled_exception(std::current_exception());
    }
}

void thread_pool::notify_task_cancel(const task_metadata& metadata) const noexcept {
    if (!options_.on_task_cancel) {
        return;
    }

    try {
        options_.on_task_cancel(metadata);
    } catch (...) {
        handle_unhandled_exception(std::current_exception());
    }
}

void thread_pool::notify_task_error(
    const task_metadata& metadata,
    std::exception_ptr error) const noexcept {
    if (!options_.on_task_error) {
        return;
    }

    try {
        options_.on_task_error(metadata, error);
    } catch (...) {
        handle_unhandled_exception(std::current_exception());
    }
}

std::string thread_pool::make_thread_name(
    const char* role,
    std::size_t id,
    const thread_pool_options& options) {
    const auto& configured_prefix =
        options.thread_name_prefix.empty() ? options.name : options.thread_name_prefix;
    std::string thread_name = configured_prefix.empty() ? "thread_pool" : configured_prefix;
    thread_name += "-";
    thread_name += role ? role : "thread";
    thread_name += "-";
    thread_name += std::to_string(id);
    return thread_name;
}

void thread_pool::set_current_thread_name(const std::string& name) noexcept {
    if (name.empty()) {
        return;
    }

#if defined(__linux__)
    const auto platform_name = name.size() > 15 ? name.substr(0, 15) : name;
    (void)pthread_setname_np(pthread_self(), platform_name.c_str());
#elif defined(__APPLE__)
    const auto platform_name = name.size() > 63 ? name.substr(0, 63) : name;
    (void)pthread_setname_np(platform_name.c_str());
#elif defined(_WIN32)
    using set_thread_description_fn = HRESULT(WINAPI*)(HANDLE, PCWSTR);
    auto* kernel32 = GetModuleHandleW(L"Kernel32.dll");
    if (!kernel32) {
        return;
    }
    auto* raw_fn = GetProcAddress(kernel32, "SetThreadDescription");
    if (!raw_fn) {
        return;
    }
    auto* set_thread_description =
        reinterpret_cast<set_thread_description_fn>(raw_fn);
    const std::wstring wide_name(name.begin(), name.end());
    (void)set_thread_description(GetCurrentThread(), wide_name.c_str());
#else
    (void)name;
#endif
}

bool thread_pool::set_current_thread_affinity(std::size_t cpu_id) noexcept {
#if defined(__linux__)
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);
    return pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) == 0;
#elif defined(_WIN32)
    if (cpu_id >= sizeof(DWORD_PTR) * 8) {
        return false;
    }
    const auto mask = static_cast<DWORD_PTR>(1) << cpu_id;
    return SetThreadAffinityMask(GetCurrentThread(), mask) != 0;
#else
    (void)cpu_id;
    return false;
#endif
}

void thread_pool::start_worker_unlocked() {
    auto control = std::make_unique<worker_control>();
    control->id = next_worker_id_++;
    control->numa_node = control->id % options_.numa_nodes;
    auto* raw_control = control.get();
    workers_.push_back(std::move(control));

    total_threads_.fetch_add(1, std::memory_order_release);
    workers_.back()->thread = std::thread([this, raw_control] {
        worker_loop(raw_control);
    });
}

void thread_pool::worker_loop(worker_control* control) noexcept {
    current_pool_ = this;
    current_worker_id_ = control->id;
    current_worker_ = control;
    bool reserved_idle_retirement = false;
    bool reserved_resize_retirement = false;

    set_current_thread_name(make_thread_name("worker", control->id, options_));

    if (options_.enable_thread_affinity || !options_.thread_affinity_cpu_ids.empty()) {
        std::size_t cpu_id = 0;
        if (!options_.thread_affinity_cpu_ids.empty()) {
            cpu_id = options_.thread_affinity_cpu_ids[
                control->id % options_.thread_affinity_cpu_ids.size()];
        } else {
            cpu_id = control->id % std::max<std::size_t>(1, default_thread_count());
        }

        if (set_current_thread_affinity(cpu_id)) {
            thread_affinity_applied_total_.fetch_add(1, std::memory_order_relaxed);
        } else {
            thread_affinity_failed_total_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    if (options_.on_thread_affinity) {
        try {
            options_.on_thread_affinity(control->id, control->numa_node);
        } catch (...) {
            handle_unhandled_exception(std::current_exception());
        }
    }

    if (options_.on_thread_start) {
        try {
            options_.on_thread_start(control->id);
        } catch (...) {
            handle_unhandled_exception(std::current_exception());
        }
    }

    std::vector<task_item> batch;
    batch.reserve(options_.worker_batch_size);

    while (true) {
        batch.clear();
        bool retire_after_idle_timeout = false;

        {
            std::unique_lock<std::mutex> lock(mutex_);
            while (!stopping_ &&
                   (paused_ || queued_size_ == 0) &&
                   retire_requests_ == 0) {
                if (should_retire_idle_worker_locked()) {
                    const auto status = task_cv_.wait_for(lock, options_.idle_timeout);
                    worker_wakeup_total_.fetch_add(1, std::memory_order_relaxed);
                    if (status == std::cv_status::timeout) {
                        worker_idle_timeout_total_.fetch_add(1, std::memory_order_relaxed);
                    }
                    if (status == std::cv_status::timeout &&
                        reserve_idle_retirement_locked()) {
                        worker_retired_total_.fetch_add(1, std::memory_order_relaxed);
                        reserved_idle_retirement = true;
                        retire_after_idle_timeout = true;
                        break;
                    }
                } else {
                    task_cv_.wait(lock);
                    worker_wakeup_total_.fetch_add(1, std::memory_order_relaxed);
                }
            }

            if (retire_after_idle_timeout) {
                break;
            }

            if (!stopping_ && queued_size_ == 0 && retire_requests_ > 0) {
                const auto current_threads = total_threads_.load(std::memory_order_acquire);
                const auto reserved =
                    std::min(current_threads, resize_retirements_ + idle_retirements_);
                const auto effective_threads = current_threads - reserved;
                if (effective_threads > target_threads_) {
                    --retire_requests_;
                    ++resize_retirements_;
                    reserved_resize_retirement = true;
                    worker_retired_total_.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                retire_requests_ = 0;
            }

            if (stop_immediately_) {
                break;
            }

            if (stopping_ && queued_size_ == 0) {
                break;
            }

            if (paused_) {
                continue;
            }

            const auto popped = pop_task_batch_locked(
                control,
                batch,
                options_.worker_batch_size);
            if (popped == 0) {
                continue;
            }

            active_tasks_ += popped;
        }

        for (auto& item : batch) {
            execute_task(item);
            finish_active_task();
        }
    }

    if (options_.on_thread_stop) {
        try {
            options_.on_thread_stop(control->id);
        } catch (...) {
            handle_unhandled_exception(std::current_exception());
        }
    }

    current_pool_ = nullptr;
    current_worker_id_ = static_cast<std::size_t>(-1);
    current_worker_ = nullptr;
    total_threads_.fetch_sub(1, std::memory_order_release);

    std::size_t restore_target = 0;
    if (reserved_idle_retirement || reserved_resize_retirement) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (reserved_idle_retirement && idle_retirements_ > 0) {
            --idle_retirements_;
        }
        if (reserved_resize_retirement && resize_retirements_ > 0) {
            --resize_retirements_;
        }
        if (!stopping_ && accepting_) {
            const auto current_threads = total_threads_.load(std::memory_order_acquire);
            if (current_threads < target_threads_) {
                restore_target = target_threads_;
            }
        }
    }

    if (restore_target > 0) {
        try {
            std::lock_guard<std::mutex> workers_lock(workers_mutex_);
            join_finished_workers_unlocked();
            auto live_threads = total_threads_.load(std::memory_order_acquire);
            while (live_threads < restore_target && live_threads < options_.max_threads) {
                start_worker_unlocked();
                ++live_threads;
            }
            task_cv_.notify_all();
        } catch (...) {
            handle_unhandled_exception(std::current_exception());
        }
    }
    control->finished.store(true, std::memory_order_release);
    idle_cv_.notify_all();
}

void thread_pool::timer_loop() noexcept {
    while (true) {
        std::vector<delayed_task> ready_tasks;

        {
            std::unique_lock<std::mutex> lock(delay_mutex_);

            while (!timer_stopping_ && delayed_tasks_.empty()) {
                delay_cv_.wait(lock);
            }

            if (timer_stopping_) {
                break;
            }

            auto next = delayed_tasks_.begin();
            const auto now = clock_type::now();
            if (next->first > now) {
                const auto next_due = next->first;
                delay_cv_.wait_until(lock, next_due);
                continue;
            }

            while (next != delayed_tasks_.end() && next->first <= now) {
                auto node = delayed_tasks_.extract(next++);
                ready_tasks.push_back(std::move(node.mapped()));
            }
        }

        if (ready_tasks.empty()) {
            continue;
        }

        for (auto& delayed : ready_tasks) {
            if (delayed.state && delayed.state->cancelled()) {
                std::vector<task_metadata> cancelled_metadata;
                cancel_pending_item(delayed.item, cancelled_metadata);
                scheduled_tasks_cancelled_total_.fetch_add(1, std::memory_order_relaxed);
                for (const auto& metadata : cancelled_metadata) {
                    notify_task_cancel(metadata);
                }
                continue;
            }

            delayed.item.enqueue_time = clock_type::now();
            if (!enqueue_or_false(std::move(delayed.item))) {
                scheduled_tasks_cancelled_total_.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }
}

void thread_pool::start_continuation_workers_unlocked() {
    continuation_threads_.reserve(options_.continuation_threads);
    for (std::size_t i = 0; i < options_.continuation_threads; ++i) {
        continuation_threads_.emplace_back([this, i] {
            set_current_thread_name(make_thread_name("cont", i, options_));
            continuation_loop();
        });
    }
}

void thread_pool::continuation_loop() noexcept {
    while (true) {
        std::packaged_task<void()> task;
        {
            std::unique_lock<std::mutex> lock(continuations_mutex_);
            continuation_cv_.wait(lock, [this] {
                return continuation_stopping_ || !continuation_tasks_.empty();
            });

            if (continuation_tasks_.empty()) {
                if (continuation_stopping_) {
                    break;
                }
                continue;
            }

            task = std::move(continuation_tasks_.front());
            continuation_tasks_.pop_front();
        }

        try {
            task();
        } catch (...) {
            handle_unhandled_exception(std::current_exception());
        }
    }
}

void thread_pool::stop_continuation_workers() noexcept {
    {
        std::lock_guard<std::mutex> lock(continuations_mutex_);
        continuation_stopping_ = true;
    }
    continuation_cv_.notify_all();

    for (auto& thread : continuation_threads_) {
        if (!thread.joinable()) {
            continue;
        }
        if (thread.get_id() == std::this_thread::get_id()) {
            thread.detach();
        } else {
            thread.join();
        }
    }

    continuation_threads_.clear();
}

void thread_pool::execute_task(task_item& item) noexcept {
    const auto start = clock_type::now();
    item.metadata.start_time = start;
    item.metadata.worker_id = current_worker_id_;
    if (item.metadata.numa_node == static_cast<std::size_t>(-1) && current_worker_) {
        item.metadata.numa_node = current_worker_->numa_node;
    }
    const auto wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        start - item.enqueue_time).count();
    const auto wait_ns_value =
        static_cast<std::uint64_t>(std::max<std::int64_t>(0, wait_ns));
    task_wait_time_ns_total_.fetch_add(wait_ns_value, std::memory_order_relaxed);
    record_latency_bucket(task_wait_time_buckets_, wait_ns_value);

    if (item.should_cancel && item.should_cancel()) {
        try {
            if (item.on_cancel) {
                item.on_cancel();
            }
        } catch (...) {
            handle_unhandled_exception(std::current_exception());
        }
        item.metadata.finish_time = clock_type::now();
        cancelled_tasks_total_.fetch_add(1, std::memory_order_relaxed);
        if (current_worker_) {
            current_worker_->cancelled_tasks.fetch_add(1, std::memory_order_relaxed);
        }
        notify_task_cancel(item.metadata);
        return;
    }

    notify_task_start(item.metadata);
    running_tasks_.fetch_add(1, std::memory_order_relaxed);

    try {
        item.function();
        item.metadata.finish_time = clock_type::now();
        completed_tasks_total_.fetch_add(1, std::memory_order_relaxed);
        if (current_worker_) {
            current_worker_->completed_tasks.fetch_add(1, std::memory_order_relaxed);
        }
        notify_task_finish(item.metadata);
    } catch (...) {
        const auto error = std::current_exception();
        item.metadata.finish_time = clock_type::now();
        failed_tasks_total_.fetch_add(1, std::memory_order_relaxed);
        if (current_worker_) {
            current_worker_->failed_tasks.fetch_add(1, std::memory_order_relaxed);
        }
        notify_task_error(item.metadata, error);
        handle_unhandled_exception(error);
    }

    const auto finish = item.metadata.finish_time;
    const auto run_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        finish - start).count();
    const auto run_ns_value =
        static_cast<std::uint64_t>(std::max<std::int64_t>(0, run_ns));
    task_run_time_ns_total_.fetch_add(run_ns_value, std::memory_order_relaxed);
    record_latency_bucket(task_run_time_buckets_, run_ns_value);
    running_tasks_.fetch_sub(1, std::memory_order_relaxed);
}

void thread_pool::finish_active_task() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (active_tasks_ > 0) {
        --active_tasks_;
    }
    if (queued_size_ == 0 && active_tasks_ == 0) {
        idle_cv_.notify_all();
    }
    queue_space_cv_.notify_one();
}

bool thread_pool::try_run_one_inline() {
    if (current_pool_ != this) {
        return false;
    }

    task_item item;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (paused_ || stop_immediately_ || queued_size_ == 0) {
            return false;
        }
        if (!pop_next_task_locked(current_worker_, item)) {
            return false;
        }
        ++active_tasks_;
    }

    execute_task(item);
    finish_active_task();
    return true;
}

bool thread_pool::begin_managed_blocking() noexcept {
    if (!options_.enable_managed_blocking || current_pool_ != this) {
        return false;
    }

    managed_blocking_total_.fetch_add(1, std::memory_order_relaxed);
    blocked_workers_.fetch_add(1, std::memory_order_relaxed);
    maybe_start_blocking_compensation_worker();
    return true;
}

void thread_pool::end_managed_blocking() noexcept {
    blocked_workers_.fetch_sub(1, std::memory_order_relaxed);
    task_cv_.notify_one();
}

void thread_pool::maybe_start_blocking_compensation_worker() noexcept {
    try {
        // Best-effort path: managed blocking must not stall while contending
        // for the main pool mutex. Later submissions can still grow the pool.
        std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
        if (!lock || stopping_ || !accepting_) {
            return;
        }

        const auto current_threads = total_threads_.load(std::memory_order_acquire);
        if (current_threads >= options_.max_threads) {
            return;
        }

        std::lock_guard<std::mutex> workers_lock(workers_mutex_);
        join_finished_workers_unlocked();
        if (total_threads_.load(std::memory_order_acquire) >= options_.max_threads) {
            return;
        }

        start_worker_unlocked();
        managed_blocking_compensations_total_.fetch_add(1, std::memory_order_relaxed);
        task_cv_.notify_all();
    } catch (...) {
        handle_unhandled_exception(std::current_exception());
    }
}

void thread_pool::join_finished_workers_unlocked() {
    auto it = workers_.begin();
    while (it != workers_.end()) {
        auto& worker = *it;
        if (worker->finished.load(std::memory_order_acquire)) {
            if (worker->thread.joinable()) {
                if (worker->thread.get_id() == std::this_thread::get_id()) {
                    worker->thread.detach();
                } else {
                    worker->thread.join();
                }
            }
            it = workers_.erase(it);
        } else {
            ++it;
        }
    }
}

void thread_pool::handle_unhandled_exception(std::exception_ptr error) const noexcept {
    if (!options_.on_unhandled_exception) {
        return;
    }

    try {
        options_.on_unhandled_exception(error);
    } catch (...) {
    }
}

} // namespace universal_thread_pool
