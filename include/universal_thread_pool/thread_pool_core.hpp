#pragma once

#include "universal_thread_pool/coroutine.hpp"

namespace universal_thread_pool {

class thread_pool {
    using clock_type = std::chrono::steady_clock;

public:
    explicit thread_pool(thread_pool_options options = {});
    ~thread_pool();

    thread_pool(const thread_pool&) = delete;
    thread_pool& operator=(const thread_pool&) = delete;

    template <class F, class... Args>
    auto submit(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        return submit_with_priority(
            task_priority::normal,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto submit_with_priority(task_priority priority, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        using result_type = std::invoke_result_t<F, Args...>;

        auto callable = make_callable<result_type>(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));
        auto promise = std::make_shared<std::promise<result_type>>();
        auto future = promise->get_future();

        task_item item;
        item.priority = priority;
        item.enqueue_time = clock_type::now();
        item.on_cancel = [promise] {
            cancel_promise<result_type>(promise, "task was cancelled before execution");
        };
        item.function = [promise, shared_callable]() mutable {
            invoke_into_promise<result_type>(promise, *shared_callable);
        };

        enqueue_or_throw(std::move(item));
        return future;
    }

    template <class F, class... Args>
    auto try_submit(F&& f, Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return try_submit_with_priority(
            task_priority::normal,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    auto try_submit_for(
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return try_submit_with_priority_for(
            task_priority::normal,
            timeout,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    auto try_submit_until(
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return try_submit_with_priority_until(
            task_priority::normal,
            deadline,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto try_submit_with_priority(task_priority priority, F&& f, Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return try_submit_with_priority_until(
            priority,
            clock_type::now(),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    auto try_submit_with_priority_for(
        task_priority priority,
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return try_submit_with_priority_until(
            priority,
            clock_type::now() + std::chrono::duration_cast<clock_type::duration>(timeout),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    auto try_submit_with_priority_until(
        task_priority priority,
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        using result_type = std::invoke_result_t<F, Args...>;

        auto callable = make_callable<result_type>(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));
        auto promise = std::make_shared<std::promise<result_type>>();
        auto future = promise->get_future();

        task_item item;
        item.priority = priority;
        item.enqueue_time = clock_type::now();
        item.on_cancel = [promise] {
            cancel_promise<result_type>(promise, "task was cancelled before execution");
        };
        item.function = [promise, shared_callable]() mutable {
            invoke_into_promise<result_type>(promise, *shared_callable);
        };

        if (!try_enqueue_until(std::move(item), to_steady_time(deadline))) {
            return std::nullopt;
        }

        return std::optional<std::future<result_type>>{std::move(future)};
    }

    template <class F, class... Args>
    auto submit_with_options(task_options options, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        using result_type = std::invoke_result_t<F, Args...>;
        const auto priority = resolve_task_priority(options, task_priority::normal);

        auto callable = make_callable<result_type>(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));
        auto promise = std::make_shared<std::promise<result_type>>();
        auto future = promise->get_future();

        task_item item;
        item.priority = priority;
        item.preferred_numa_node = options.numa_node;
        item.metadata = make_task_metadata(options, priority);
        item.enqueue_time = clock_type::now();
        item.should_cancel = [cancellation = options.cancellation, deadline = options.deadline] {
            return cancellation.stop_requested() ||
                (deadline && clock_type::now() >= *deadline);
        };
        item.on_cancel = [promise] {
            cancel_promise<result_type>(promise, "task was cancelled before execution");
        };
        item.function = [promise, shared_callable]() mutable {
            invoke_into_promise<result_type>(promise, *shared_callable);
        };

        enqueue_or_throw(std::move(item));
        return future;
    }

    template <class F, class... Args>
    auto try_submit_with_options(task_options options, F&& f, Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return try_submit_with_options_until(
            std::move(options),
            clock_type::now(),
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
        return try_submit_with_options_until(
            std::move(options),
            clock_type::now() + std::chrono::duration_cast<clock_type::duration>(timeout),
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
        using result_type = std::invoke_result_t<F, Args...>;
        const auto priority = resolve_task_priority(options, task_priority::normal);

        auto callable = make_callable<result_type>(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));
        auto promise = std::make_shared<std::promise<result_type>>();
        auto future = promise->get_future();

        task_item item;
        item.priority = priority;
        item.preferred_numa_node = options.numa_node;
        item.metadata = make_task_metadata(options, priority);
        item.enqueue_time = clock_type::now();
        item.should_cancel = [cancellation = options.cancellation, deadline = options.deadline] {
            return cancellation.stop_requested() ||
                (deadline && clock_type::now() >= *deadline);
        };
        item.on_cancel = [promise] {
            cancel_promise<result_type>(promise, "task was cancelled before execution");
        };
        item.function = [promise, shared_callable]() mutable {
            invoke_into_promise<result_type>(promise, *shared_callable);
        };

        if (!try_enqueue_until(std::move(item), to_steady_time(deadline))) {
            return std::nullopt;
        }

        return std::optional<std::future<result_type>>{std::move(future)};
    }

    template <class F, class... Args>
    bool detach(F&& f, Args&&... args) {
        return detach_with_priority(
            task_priority::normal,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    bool detach_with_priority(task_priority priority, F&& f, Args&&... args) {
        auto callable = make_void_callable(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));

        task_item item;
        item.priority = priority;
        item.enqueue_time = clock_type::now();
        item.function = [shared_callable]() mutable {
            (*shared_callable)();
        };

        return enqueue_or_false(std::move(item));
    }

    template <class F, class... Args>
    bool try_detach(F&& f, Args&&... args) {
        return try_detach_with_priority(
            task_priority::normal,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    bool try_detach_for(
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args) {
        return try_detach_with_priority_for(
            task_priority::normal,
            timeout,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    bool try_detach_until(
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args) {
        return try_detach_with_priority_until(
            task_priority::normal,
            deadline,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    bool try_detach_with_priority(task_priority priority, F&& f, Args&&... args) {
        return try_detach_with_priority_until(
            priority,
            clock_type::now(),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    bool try_detach_with_priority_for(
        task_priority priority,
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args) {
        return try_detach_with_priority_until(
            priority,
            clock_type::now() + std::chrono::duration_cast<clock_type::duration>(timeout),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    bool try_detach_with_priority_until(
        task_priority priority,
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args) {
        auto callable = make_void_callable(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));

        task_item item;
        item.priority = priority;
        item.enqueue_time = clock_type::now();
        item.function = [shared_callable]() mutable {
            (*shared_callable)();
        };

        return try_enqueue_until(std::move(item), to_steady_time(deadline));
    }

    template <class F, class... Args>
    bool detach_with_options(task_options options, F&& f, Args&&... args) {
        const auto priority = resolve_task_priority(options, task_priority::normal);
        auto callable = make_void_callable(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));

        task_item item;
        item.priority = priority;
        item.preferred_numa_node = options.numa_node;
        item.metadata = make_task_metadata(options, priority);
        item.enqueue_time = clock_type::now();
        item.should_cancel = [cancellation = options.cancellation, deadline = options.deadline] {
            return cancellation.stop_requested() ||
                (deadline && clock_type::now() >= *deadline);
        };
        item.function = [shared_callable]() mutable {
            (*shared_callable)();
        };

        return enqueue_or_false(std::move(item));
    }

    template <class F, class... Args>
    bool try_detach_with_options(task_options options, F&& f, Args&&... args) {
        return try_detach_with_options_until(
            std::move(options),
            clock_type::now(),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    bool try_detach_with_options_for(
        task_options options,
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args) {
        return try_detach_with_options_until(
            std::move(options),
            clock_type::now() + std::chrono::duration_cast<clock_type::duration>(timeout),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    bool try_detach_with_options_until(
        task_options options,
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args) {
        const auto priority = resolve_task_priority(options, task_priority::normal);
        auto callable = make_void_callable(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));

        task_item item;
        item.priority = priority;
        item.preferred_numa_node = options.numa_node;
        item.metadata = make_task_metadata(options, priority);
        item.enqueue_time = clock_type::now();
        item.should_cancel = [cancellation = options.cancellation, deadline = options.deadline] {
            return cancellation.stop_requested() ||
                (deadline && clock_type::now() >= *deadline);
        };
        item.function = [shared_callable]() mutable {
            (*shared_callable)();
        };

        return try_enqueue_until(std::move(item), to_steady_time(deadline));
    }

    template <class F, class... Args>
    auto submit_cancellable(cancellation_token token, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        return submit_cancellable_with_priority(
            task_priority::normal,
            std::move(token),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto submit_cancellable_with_priority(
        task_priority priority,
        cancellation_token token,
        F&& f,
        Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        using result_type = std::invoke_result_t<F, Args...>;

        auto callable = make_callable<result_type>(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));
        auto promise = std::make_shared<std::promise<result_type>>();
        auto future = promise->get_future();

        task_item item;
        item.priority = priority;
        item.enqueue_time = clock_type::now();
        item.should_cancel = [token] {
            return token.stop_requested();
        };
        item.on_cancel = [promise] {
            cancel_promise<result_type>(promise, "task was cancelled before execution");
        };
        item.function = [promise, shared_callable]() mutable {
            invoke_into_promise<result_type>(promise, *shared_callable);
        };

        enqueue_or_throw(std::move(item));
        return future;
    }

    template <class F, class... Args>
    bool detach_cancellable(cancellation_token token, F&& f, Args&&... args) {
        return detach_cancellable_with_priority(
            task_priority::normal,
            std::move(token),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    bool detach_cancellable_with_priority(
        task_priority priority,
        cancellation_token token,
        F&& f,
        Args&&... args) {
        auto callable = make_void_callable(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));

        task_item item;
        item.priority = priority;
        item.enqueue_time = clock_type::now();
        item.should_cancel = [token] {
            return token.stop_requested();
        };
        item.function = [shared_callable]() mutable {
            (*shared_callable)();
        };

        return enqueue_or_false(std::move(item));
    }

#if UNIVERSAL_THREAD_POOL_HAS_STOP_TOKEN
    template <class F, class... Args>
    auto submit_stop_token(std::stop_token token, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        return submit_stop_token_with_priority(
            task_priority::normal,
            std::move(token),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto submit_stop_token_with_priority(
        task_priority priority,
        std::stop_token token,
        F&& f,
        Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        using result_type = std::invoke_result_t<F, Args...>;

        auto callable = make_callable<result_type>(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));
        auto promise = std::make_shared<std::promise<result_type>>();
        auto future = promise->get_future();

        task_item item;
        item.priority = priority;
        item.enqueue_time = clock_type::now();
        item.should_cancel = [token] {
            return token.stop_requested();
        };
        item.on_cancel = [promise] {
            cancel_promise<result_type>(promise, "std::stop_token requested before execution");
        };
        item.function = [promise, shared_callable]() mutable {
            invoke_into_promise<result_type>(promise, *shared_callable);
        };

        enqueue_or_throw(std::move(item));
        return future;
    }

    template <class F, class... Args>
    bool detach_stop_token(std::stop_token token, F&& f, Args&&... args) {
        return detach_stop_token_with_priority(
            task_priority::normal,
            std::move(token),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    bool detach_stop_token_with_priority(
        task_priority priority,
        std::stop_token token,
        F&& f,
        Args&&... args) {
        auto callable = make_void_callable(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));

        task_item item;
        item.priority = priority;
        item.enqueue_time = clock_type::now();
        item.should_cancel = [token] {
            return token.stop_requested();
        };
        item.function = [shared_callable]() mutable {
            (*shared_callable)();
        };

        return enqueue_or_false(std::move(item));
    }
#endif

    template <class F, class... Args>
    auto submit_retry(retry_options retry, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<std::decay_t<F>&, std::decay_t<Args>&...>> {
        return submit_retry_with_options(
            std::move(retry),
            task_options{},
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto submit_retry_with_priority(
        retry_options retry,
        task_priority priority,
        F&& f,
        Args&&... args)
        -> std::future<std::invoke_result_t<std::decay_t<F>&, std::decay_t<Args>&...>> {
        task_options options;
        options.priority = priority;
        return submit_retry_with_options(
            std::move(retry),
            std::move(options),
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
        using function_type = std::decay_t<F>;
        using args_tuple_type = std::tuple<std::decay_t<Args>...>;
        using result_type = std::invoke_result_t<function_type&, std::decay_t<Args>&...>;

        function_type function(std::forward<F>(f));
        args_tuple_type args_tuple(std::forward<Args>(args)...);
        if (retry.max_attempts == 0) {
            retry.max_attempts = 1;
        }
        if (retry.backoff_multiplier < 1.0) {
            retry.backoff_multiplier = 1.0;
        }

        auto retry_task = [this,
                           retry = std::move(retry),
                           function = std::move(function),
                           args_tuple = std::move(args_tuple)]() mutable -> result_type {
            auto next_backoff = retry.initial_backoff;
            std::size_t attempt = 0;

            while (true) {
                ++attempt;
                try {
                    if constexpr (std::is_void<result_type>::value) {
                        std::apply(function, args_tuple);
                        return;
                    } else {
                        return std::apply(function, args_tuple);
                    }
                } catch (...) {
                    const auto error = std::current_exception();
                    bool should_retry = attempt < retry.max_attempts;
                    if (should_retry && retry.should_retry) {
                        should_retry = retry.should_retry(error, attempt);
                    } else if (should_retry && !retry.retry_on_task_cancelled) {
                        try {
                            std::rethrow_exception(error);
                        } catch (const task_cancelled&) {
                            should_retry = false;
                        } catch (...) {
                        }
                    }

                    if (!should_retry) {
                        std::rethrow_exception(error);
                    }

                    if (next_backoff.count() > 0) {
                        auto scope = managed_blocking();
                        (void)scope;
                        std::this_thread::sleep_for(next_backoff);
                    }

                    if (retry.backoff_multiplier > 1.0 && next_backoff.count() > 0) {
                        const auto scaled =
                            static_cast<long double>(next_backoff.count()) *
                            retry.backoff_multiplier;
                        auto next_count = static_cast<std::chrono::nanoseconds::rep>(scaled);
                        if (retry.max_backoff.count() > 0) {
                            next_count = std::min(next_count, retry.max_backoff.count());
                        }
                        next_backoff = std::chrono::nanoseconds{next_count};
                    }
                }
            }
        };

        return submit_with_options(
            std::move(options),
            std::move(retry_task));
    }

    template <class Clock, class Duration, class F, class... Args>
    auto submit_with_deadline(
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        using result_type = std::invoke_result_t<F, Args...>;

        auto callable = make_callable<result_type>(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));
        auto promise = std::make_shared<std::promise<result_type>>();
        auto future = promise->get_future();
        const auto steady_deadline = to_steady_time(deadline);

        task_item item;
        item.priority = task_priority::normal;
        item.enqueue_time = clock_type::now();
        item.should_cancel = [steady_deadline] {
            return clock_type::now() >= steady_deadline;
        };
        item.on_cancel = [promise] {
            cancel_promise<result_type>(promise, "task deadline expired before execution");
        };
        item.function = [promise, shared_callable]() mutable {
            invoke_into_promise<result_type>(promise, *shared_callable);
        };

        enqueue_or_throw(std::move(item));
        return future;
    }

    template <class Rep, class Period, class F, class... Args>
    auto submit_with_timeout(
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        return submit_with_deadline(
            clock_type::now() + std::chrono::duration_cast<clock_type::duration>(timeout),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    bool detach_with_deadline(
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args) {
        auto callable = make_void_callable(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));
        const auto steady_deadline = to_steady_time(deadline);

        task_item item;
        item.priority = task_priority::normal;
        item.enqueue_time = clock_type::now();
        item.should_cancel = [steady_deadline] {
            return clock_type::now() >= steady_deadline;
        };
        item.function = [shared_callable]() mutable {
            (*shared_callable)();
        };

        return enqueue_or_false(std::move(item));
    }

    template <class Rep, class Period, class F, class... Args>
    bool detach_with_timeout(
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args) {
        return detach_with_deadline(
            clock_type::now() + std::chrono::duration_cast<clock_type::duration>(timeout),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F>
    auto bulk_submit(std::size_t count, F&& f)
        -> std::vector<std::future<std::invoke_result_t<std::decay_t<F>&, std::size_t>>> {
        return bulk_submit_with_priority(
            task_priority::normal,
            count,
            std::forward<F>(f));
    }

    template <class F>
    auto bulk_submit_with_priority(task_priority priority, std::size_t count, F&& f)
        -> std::vector<std::future<std::invoke_result_t<std::decay_t<F>&, std::size_t>>> {
        using function_type = std::decay_t<F>;
        using result_type = std::invoke_result_t<function_type&, std::size_t>;
        static_assert(
            std::is_copy_constructible<function_type>::value,
            "bulk_submit requires a copy-constructible callable");

        function_type function(std::forward<F>(f));
        std::vector<std::future<result_type>> futures;
        futures.reserve(count);
        if (options_.queue == queue_policy::bounded_block ||
            options_.queue == queue_policy::bounded_caller_runs) {
            for (std::size_t index = 0; index < count; ++index) {
                futures.emplace_back(submit_with_priority(priority, [function, index]() mutable -> result_type {
                    if constexpr (std::is_void<result_type>::value) {
                        function(index);
                    } else {
                        return function(index);
                    }
                }));
            }
            return futures;
        }

        std::vector<task_item> items;
        items.reserve(count);
        for (std::size_t index = 0; index < count; ++index) {
            auto task_function = [function, index]() mutable -> result_type {
                if constexpr (std::is_void<result_type>::value) {
                    function(index);
                } else {
                    return function(index);
                }
            };
            using task_function_type = decltype(task_function);
            auto shared_callable = std::make_shared<task_function_type>(std::move(task_function));
            auto promise = std::make_shared<std::promise<result_type>>();
            futures.emplace_back(promise->get_future());

            task_item item;
            item.priority = priority;
            item.enqueue_time = clock_type::now();
            item.on_cancel = [promise] {
                cancel_promise<result_type>(promise, "task was cancelled before execution");
            };
            item.function = [promise, shared_callable]() mutable {
                invoke_into_promise<result_type>(promise, *shared_callable);
            };
            items.push_back(std::move(item));
        }

        enqueue_many_or_throw(std::move(items));
        return futures;
    }

    // Copies task_options for each item. The cancellation token remains shared,
    // so cancelling it intentionally cancels the whole submitted batch.
    template <class F>
    auto bulk_submit_with_options(task_options options, std::size_t count, F&& f)
        -> std::vector<std::future<std::invoke_result_t<std::decay_t<F>&, std::size_t>>> {
        using function_type = std::decay_t<F>;
        using result_type = std::invoke_result_t<function_type&, std::size_t>;
        static_assert(
            std::is_copy_constructible<function_type>::value,
            "bulk_submit_with_options requires a copy-constructible callable");

        function_type function(std::forward<F>(f));
        task_options base_options = std::move(options);
        std::vector<std::future<result_type>> futures;
        futures.reserve(count);
        if (options_.queue == queue_policy::bounded_block ||
            options_.queue == queue_policy::bounded_caller_runs) {
            for (std::size_t index = 0; index < count; ++index) {
                auto item_options = base_options;
                futures.emplace_back(submit_with_options(
                    std::move(item_options),
                    [function, index]() mutable -> result_type {
                        if constexpr (std::is_void<result_type>::value) {
                            function(index);
                        } else {
                            return function(index);
                        }
                    }));
            }
            return futures;
        }

        const auto priority = resolve_task_priority(base_options, task_priority::normal);
        std::vector<task_item> items;
        items.reserve(count);
        for (std::size_t index = 0; index < count; ++index) {
            auto task_function = [function, index]() mutable -> result_type {
                if constexpr (std::is_void<result_type>::value) {
                    function(index);
                } else {
                    return function(index);
                }
            };
            using task_function_type = decltype(task_function);
            auto shared_callable = std::make_shared<task_function_type>(std::move(task_function));
            auto promise = std::make_shared<std::promise<result_type>>();
            futures.emplace_back(promise->get_future());

            task_item item;
            item.priority = priority;
            item.preferred_numa_node = base_options.numa_node;
            item.metadata = make_task_metadata(base_options, priority);
            item.enqueue_time = clock_type::now();
            item.should_cancel = [cancellation = base_options.cancellation, deadline = base_options.deadline] {
                return cancellation.stop_requested() ||
                    (deadline && clock_type::now() >= *deadline);
            };
            item.on_cancel = [promise] {
                cancel_promise<result_type>(promise, "task was cancelled before execution");
            };
            item.function = [promise, shared_callable]() mutable {
                invoke_into_promise<result_type>(promise, *shared_callable);
            };
            items.push_back(std::move(item));
        }

        enqueue_many_or_throw(std::move(items));
        return futures;
    }

    template <class F>
    std::size_t bulk_detach(std::size_t count, F&& f) {
        return bulk_detach_with_priority(
            task_priority::normal,
            count,
            std::forward<F>(f));
    }

    template <class F>
    std::size_t bulk_detach_with_priority(task_priority priority, std::size_t count, F&& f) {
        using function_type = std::decay_t<F>;
        static_assert(
            std::is_copy_constructible<function_type>::value,
            "bulk_detach requires a copy-constructible callable");

        function_type function(std::forward<F>(f));
        std::size_t accepted = 0;
        if (options_.queue != queue_policy::bounded_block &&
            options_.queue != queue_policy::bounded_caller_runs) {
            std::vector<task_item> items;
            items.reserve(count);
            for (std::size_t index = 0; index < count; ++index) {
                auto task_function = [function, index]() mutable {
                    function(index);
                };
                using task_function_type = decltype(task_function);
                auto shared_callable = std::make_shared<task_function_type>(std::move(task_function));

                task_item item;
                item.priority = priority;
                item.enqueue_time = clock_type::now();
                item.function = [shared_callable]() mutable {
                    (*shared_callable)();
                };
                items.push_back(std::move(item));
            }
            return enqueue_many_or_count(std::move(items));
        }

        for (std::size_t index = 0; index < count; ++index) {
            if (detach_with_priority(priority, [function, index]() mutable {
                    function(index);
                })) {
                ++accepted;
            }
        }

        return accepted;
    }

    // Copies task_options for each item. The cancellation token remains shared,
    // so cancelling it intentionally cancels the whole detached batch.
    template <class F>
    std::size_t bulk_detach_with_options(task_options options, std::size_t count, F&& f) {
        using function_type = std::decay_t<F>;
        static_assert(
            std::is_copy_constructible<function_type>::value,
            "bulk_detach_with_options requires a copy-constructible callable");

        function_type function(std::forward<F>(f));
        task_options base_options = std::move(options);
        if (options_.queue == queue_policy::bounded_block ||
            options_.queue == queue_policy::bounded_caller_runs) {
            std::size_t accepted = 0;
            for (std::size_t index = 0; index < count; ++index) {
                auto item_options = base_options;
                if (detach_with_options(std::move(item_options), [function, index]() mutable {
                        function(index);
                    })) {
                    ++accepted;
                }
            }
            return accepted;
        }

        const auto priority = resolve_task_priority(base_options, task_priority::normal);
        std::vector<task_item> items;
        items.reserve(count);
        for (std::size_t index = 0; index < count; ++index) {
            auto task_function = [function, index]() mutable {
                function(index);
            };
            using task_function_type = decltype(task_function);
            auto shared_callable = std::make_shared<task_function_type>(std::move(task_function));

            task_item item;
            item.priority = priority;
            item.preferred_numa_node = base_options.numa_node;
            item.metadata = make_task_metadata(base_options, priority);
            item.enqueue_time = clock_type::now();
            item.should_cancel = [cancellation = base_options.cancellation, deadline = base_options.deadline] {
                return cancellation.stop_requested() ||
                    (deadline && clock_type::now() >= *deadline);
            };
            item.function = [shared_callable]() mutable {
                (*shared_callable)();
            };
            items.push_back(std::move(item));
        }

        return enqueue_many_or_count(std::move(items));
    }

    template <class Rep, class Period, class F, class... Args>
    auto submit_after(std::chrono::duration<Rep, Period> delay, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        return submit_at(
            clock_type::now() + std::chrono::duration_cast<clock_type::duration>(delay),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    auto submit_at(std::chrono::time_point<Clock, Duration> time_point, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        auto scheduled = schedule_submit_at(
            time_point,
            std::forward<F>(f),
            std::forward<Args>(args)...);
        return std::move(scheduled.future);
    }

    template <class Rep, class Period, class F, class... Args>
    auto schedule_submit_after(
        std::chrono::duration<Rep, Period> delay,
        F&& f,
        Args&&... args)
        -> scheduled_future<std::invoke_result_t<F, Args...>> {
        return schedule_submit_at(
            clock_type::now() + std::chrono::duration_cast<clock_type::duration>(delay),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    auto schedule_submit_at(
        std::chrono::time_point<Clock, Duration> time_point,
        F&& f,
        Args&&... args)
        -> scheduled_future<std::invoke_result_t<F, Args...>> {
        return schedule_submit_at_with_priority(
            task_priority::normal,
            time_point,
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    auto schedule_submit_after_with_priority(
        task_priority priority,
        std::chrono::duration<Rep, Period> delay,
        F&& f,
        Args&&... args)
        -> scheduled_future<std::invoke_result_t<F, Args...>> {
        return schedule_submit_at_with_priority(
            priority,
            clock_type::now() + std::chrono::duration_cast<clock_type::duration>(delay),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    auto schedule_submit_at_with_priority(
        task_priority priority,
        std::chrono::time_point<Clock, Duration> time_point,
        F&& f,
        Args&&... args)
        -> scheduled_future<std::invoke_result_t<F, Args...>> {
        using result_type = std::invoke_result_t<F, Args...>;

        auto callable = make_callable<result_type>(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));
        auto promise = std::make_shared<std::promise<result_type>>();
        auto future = promise->get_future();

        task_item item;
        item.priority = priority;
        item.enqueue_time = clock_type::now();
        item.on_cancel = [promise] {
            cancel_promise<result_type>(promise, "scheduled task was cancelled before execution");
        };
        item.function = [promise, shared_callable]() mutable {
            invoke_into_promise<result_type>(promise, *shared_callable);
        };

        auto handle = schedule_task_at(to_steady_time(time_point), std::move(item), true);
        return scheduled_future<result_type>{std::move(future), std::move(handle)};
    }

    template <class Rep, class Period, class F, class... Args>
    auto schedule_submit_after_with_options(
        task_options options,
        std::chrono::duration<Rep, Period> delay,
        F&& f,
        Args&&... args)
        -> scheduled_future<std::invoke_result_t<F, Args...>> {
        return schedule_submit_at_with_options(
            std::move(options),
            clock_type::now() + std::chrono::duration_cast<clock_type::duration>(delay),
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
        using result_type = std::invoke_result_t<F, Args...>;
        const auto priority = resolve_task_priority(options, task_priority::normal);

        auto callable = make_callable<result_type>(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));
        auto promise = std::make_shared<std::promise<result_type>>();
        auto future = promise->get_future();

        task_item item;
        item.priority = priority;
        item.preferred_numa_node = options.numa_node;
        item.metadata = make_task_metadata(options, priority);
        item.enqueue_time = clock_type::now();
        item.should_cancel = [cancellation = options.cancellation, deadline = options.deadline] {
            return cancellation.stop_requested() ||
                (deadline && clock_type::now() >= *deadline);
        };
        item.on_cancel = [promise] {
            cancel_promise<result_type>(promise, "scheduled task was cancelled before execution");
        };
        item.function = [promise, shared_callable]() mutable {
            invoke_into_promise<result_type>(promise, *shared_callable);
        };

        auto handle = schedule_task_at(to_steady_time(time_point), std::move(item), true);
        return scheduled_future<result_type>{std::move(future), std::move(handle)};
    }

    template <class Rep, class Period, class F, class... Args>
    scheduled_task_handle detach_after(
        std::chrono::duration<Rep, Period> delay,
        F&& f,
        Args&&... args) {
        return detach_at(
            clock_type::now() + std::chrono::duration_cast<clock_type::duration>(delay),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    scheduled_task_handle detach_at(
        std::chrono::time_point<Clock, Duration> time_point,
        F&& f,
        Args&&... args) {
        auto callable = make_void_callable(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);
        auto shared_callable = std::make_shared<callable_type>(std::move(callable));

        task_item item;
        item.priority = task_priority::normal;
        item.enqueue_time = clock_type::now();
        item.function = [shared_callable]() mutable {
            (*shared_callable)();
        };

        return schedule_task_at(to_steady_time(time_point), std::move(item), false);
    }

    template <class Rep, class Period, class F, class... Args>
    periodic_task_handle detach_periodic(
        std::chrono::duration<Rep, Period> interval,
        F&& f,
        Args&&... args) {
        periodic_options periodic;
        periodic.interval = std::chrono::duration_cast<std::chrono::nanoseconds>(interval);
        return detach_periodic_with_options(
            std::move(periodic),
            task_options{},
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    periodic_task_handle detach_periodic_with_options(
        periodic_options periodic,
        task_options options,
        F&& f,
        Args&&... args) {
        using function_type = std::decay_t<F>;
        using args_tuple_type = std::tuple<std::decay_t<Args>...>;

        if (periodic.interval.count() <= 0) {
            periodic.interval = std::chrono::milliseconds{1};
        }
        if (periodic.initial_delay.count() < 0) {
            periodic.initial_delay = std::chrono::nanoseconds{0};
        }

        struct periodic_runner : std::enable_shared_from_this<periodic_runner> {
            thread_pool* pool = nullptr;
            std::shared_ptr<periodic_task_state> state;
            periodic_options periodic;
            task_options options;
            function_type function;
            args_tuple_type args_tuple;

            periodic_runner(
                thread_pool* owner,
                std::shared_ptr<periodic_task_state> run_state,
                periodic_options periodic_config,
                task_options task_config,
                function_type task_function,
                args_tuple_type task_args)
                : pool(owner),
                  state(std::move(run_state)),
                  periodic(std::move(periodic_config)),
                  options(std::move(task_config)),
                  function(std::move(task_function)),
                  args_tuple(std::move(task_args)) {}

            void schedule(std::chrono::steady_clock::time_point due_time) {
                if (!pool || state->cancelled()) {
                    return;
                }

                auto self = this->shared_from_this();
                auto schedule_options = options;
                try {
                    auto scheduled = pool->schedule_submit_at_with_options(
                        std::move(schedule_options),
                        due_time,
                        [self]() mutable {
                            self->run_once();
                        });
                    state->set_pending(std::move(scheduled.handle));
                } catch (...) {
                    state->request_cancel();
                }
            }

            void run_once() {
                if (state->cancelled()) {
                    return;
                }

                std::exception_ptr failure;
                try {
                    std::apply(function, args_tuple);
                } catch (...) {
                    failure = std::current_exception();
                }

                const auto runs = state->record_run();
                const bool can_continue_after_error = !failure || periodic.continue_on_error;
                const bool under_run_limit =
                    periodic.max_runs == 0 || runs < periodic.max_runs;

                if (!can_continue_after_error) {
                    state->request_cancel();
                } else if (!state->cancelled() && under_run_limit) {
                    schedule(std::chrono::steady_clock::now() + periodic.interval);
                }

                if (failure) {
                    std::rethrow_exception(failure);
                }
            }
        };

        auto state = std::make_shared<periodic_task_state>();
        auto runner = std::make_shared<periodic_runner>(
            this,
            state,
            std::move(periodic),
            std::move(options),
            function_type(std::forward<F>(f)),
            args_tuple_type(std::forward<Args>(args)...));

        const auto first_delay = runner->periodic.run_immediately
            ? std::chrono::nanoseconds{0}
            : (runner->periodic.initial_delay.count() > 0
                ? runner->periodic.initial_delay
                : runner->periodic.interval);
        runner->schedule(std::chrono::steady_clock::now() + first_delay);
        return periodic_task_handle{state};
    }

    template <class T, class F>
    auto submit_then(std::future<T> predecessor, F&& continuation)
        -> std::future<detail::continuation_result_t<T, F>> {
        return continue_with(
            std::move(predecessor),
            std::forward<F>(continuation));
    }

    template <class T, class F>
    auto continue_with(std::future<T> predecessor, F&& continuation)
        -> std::future<detail::continuation_result_t<T, F>> {
        return continue_with_priority(
            task_priority::normal,
            std::move(predecessor),
            std::forward<F>(continuation));
    }

    template <class T, class F>
    auto continue_on(executor completion, std::future<T> predecessor, F&& continuation)
        -> std::future<detail::continuation_result_t<T, F>> {
        using result_type = detail::continuation_result_t<T, F>;
        using function_type = std::decay_t<F>;

        if (!completion) {
            throw thread_pool_closed("completion executor does not reference a thread pool");
        }

        auto promise = std::make_shared<std::promise<result_type>>();
        auto result = promise->get_future();
        function_type function(std::forward<F>(continuation));

        start_continuation_waiter(
            [completion = std::move(completion),
             predecessor = std::move(predecessor),
             function = std::move(function),
             promise]() mutable {
                try {
                    if constexpr (std::is_void<T>::value) {
                        predecessor.get();
                        auto continuation_future = completion.submit(
                            [function = std::move(function)]() mutable -> result_type {
                                if constexpr (std::is_void<result_type>::value) {
                                    function();
                                } else {
                                    return function();
                                }
                            });
                        thread_pool::complete_promise_from_future<result_type>(
                            promise,
                            std::move(continuation_future));
                    } else {
                        auto value = predecessor.get();
                        auto continuation_future = completion.submit(
                            [function = std::move(function),
                             value = std::move(value)]() mutable -> result_type {
                                if constexpr (std::is_void<result_type>::value) {
                                    function(std::move(value));
                                } else {
                                    return function(std::move(value));
                                }
                            });
                        thread_pool::complete_promise_from_future<result_type>(
                            promise,
                            std::move(continuation_future));
                    }
                } catch (...) {
                    thread_pool::set_promise_exception<result_type>(
                        promise,
                        std::current_exception());
                }
            });

        return result;
    }

    template <class T, class F>
    auto continue_with_priority(
        task_priority priority,
        std::future<T> predecessor,
        F&& continuation)
        -> std::future<detail::continuation_result_t<T, F>> {
        return continue_on(
            get_executor(priority),
            std::move(predecessor),
            std::forward<F>(continuation));
    }

    template <class T, class F>
    auto dataflow(std::vector<std::future<T>> predecessors, F&& continuation)
        -> std::future<detail::dataflow_result_t<T, F>> {
        return dataflow_with_priority(
            task_priority::normal,
            std::move(predecessors),
            std::forward<F>(continuation));
    }

    template <class T, class F>
    auto dataflow_on(
        executor completion,
        std::vector<std::future<T>> predecessors,
        F&& continuation)
        -> std::future<detail::dataflow_result_t<T, F>> {
        using result_type = detail::dataflow_result_t<T, F>;
        using function_type = std::decay_t<F>;

        if (!completion) {
            throw thread_pool_closed("completion executor does not reference a thread pool");
        }

        auto promise = std::make_shared<std::promise<result_type>>();
        auto result = promise->get_future();
        function_type function(std::forward<F>(continuation));

        start_continuation_waiter(
            [completion = std::move(completion),
             predecessors = std::move(predecessors),
             function = std::move(function),
             promise]() mutable {
                try {
                    if constexpr (std::is_void<T>::value) {
                        for (auto& predecessor : predecessors) {
                            predecessor.get();
                        }

                        auto continuation_future = completion.submit(
                            [function = std::move(function)]() mutable -> result_type {
                                if constexpr (std::is_void<result_type>::value) {
                                    function();
                                } else {
                                    return function();
                                }
                            });
                        thread_pool::complete_promise_from_future<result_type>(
                            promise,
                            std::move(continuation_future));
                    } else {
                        std::vector<T> values;
                        values.reserve(predecessors.size());
                        for (auto& predecessor : predecessors) {
                            values.push_back(predecessor.get());
                        }

                        auto continuation_future = completion.submit(
                            [function = std::move(function),
                             values = std::move(values)]() mutable -> result_type {
                                if constexpr (std::is_void<result_type>::value) {
                                    function(std::move(values));
                                } else {
                                    return function(std::move(values));
                                }
                            });
                        thread_pool::complete_promise_from_future<result_type>(
                            promise,
                            std::move(continuation_future));
                    }
                } catch (...) {
                    thread_pool::set_promise_exception<result_type>(
                        promise,
                        std::current_exception());
                }
            });

        return result;
    }

    template <class T, class F>
    auto dataflow_with_priority(
        task_priority priority,
        std::vector<std::future<T>> predecessors,
        F&& continuation)
        -> std::future<detail::dataflow_result_t<T, F>> {
        return dataflow_on(
            get_executor(priority),
            std::move(predecessors),
            std::forward<F>(continuation));
    }

    template <class T>
    auto when_all(std::vector<std::future<T>> futures)
        -> std::future<std::vector<T>> {
        auto promise = std::make_shared<std::promise<std::vector<T>>>();
        auto result = promise->get_future();

        start_continuation_waiter(
            [futures = std::move(futures), promise]() mutable {
                std::vector<T> results;
                results.reserve(futures.size());
                std::exception_ptr first_error;

                for (auto& future : futures) {
                    try {
                        if (first_error) {
                            (void)future.get();
                        } else {
                            results.push_back(future.get());
                        }
                    } catch (...) {
                        if (!first_error) {
                            first_error = std::current_exception();
                        }
                    }
                }

                if (first_error) {
                    thread_pool::set_promise_exception<std::vector<T>>(
                        promise,
                        first_error);
                    return;
                }

                try {
                    promise->set_value(std::move(results));
                } catch (...) {
                    thread_pool::set_promise_exception<std::vector<T>>(
                        promise,
                        std::current_exception());
                }
            });

        return result;
    }

    std::future<void> when_all(std::vector<std::future<void>> futures);

    // Runs the wait loop on the continuation scheduler, not on normal workers.
    // Completion is polled with 1ms granularity, so readiness can be delayed by
    // up to one polling interval.
    template <class T>
    auto when_any(std::vector<std::future<T>> futures)
        -> std::future<when_any_result<T>> {
        if (futures.empty()) {
            throw std::invalid_argument("when_any requires at least one future");
        }

        auto promise = std::make_shared<std::promise<when_any_result<T>>>();
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
                                promise->set_value(when_any_result<T>{i, futures[i].get()});
                                return;
                            }
                        }

                        if (!has_valid_future) {
                            throw std::invalid_argument("when_any requires at least one valid future");
                        }

                        std::this_thread::sleep_for(std::chrono::milliseconds{1});
                    }
                } catch (...) {
                    thread_pool::set_promise_exception<when_any_result<T>>(
                        promise,
                        std::current_exception());
                }
            });

        return result;
    }

    // Same continuation-scheduler and 1ms polling semantics as the typed
    // overload.
    std::future<when_any_result<void>> when_any(std::vector<std::future<void>> futures);

    template <class F>
    bool post(F&& f) {
        return detach(std::forward<F>(f));
    }

    template <class F>
    bool defer(F&& f) {
        return detach(std::forward<F>(f));
    }

    // If called from this pool's worker, dispatch runs inline as a fast path.
    // It updates submitted/completed/failed counters but intentionally skips
    // task lifecycle hooks and wait/run latency histograms.
    template <class F>
    bool dispatch(F&& f) {
        if (current_pool_ == this) {
            submitted_tasks_total_.fetch_add(1, std::memory_order_relaxed);
            try {
                std::forward<F>(f)();
                completed_tasks_total_.fetch_add(1, std::memory_order_relaxed);
                if (current_worker_) {
                    current_worker_->completed_tasks.fetch_add(1, std::memory_order_relaxed);
                }
                return true;
            } catch (...) {
                failed_tasks_total_.fetch_add(1, std::memory_order_relaxed);
                if (current_worker_) {
                    current_worker_->failed_tasks.fetch_add(1, std::memory_order_relaxed);
                }
                handle_unhandled_exception(std::current_exception());
                return false;
            }
        }

        return detach(std::forward<F>(f));
    }

    template <class Index, class F>
    void parallel_for(Index first, Index last, F&& f, loop_options options = {}) {
        if (!(first < last)) {
            return;
        }

        using diff_type = decltype(last - first);
        const auto total_signed = static_cast<diff_type>(last - first);
        const auto total = static_cast<std::size_t>(total_signed);
        if (total == 0) {
            return;
        }

        if (options_.enable_deadlock_check && current_pool_ == this) {
            for (Index i = first; i < last; ++i) {
                f(i);
            }
            return;
        }

        const auto threads = std::max<std::size_t>(1, total_threads_.load(std::memory_order_acquire));
        std::size_t block_size = options.block_size;
        if (block_size == 0) {
            block_size = std::max<std::size_t>(1, total / (threads * 4));
            if (block_size == 0) {
                block_size = 1;
            }
        }

        const auto blocks = (total + block_size - 1) / block_size;
        std::vector<std::future<void>> futures;
        futures.reserve(options.schedule == loop_schedule::static_blocks
                            ? blocks
                            : std::min(blocks, threads));

        auto run_block = [&](std::size_t block_first_offset, std::size_t block_last_offset) {
            const auto block_first = static_cast<Index>(
                first + static_cast<Index>(block_first_offset));
            const auto block_last = static_cast<Index>(
                first + static_cast<Index>(block_last_offset));

            for (Index i = block_first; i < block_last; ++i) {
                f(i);
            }
        };

        if (options.schedule == loop_schedule::static_blocks) {
            for (std::size_t block = 0; block < blocks; ++block) {
                const auto block_first = block * block_size;
                const auto block_last = std::min<std::size_t>(total, (block + 1) * block_size);

                futures.emplace_back(submit_with_priority(options.priority, [&, block_first, block_last]() {
                    run_block(block_first, block_last);
                }));
            }
        } else {
            const auto worker_tasks = std::min(blocks, threads);
            auto next_offset = std::make_shared<std::atomic<std::size_t>>(0);

            for (std::size_t task = 0; task < worker_tasks; ++task) {
                futures.emplace_back(submit_with_priority(options.priority, [&, next_offset, block_size, total, threads]() {
                    while (true) {
                        if (options.schedule == loop_schedule::dynamic_blocks) {
                            const auto block_first = next_offset->fetch_add(
                                block_size,
                                std::memory_order_relaxed);
                            if (block_first >= total) {
                                break;
                            }

                            const auto block_last = std::min<std::size_t>(total, block_first + block_size);
                            run_block(block_first, block_last);
                            continue;
                        }

                        auto block_first = next_offset->load(std::memory_order_relaxed);
                        if (block_first >= total) {
                            break;
                        }

                        while (block_first < total) {
                            const auto remaining = total - block_first;
                            const auto guided_size = std::max<std::size_t>(
                                block_size,
                                remaining / std::max<std::size_t>(1, threads * 2));
                            const auto block_last = std::min<std::size_t>(
                                total,
                                block_first + guided_size);

                            if (next_offset->compare_exchange_weak(
                                    block_first,
                                    block_last,
                                    std::memory_order_relaxed,
                                    std::memory_order_relaxed)) {
                                run_block(block_first, block_last);
                                break;
                            }
                        }
                    }
                }));
            }
        }

        detail::get_all_futures_or_rethrow(futures);
    }

    template <class Iterator, class F>
    void parallel_for_each(Iterator first, Iterator last, F&& f, loop_options options = {}) {
        const auto count = std::distance(first, last);
        if (count <= 0) {
            return;
        }

        using diff_type = std::remove_cv_t<decltype(count)>;
        using category = typename std::iterator_traits<Iterator>::iterator_category;
        parallel_for<diff_type>(0, count, [first, &f](diff_type offset) {
            if constexpr (std::is_base_of<std::random_access_iterator_tag, category>::value) {
                f(*(first + offset));
            } else {
                auto it = first;
                std::advance(it, offset);
                f(*it);
            }
        }, options);
    }

    template <class InputIterator, class OutputIterator, class F>
    void parallel_transform(
        InputIterator first,
        InputIterator last,
        OutputIterator output,
        F&& f,
        loop_options options = {}) {
        const auto count = std::distance(first, last);
        if (count <= 0) {
            return;
        }

        using diff_type = std::remove_cv_t<decltype(count)>;
        using input_category = typename std::iterator_traits<InputIterator>::iterator_category;
        using output_category = typename std::iterator_traits<OutputIterator>::iterator_category;
        parallel_for<diff_type>(0, count, [first, output, &f](diff_type offset) {
            auto input_it = first;
            if constexpr (std::is_base_of<std::random_access_iterator_tag, input_category>::value) {
                input_it = first + offset;
            } else {
                std::advance(input_it, offset);
            }

            auto output_it = output;
            if constexpr (std::is_base_of<std::random_access_iterator_tag, output_category>::value) {
                output_it = output + offset;
            } else {
                std::advance(output_it, offset);
            }

            *output_it = f(*input_it);
        }, options);
    }

    template <class Index, class Value, class Map, class Reduce>
    Value parallel_transform_reduce(
        Index first,
        Index last,
        Value init,
        Map&& map,
        Reduce&& reduce,
        loop_options options = {}) {
        if (!(first < last)) {
            return init;
        }

        using diff_type = decltype(last - first);
        const auto total = static_cast<std::size_t>(static_cast<diff_type>(last - first));
        if (options_.enable_deadlock_check && current_pool_ == this) {
            Value result = std::move(init);
            for (Index i = first; i < last; ++i) {
                result = reduce(std::move(result), map(i));
            }
            return result;
        }

        const auto threads = std::max<std::size_t>(1, total_threads_.load(std::memory_order_acquire));

        std::size_t block_size = options.block_size;
        if (block_size == 0) {
            block_size = std::max<std::size_t>(1, total / (threads * 4));
            if (block_size == 0) {
                block_size = 1;
            }
        }

        const auto blocks = (total + block_size - 1) / block_size;
        std::vector<std::future<Value>> futures;
        futures.reserve(blocks);

        for (std::size_t block = 0; block < blocks; ++block) {
            const auto block_first_offset = block * block_size;
            const auto block_last_offset = std::min<std::size_t>(total, (block + 1) * block_size);

            futures.emplace_back(submit_with_priority(
                options.priority,
                [=, &map, &reduce]() mutable {
                    std::optional<Value> local;

                    const auto block_first = static_cast<Index>(
                        first + static_cast<Index>(block_first_offset));
                    const auto block_last = static_cast<Index>(
                        first + static_cast<Index>(block_last_offset));

                    for (Index i = block_first; i < block_last; ++i) {
                        auto mapped = map(i);
                        if (!local) {
                            local.emplace(std::move(mapped));
                        } else {
                            local.emplace(reduce(std::move(*local), std::move(mapped)));
                        }
                    }

                    return std::move(*local);
                }));
        }

        std::exception_ptr first_error;
        for (auto& future : futures) {
            try {
                auto partial = future.get();
                if (!first_error) {
                    init = reduce(std::move(init), std::move(partial));
                }
            } catch (...) {
                if (!first_error) {
                    first_error = std::current_exception();
                }
            }
        }

        if (first_error) {
            std::rethrow_exception(first_error);
        }

        return init;
    }

    template <class Index, class Value, class Reduce>
    Value parallel_reduce(
        Index first,
        Index last,
        Value init,
        Reduce&& reduce,
        loop_options options = {}) {
        return parallel_transform_reduce(
            first,
            last,
            std::move(init),
            [](Index i) {
                return i;
            },
            std::forward<Reduce>(reduce),
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
        parallel_for_nd<Index, 2>(
            std::array<Index, 2>{x_first, y_first},
            std::array<Index, 2>{x_last, y_last},
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
        parallel_for_nd<Index, 3>(
            std::array<Index, 3>{x_first, y_first, z_first},
            std::array<Index, 3>{x_last, y_last, z_last},
            std::forward<F>(f),
            options);
    }

    template <class Index, std::size_t Dimensions, class F>
    void parallel_for_nd(
        const std::array<Index, Dimensions>& first,
        const std::array<Index, Dimensions>& last,
        F&& f,
        loop_options options = {}) {
        static_assert(Dimensions > 0, "parallel_for_nd requires at least one dimension");

        std::array<std::size_t, Dimensions> extents{};
        std::size_t total = 1;
        for (std::size_t dimension = 0; dimension < Dimensions; ++dimension) {
            if (!(first[dimension] < last[dimension])) {
                return;
            }

            const auto extent = static_cast<std::size_t>(last[dimension] - first[dimension]);
            extents[dimension] = extent;
            total *= extent;
        }

        parallel_for<std::size_t>(0, total, [&](std::size_t linear_index) {
            std::array<Index, Dimensions> point{};
            auto offset = linear_index;

            for (std::size_t reverse = Dimensions; reverse > 0; --reverse) {
                const auto dimension = reverse - 1;
                const auto extent = extents[dimension];
                const auto coordinate = offset % extent;
                offset /= extent;
                point[dimension] = static_cast<Index>(
                    first[dimension] + static_cast<Index>(coordinate));
            }

            std::apply(f, point);
        }, options);
    }

    class managed_blocking_scope {
    public:
        explicit managed_blocking_scope(thread_pool& pool) noexcept;
        ~managed_blocking_scope();

        managed_blocking_scope(const managed_blocking_scope&) = delete;
        managed_blocking_scope& operator=(const managed_blocking_scope&) = delete;

        managed_blocking_scope(managed_blocking_scope&& other) noexcept;
        managed_blocking_scope& operator=(managed_blocking_scope&& other) noexcept;

        explicit operator bool() const noexcept {
            return pool_ != nullptr;
        }

    private:
        thread_pool* pool_ = nullptr;
    };

    bool is_worker_thread() const noexcept;
    static std::size_t current_worker_id() noexcept;

    executor get_executor(task_priority priority = task_priority::normal) noexcept;

    managed_blocking_scope managed_blocking() noexcept;

    template <class F>
    auto run_managed_blocking(F&& f) -> std::invoke_result_t<F&&> {
        auto scope = managed_blocking();
        (void)scope;
        if constexpr (std::is_void<std::invoke_result_t<F&&>>::value) {
            std::forward<F>(f)();
        } else {
            return std::forward<F>(f)();
        }
    }

    template <class T>
    void wait_with_help(std::future<T>& future) {
        while (future.wait_for(std::chrono::nanoseconds{0}) != std::future_status::ready) {
            if (!try_run_one_inline()) {
                std::this_thread::yield();
            }
        }
    }

    template <class T>
    T get_with_help(std::future<T> future) {
        wait_with_help(future);
        if constexpr (std::is_void<T>::value) {
            future.get();
        } else {
            return future.get();
        }
    }

#if UNIVERSAL_THREAD_POOL_HAS_COROUTINE
    schedule_awaiter schedule(task_priority priority = task_priority::normal) noexcept {
        return get_executor(priority).schedule();
    }

    template <class F, class... Args>
    auto submit_awaitable(F&& f, Args&&... args)
        -> submit_awaiter<
            detail::bound_task<detail::bound_result_t<F, Args...>, F, Args...>> {
        return get_executor().submit_awaitable(
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class T>
    future_awaiter<T> await_future(std::future<T> future) {
        return get_executor().await_future(std::move(future));
    }
#endif

    void pause();
    void resume();
    void resize(std::size_t new_thread_count);
    void shutdown(shutdown_policy policy = shutdown_policy::drain);
    // Waits for ready/running work to drain. Delayed tasks scheduled for the
    // future do not make workers busy until the timer promotes them.
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
    thread_pool_metrics metrics() const;
    std::size_t thread_count() const noexcept;

private:
    struct task_item {
        std::function<void()> function;
        std::function<bool()> should_cancel;
        std::function<void()> on_cancel;
        task_metadata metadata;
        task_priority priority = task_priority::normal;
        std::optional<std::size_t> preferred_numa_node;
        std::uint64_t sequence = 0;
        clock_type::time_point enqueue_time = clock_type::now();
    };

    struct delayed_task {
        task_item item;
        std::shared_ptr<scheduled_task_state> state;
    };

    struct worker_control {
        std::size_t id = 0;
        std::size_t numa_node = 0;
        std::thread thread;
        std::atomic<bool> finished{false};
        std::atomic<std::uint64_t> completed_tasks{0};
        std::atomic<std::uint64_t> failed_tasks{0};
        std::atomic<std::uint64_t> cancelled_tasks{0};
        std::mutex local_mutex;
        std::deque<task_item> local_queue;
    };

    template <class Result, class F, class... Args>
    static auto make_callable(F&& f, Args&&... args) {
        return [func = std::forward<F>(f),
                tuple = std::make_tuple(std::forward<Args>(args)...)]() mutable -> Result {
            if constexpr (std::is_void<Result>::value) {
                std::apply(std::move(func), std::move(tuple));
            } else {
                return std::apply(std::move(func), std::move(tuple));
            }
        };
    }

    template <class F, class... Args>
    static auto make_void_callable(F&& f, Args&&... args) {
        return [func = std::forward<F>(f),
                tuple = std::make_tuple(std::forward<Args>(args)...)]() mutable {
            std::apply(std::move(func), std::move(tuple));
        };
    }

    template <class Result, class Callable>
    static void invoke_into_promise(
        const std::shared_ptr<std::promise<Result>>& promise,
        Callable& callable) {
        try {
            if constexpr (std::is_void<Result>::value) {
                callable();
                promise->set_value();
            } else {
                promise->set_value(callable());
            }
        } catch (...) {
            try {
                promise->set_exception(std::current_exception());
            } catch (...) {
            }
        }
    }

    template <class Result>
    static void set_promise_exception(
        const std::shared_ptr<std::promise<Result>>& promise,
        std::exception_ptr error) noexcept {
        try {
            promise->set_exception(std::move(error));
        } catch (...) {
        }
    }

    template <class Result>
    static void complete_promise_from_future(
        const std::shared_ptr<std::promise<Result>>& promise,
        std::future<Result> future) noexcept {
        try {
            if constexpr (std::is_void<Result>::value) {
                future.get();
                promise->set_value();
            } else {
                promise->set_value(future.get());
            }
        } catch (...) {
            set_promise_exception<Result>(promise, std::current_exception());
        }
    }

    template <class Result>
    static void cancel_promise(
        const std::shared_ptr<std::promise<Result>>& promise,
        const char* message) noexcept {
        try {
            promise->set_exception(std::make_exception_ptr(task_cancelled(message)));
        } catch (...) {
        }
    }

    static thread_pool_options normalize_options(thread_pool_options options);

    template <class F>
    void start_continuation_waiter(F&& waiter) {
        auto task = std::packaged_task<void()>(
            [waiter_task = std::forward<F>(waiter)]() mutable {
                try {
                    waiter_task();
                } catch (...) {
                }
            });

        {
            std::unique_lock<std::mutex> lock(continuations_mutex_);
            if (!continuation_stopping_) {
                continuation_tasks_.push_back(std::move(task));
                lock.unlock();
                continuation_cv_.notify_one();
                return;
            }
        }

        task();
    }

    template <class Clock, class Duration>
    static clock_type::time_point to_steady_time(
        std::chrono::time_point<Clock, Duration> time_point) {
        if constexpr (std::is_same<Clock, clock_type>::value) {
            return std::chrono::time_point_cast<clock_type::duration>(time_point);
        } else {
            const auto source_now = Clock::now();
            const auto steady_now = clock_type::now();
            if (time_point <= source_now) {
                return steady_now;
            }
            return steady_now + std::chrono::duration_cast<clock_type::duration>(time_point - source_now);
        }
    }

    scheduled_task_handle schedule_task_at(
        clock_type::time_point due_time,
        task_item item,
        bool throw_on_failure);

    void enqueue_or_throw(task_item item);
    bool enqueue_or_false(task_item item);
    bool try_enqueue(task_item item);
    bool try_enqueue_until(task_item item, clock_type::time_point deadline);
    bool enqueue_impl(
        task_item item,
        bool throw_on_failure,
        bool allow_block,
        std::optional<clock_type::time_point> wait_deadline);
    void enqueue_many_or_throw(std::vector<task_item> items);
    std::size_t enqueue_many_or_count(std::vector<task_item> items);
    std::size_t enqueue_many_impl(
        std::vector<task_item> items,
        bool throw_on_failure);
    void push_task_locked(task_item item);
    void push_local_task_locked(worker_control* worker, task_item item);
    bool should_use_local_queue_locked(const task_item& item) const noexcept;
    void record_queue_size_locked();
    bool pop_task_locked(worker_control* worker, task_item& item);
    bool pop_local_task_locked(worker_control* worker, task_item& item);
    bool steal_task_locked(worker_control* thief, task_item& item);
    bool pop_next_task_locked(worker_control* worker, task_item& item);
    std::size_t pop_task_batch_locked(
        worker_control* worker,
        std::vector<task_item>& batch,
        std::size_t max_items);
    bool drop_oldest_locked(std::vector<task_metadata>& cancelled_metadata);
    void clear_queues_locked(std::vector<task_metadata>& cancelled_metadata);
    void cancel_pending_item(
        task_item& item,
        std::vector<task_metadata>& cancelled_metadata) noexcept;
    void maybe_grow_for_pending_task_locked();
    bool should_retire_idle_worker_locked() const noexcept;
    bool reserve_idle_retirement_locked() noexcept;
    static task_priority resolve_task_priority(
        const task_options& options,
        task_priority fallback) noexcept;
    static std::size_t priority_bucket(task_priority priority) noexcept;
    static std::size_t latency_bucket_index(std::uint64_t nanoseconds) noexcept;
    static task_metadata make_task_metadata(
        const task_options& options,
        task_priority priority);
    using latency_atomic_buckets =
        std::array<std::atomic<std::uint64_t>, metric_latency_bucket_count>;
    static void record_latency_bucket(
        latency_atomic_buckets& buckets,
        std::uint64_t nanoseconds) noexcept;
    void assign_task_metadata_locked(task_item& item);
    void notify_task_queued(const task_metadata& metadata) const noexcept;
    void notify_task_start(const task_metadata& metadata) const noexcept;
    void notify_task_finish(const task_metadata& metadata) const noexcept;
    void notify_task_cancel(const task_metadata& metadata) const noexcept;
    void notify_task_error(const task_metadata& metadata, std::exception_ptr error) const noexcept;
    static std::string make_thread_name(
        const char* role,
        std::size_t id,
        const thread_pool_options& options);
    static void set_current_thread_name(const std::string& name) noexcept;
    static bool set_current_thread_affinity(std::size_t cpu_id) noexcept;
    void start_worker_unlocked();
    void worker_loop(worker_control* control) noexcept;
    void timer_loop() noexcept;
    void start_continuation_workers_unlocked();
    void continuation_loop() noexcept;
    void stop_continuation_workers() noexcept;
    void execute_task(task_item& item) noexcept;
    void finish_active_task();
    bool try_run_one_inline();
    bool begin_managed_blocking() noexcept;
    void end_managed_blocking() noexcept;
    void maybe_start_blocking_compensation_worker() noexcept;
    void join_finished_workers_unlocked();
    void handle_unhandled_exception(std::exception_ptr error) const noexcept;

    thread_pool_options options_;

    mutable std::mutex mutex_;
    std::condition_variable task_cv_;
    std::condition_variable queue_space_cv_;
    std::condition_variable idle_cv_;

    std::array<std::deque<task_item>, 4> queues_;
    std::size_t queued_size_ = 0;
    std::size_t active_tasks_ = 0;
    std::size_t target_threads_ = 0;
    std::size_t retire_requests_ = 0;
    std::size_t resize_retirements_ = 0;
    std::size_t idle_retirements_ = 0;
    std::size_t priority_strict_picks_ = 0;

    bool accepting_ = true;
    bool paused_ = false;
    bool stopping_ = false;
    bool stop_immediately_ = false;

    mutable std::mutex workers_mutex_;
    std::vector<std::unique_ptr<worker_control>> workers_;
    std::size_t next_worker_id_ = 0;
    std::uint64_t next_sequence_ = 0;
    std::uint64_t next_task_id_ = 1;

    std::mutex continuations_mutex_;
    std::condition_variable continuation_cv_;
    std::deque<std::packaged_task<void()>> continuation_tasks_;
    std::vector<std::thread> continuation_threads_;
    bool continuation_stopping_ = false;

    mutable std::mutex delay_mutex_;
    std::condition_variable delay_cv_;
    std::multimap<clock_type::time_point, delayed_task> delayed_tasks_;
    std::thread timer_thread_;
    bool timer_stopping_ = false;

    alignas(cache_line_size) std::atomic<std::uint64_t> submitted_tasks_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> completed_tasks_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> failed_tasks_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> cancelled_tasks_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> rejected_tasks_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> caller_runs_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> scheduled_tasks_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> scheduled_tasks_cancelled_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> local_tasks_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> steal_success_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> steal_fail_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> priority_fairness_picks_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> managed_blocking_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> managed_blocking_compensations_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> thread_affinity_applied_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> thread_affinity_failed_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> worker_wakeup_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> worker_idle_timeout_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> worker_retired_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> task_wait_time_ns_total_{0};
    alignas(cache_line_size) std::atomic<std::uint64_t> task_run_time_ns_total_{0};
    latency_atomic_buckets task_wait_time_buckets_{};
    latency_atomic_buckets task_run_time_buckets_{};
    std::atomic<std::uint64_t> max_queue_size_seen_{0};
    alignas(cache_line_size) std::atomic<std::size_t> running_tasks_{0};
    alignas(cache_line_size) std::atomic<std::size_t> total_threads_{0};
    alignas(cache_line_size) std::atomic<std::size_t> blocked_workers_{0};

    static thread_local thread_pool* current_pool_;
    static thread_local std::size_t current_worker_id_;
    static thread_local worker_control* current_worker_;
};

thread_pool& global_thread_pool();

#if UNIVERSAL_THREAD_POOL_HAS_COROUTINE
template <class F, class... Args>
auto executor::submit_awaitable(F&& f, Args&&... args) const
    -> submit_awaiter<
        detail::bound_task<detail::bound_result_t<F, Args...>, F, Args...>> {
    using result_type = detail::bound_result_t<F, Args...>;
    using callable_type = detail::bound_task<result_type, F, Args...>;

    return submit_awaiter<callable_type>{
        *this,
        callable_type{std::forward<F>(f), std::forward<Args>(args)...}};
}

template <class T>
future_awaiter<T> executor::await_future(std::future<T> future) const {
    return future_awaiter<T>{*this, std::move(future)};
}
#endif

template <class F, class... Args>
auto executor::submit(F&& f, Args&&... args) const
    -> std::future<std::invoke_result_t<F, Args...>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->submit_with_priority(
        priority_,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class F, class... Args>
auto executor::submit_with_options(task_options options, F&& f, Args&&... args) const
    -> std::future<std::invoke_result_t<F, Args...>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    apply_default_priority(options, priority_);
    return pool_->submit_with_options(
        std::move(options),
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class F, class... Args>
auto executor::try_submit(F&& f, Args&&... args) const
    -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
    if (!pool_) {
        return std::nullopt;
    }
    return pool_->try_submit_with_priority(
        priority_,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class F, class... Args>
auto executor::try_submit_with_options(task_options options, F&& f, Args&&... args) const
    -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
    if (!pool_) {
        return std::nullopt;
    }
    apply_default_priority(options, priority_);
    return pool_->try_submit_with_options(
        std::move(options),
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class Rep, class Period, class F, class... Args>
auto executor::try_submit_for(
    std::chrono::duration<Rep, Period> timeout,
    F&& f,
    Args&&... args) const
    -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
    if (!pool_) {
        return std::nullopt;
    }
    return pool_->try_submit_with_priority_for(
        priority_,
        timeout,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class Rep, class Period, class F, class... Args>
auto executor::try_submit_with_options_for(
    task_options options,
    std::chrono::duration<Rep, Period> timeout,
    F&& f,
    Args&&... args) const
    -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
    if (!pool_) {
        return std::nullopt;
    }
    apply_default_priority(options, priority_);
    return pool_->try_submit_with_options_for(
        std::move(options),
        timeout,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class Clock, class Duration, class F, class... Args>
auto executor::try_submit_until(
    std::chrono::time_point<Clock, Duration> deadline,
    F&& f,
    Args&&... args) const
    -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
    if (!pool_) {
        return std::nullopt;
    }
    return pool_->try_submit_with_priority_until(
        priority_,
        deadline,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class Clock, class Duration, class F, class... Args>
auto executor::try_submit_with_options_until(
    task_options options,
    std::chrono::time_point<Clock, Duration> deadline,
    F&& f,
    Args&&... args) const
    -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
    if (!pool_) {
        return std::nullopt;
    }
    apply_default_priority(options, priority_);
    return pool_->try_submit_with_options_until(
        std::move(options),
        deadline,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class F, class... Args>
auto executor::submit_cancellable(cancellation_token token, F&& f, Args&&... args) const
    -> std::future<std::invoke_result_t<F, Args...>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->submit_cancellable_with_priority(
        priority_,
        std::move(token),
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class F, class... Args>
auto executor::submit_retry(retry_options retry, F&& f, Args&&... args) const
    -> std::future<std::invoke_result_t<std::decay_t<F>&, std::decay_t<Args>&...>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->submit_retry_with_priority(
        std::move(retry),
        priority_,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class F, class... Args>
auto executor::submit_retry_with_options(
    retry_options retry,
    task_options options,
    F&& f,
    Args&&... args) const
    -> std::future<std::invoke_result_t<std::decay_t<F>&, std::decay_t<Args>&...>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    apply_default_priority(options, priority_);
    return pool_->submit_retry_with_options(
        std::move(retry),
        std::move(options),
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

#if UNIVERSAL_THREAD_POOL_HAS_STOP_TOKEN
template <class F, class... Args>
auto executor::submit_stop_token(std::stop_token token, F&& f, Args&&... args) const
    -> std::future<std::invoke_result_t<F, Args...>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->submit_stop_token_with_priority(
        priority_,
        std::move(token),
        std::forward<F>(f),
        std::forward<Args>(args)...);
}
#endif

template <class F>
auto executor::bulk_submit(std::size_t count, F&& f) const
    -> std::vector<std::future<std::invoke_result_t<std::decay_t<F>&, std::size_t>>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->bulk_submit_with_priority(
        priority_,
        count,
        std::forward<F>(f));
}

template <class F>
auto executor::bulk_submit_with_options(task_options options, std::size_t count, F&& f) const
    -> std::vector<std::future<std::invoke_result_t<std::decay_t<F>&, std::size_t>>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    apply_default_priority(options, priority_);
    return pool_->bulk_submit_with_options(
        std::move(options),
        count,
        std::forward<F>(f));
}

template <class Rep, class Period, class F, class... Args>
auto executor::schedule_submit_after(
    std::chrono::duration<Rep, Period> delay,
    F&& f,
    Args&&... args) const
    -> scheduled_future<std::invoke_result_t<F, Args...>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->schedule_submit_after_with_priority(
        priority_,
        delay,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class Clock, class Duration, class F, class... Args>
auto executor::schedule_submit_at(
    std::chrono::time_point<Clock, Duration> time_point,
    F&& f,
    Args&&... args) const
    -> scheduled_future<std::invoke_result_t<F, Args...>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->schedule_submit_at_with_priority(
        priority_,
        time_point,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class Rep, class Period, class F, class... Args>
auto executor::schedule_submit_after_with_options(
    task_options options,
    std::chrono::duration<Rep, Period> delay,
    F&& f,
    Args&&... args) const
    -> scheduled_future<std::invoke_result_t<F, Args...>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    apply_default_priority(options, priority_);
    return pool_->schedule_submit_after_with_options(
        std::move(options),
        delay,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class Clock, class Duration, class F, class... Args>
auto executor::schedule_submit_at_with_options(
    task_options options,
    std::chrono::time_point<Clock, Duration> time_point,
    F&& f,
    Args&&... args) const
    -> scheduled_future<std::invoke_result_t<F, Args...>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    apply_default_priority(options, priority_);
    return pool_->schedule_submit_at_with_options(
        std::move(options),
        time_point,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class Rep, class Period, class F, class... Args>
periodic_task_handle executor::detach_periodic(
    std::chrono::duration<Rep, Period> interval,
    F&& f,
    Args&&... args) const {
    if (!pool_) {
        return periodic_task_handle{};
    }
    periodic_options periodic;
    periodic.interval = std::chrono::duration_cast<std::chrono::nanoseconds>(interval);
    task_options options;
    options.priority = priority_;
    return pool_->detach_periodic_with_options(
        std::move(periodic),
        std::move(options),
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class F, class... Args>
periodic_task_handle executor::detach_periodic_with_options(
    periodic_options periodic,
    task_options options,
    F&& f,
    Args&&... args) const {
    if (!pool_) {
        return periodic_task_handle{};
    }
    apply_default_priority(options, priority_);
    return pool_->detach_periodic_with_options(
        std::move(periodic),
        std::move(options),
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class T, class F>
auto executor::submit_then(std::future<T> predecessor, F&& continuation) const
    -> std::future<detail::continuation_result_t<T, F>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->submit_then(std::move(predecessor), std::forward<F>(continuation));
}

template <class T, class F>
auto executor::continue_with(std::future<T> predecessor, F&& continuation) const
    -> std::future<detail::continuation_result_t<T, F>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->continue_with_priority(
        priority_,
        std::move(predecessor),
        std::forward<F>(continuation));
}

template <class T, class F>
auto executor::continue_on(executor completion, std::future<T> predecessor, F&& continuation) const
    -> std::future<detail::continuation_result_t<T, F>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->continue_on(
        std::move(completion),
        std::move(predecessor),
        std::forward<F>(continuation));
}

template <class T, class F>
auto executor::dataflow(std::vector<std::future<T>> predecessors, F&& continuation) const
    -> std::future<detail::dataflow_result_t<T, F>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->dataflow_with_priority(
        priority_,
        std::move(predecessors),
        std::forward<F>(continuation));
}

template <class T, class F>
auto executor::dataflow_on(
    executor completion,
    std::vector<std::future<T>> predecessors,
    F&& continuation) const
    -> std::future<detail::dataflow_result_t<T, F>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->dataflow_on(
        std::move(completion),
        std::move(predecessors),
        std::forward<F>(continuation));
}

template <class T>
auto executor::when_all(std::vector<std::future<T>> futures) const
    -> std::future<std::vector<T>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->when_all(std::move(futures));
}

template <class T>
auto executor::when_any(std::vector<std::future<T>> futures) const
    -> std::future<when_any_result<T>> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->when_any(std::move(futures));
}

template <class F, class... Args>
bool executor::detach(F&& f, Args&&... args) const {
    if (!pool_) {
        return false;
    }
    return pool_->detach_with_priority(
        priority_,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class F, class... Args>
bool executor::detach_with_options(task_options options, F&& f, Args&&... args) const {
    if (!pool_) {
        return false;
    }
    apply_default_priority(options, priority_);
    return pool_->detach_with_options(
        std::move(options),
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class F, class... Args>
bool executor::try_detach(F&& f, Args&&... args) const {
    if (!pool_) {
        return false;
    }
    return pool_->try_detach_with_priority(
        priority_,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class F, class... Args>
bool executor::try_detach_with_options(task_options options, F&& f, Args&&... args) const {
    if (!pool_) {
        return false;
    }
    apply_default_priority(options, priority_);
    return pool_->try_detach_with_options(
        std::move(options),
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class Rep, class Period, class F, class... Args>
bool executor::try_detach_for(
    std::chrono::duration<Rep, Period> timeout,
    F&& f,
    Args&&... args) const {
    if (!pool_) {
        return false;
    }
    return pool_->try_detach_with_priority_for(
        priority_,
        timeout,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class Rep, class Period, class F, class... Args>
bool executor::try_detach_with_options_for(
    task_options options,
    std::chrono::duration<Rep, Period> timeout,
    F&& f,
    Args&&... args) const {
    if (!pool_) {
        return false;
    }
    apply_default_priority(options, priority_);
    return pool_->try_detach_with_options_for(
        std::move(options),
        timeout,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class Clock, class Duration, class F, class... Args>
bool executor::try_detach_until(
    std::chrono::time_point<Clock, Duration> deadline,
    F&& f,
    Args&&... args) const {
    if (!pool_) {
        return false;
    }
    return pool_->try_detach_with_priority_until(
        priority_,
        deadline,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class Clock, class Duration, class F, class... Args>
bool executor::try_detach_with_options_until(
    task_options options,
    std::chrono::time_point<Clock, Duration> deadline,
    F&& f,
    Args&&... args) const {
    if (!pool_) {
        return false;
    }
    apply_default_priority(options, priority_);
    return pool_->try_detach_with_options_until(
        std::move(options),
        deadline,
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

template <class F, class... Args>
bool executor::detach_cancellable(cancellation_token token, F&& f, Args&&... args) const {
    if (!pool_) {
        return false;
    }
    return pool_->detach_cancellable_with_priority(
        priority_,
        std::move(token),
        std::forward<F>(f),
        std::forward<Args>(args)...);
}

#if UNIVERSAL_THREAD_POOL_HAS_STOP_TOKEN
template <class F, class... Args>
bool executor::detach_stop_token(std::stop_token token, F&& f, Args&&... args) const {
    if (!pool_) {
        return false;
    }
    return pool_->detach_stop_token_with_priority(
        priority_,
        std::move(token),
        std::forward<F>(f),
        std::forward<Args>(args)...);
}
#endif

template <class F>
std::size_t executor::bulk_detach(std::size_t count, F&& f) const {
    if (!pool_) {
        return 0;
    }
    return pool_->bulk_detach_with_priority(
        priority_,
        count,
        std::forward<F>(f));
}

template <class F>
std::size_t executor::bulk_detach_with_options(task_options options, std::size_t count, F&& f) const {
    if (!pool_) {
        return 0;
    }
    apply_default_priority(options, priority_);
    return pool_->bulk_detach_with_options(
        std::move(options),
        count,
        std::forward<F>(f));
}

template <class F>
bool executor::post(F&& f) const {
    return detach(std::forward<F>(f));
}

template <class F>
bool executor::defer(F&& f) const {
    return detach(std::forward<F>(f));
}

template <class F>
bool executor::dispatch(F&& f) const {
    if (!pool_) {
        return false;
    }
    return pool_->dispatch(std::forward<F>(f));
}

template <class T>
void executor::wait_with_help(std::future<T>& future) const {
    if (pool_ && pool_->is_worker_thread()) {
        pool_->wait_with_help(future);
        return;
    }
    future.wait();
}

template <class T>
T executor::get_with_help(std::future<T> future) const {
    wait_with_help(future);
    if constexpr (std::is_void<T>::value) {
        future.get();
    } else {
        return future.get();
    }
}

template <class F>
auto executor::run_managed_blocking(F&& f) const -> std::invoke_result_t<F&&> {
    if (!pool_) {
        throw thread_pool_closed("executor does not reference a thread pool");
    }
    return pool_->run_managed_blocking(std::forward<F>(f));
}

} // namespace universal_thread_pool
