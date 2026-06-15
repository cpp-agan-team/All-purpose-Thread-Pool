#pragma once

#include "universal_thread_pool/thread_pool_core.hpp"

namespace universal_thread_pool {

enum class scoped_task_group_policy {
    wait,
    cancel_and_wait,
    detach
};

class task_group {
public:
    explicit task_group(executor ex);

    template <class F>
    void run(F&& f) {
        auto token = cancellation_.token();
        auto future = executor_.submit([token, func = std::forward<F>(f)]() mutable {
            if (!token.stop_requested()) {
                func();
            }
        });

        std::lock_guard<std::mutex> lock(mutex_);
        futures_.push_back(std::move(future));
    }

    template <class F>
    void run_with_token(F&& f) {
        auto token = cancellation_.token();
        auto future = executor_.submit([token, func = std::forward<F>(f)]() mutable {
            if (!token.stop_requested()) {
                func(token);
            }
        });

        std::lock_guard<std::mutex> lock(mutex_);
        futures_.push_back(std::move(future));
    }

    template <class F, class... Args>
    auto submit(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        using result_type = std::invoke_result_t<F, Args...>;

        auto callable = make_group_callable<result_type>(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);

        auto result_promise = std::make_shared<std::promise<result_type>>();
        auto completion_promise = std::make_shared<std::promise<void>>();
        auto result = result_promise->get_future();
        auto completion = completion_promise->get_future();
        auto token = cancellation_.token();

        (void)executor_.submit(
            [token,
             result_promise,
             completion_promise,
             callable = std::make_shared<callable_type>(std::move(callable))]() mutable {
                complete_task<result_type>(
                    token,
                    result_promise,
                    completion_promise,
                    *callable);
            });

        {
            std::lock_guard<std::mutex> lock(mutex_);
            futures_.push_back(std::move(completion));
        }

        return result;
    }

    template <class F, class... Args>
    auto submit_with_token(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, cancellation_token, Args...>> {
        using result_type = std::invoke_result_t<F, cancellation_token, Args...>;

        auto token = cancellation_.token();
        auto callable = make_group_token_callable<result_type>(
            token,
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);

        auto result_promise = std::make_shared<std::promise<result_type>>();
        auto completion_promise = std::make_shared<std::promise<void>>();
        auto result = result_promise->get_future();
        auto completion = completion_promise->get_future();

        (void)executor_.submit(
            [token,
             result_promise,
             completion_promise,
             callable = std::make_shared<callable_type>(std::move(callable))]() mutable {
                complete_task<result_type>(
                    token,
                    result_promise,
                    completion_promise,
                    *callable);
            });

        {
            std::lock_guard<std::mutex> lock(mutex_);
            futures_.push_back(std::move(completion));
        }

        return result;
    }

    template <class F, class... Args>
    auto try_submit(F&& f, Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return try_submit_until(
            std::chrono::steady_clock::now(),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    auto try_submit_for(
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        return try_submit_until(
            std::chrono::steady_clock::now() +
                std::chrono::duration_cast<std::chrono::steady_clock::duration>(timeout),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    auto try_submit_until(
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
        using result_type = std::invoke_result_t<F, Args...>;

        auto callable = make_group_callable<result_type>(
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);

        auto result_promise = std::make_shared<std::promise<result_type>>();
        auto completion_promise = std::make_shared<std::promise<void>>();
        auto result = result_promise->get_future();
        auto completion = completion_promise->get_future();
        auto token = cancellation_.token();

        auto accepted = executor_.try_submit_until(
            deadline,
            [token,
             result_promise,
             completion_promise,
             callable = std::make_shared<callable_type>(std::move(callable))]() mutable {
                complete_task<result_type>(
                    token,
                    result_promise,
                    completion_promise,
                    *callable);
            });

        if (!accepted) {
            return std::nullopt;
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            futures_.push_back(std::move(completion));
        }

        return std::optional<std::future<result_type>>{std::move(result)};
    }

    template <class F, class... Args>
    auto try_submit_with_token(F&& f, Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, cancellation_token, Args...>>> {
        return try_submit_with_token_until(
            std::chrono::steady_clock::now(),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Rep, class Period, class F, class... Args>
    auto try_submit_with_token_for(
        std::chrono::duration<Rep, Period> timeout,
        F&& f,
        Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, cancellation_token, Args...>>> {
        return try_submit_with_token_until(
            std::chrono::steady_clock::now() +
                std::chrono::duration_cast<std::chrono::steady_clock::duration>(timeout),
            std::forward<F>(f),
            std::forward<Args>(args)...);
    }

    template <class Clock, class Duration, class F, class... Args>
    auto try_submit_with_token_until(
        std::chrono::time_point<Clock, Duration> deadline,
        F&& f,
        Args&&... args)
        -> std::optional<std::future<std::invoke_result_t<F, cancellation_token, Args...>>> {
        using result_type = std::invoke_result_t<F, cancellation_token, Args...>;

        auto token = cancellation_.token();
        auto callable = make_group_token_callable<result_type>(
            token,
            std::forward<F>(f),
            std::forward<Args>(args)...);
        using callable_type = decltype(callable);

        auto result_promise = std::make_shared<std::promise<result_type>>();
        auto completion_promise = std::make_shared<std::promise<void>>();
        auto result = result_promise->get_future();
        auto completion = completion_promise->get_future();

        auto accepted = executor_.try_submit_until(
            deadline,
            [token,
             result_promise,
             completion_promise,
             callable = std::make_shared<callable_type>(std::move(callable))]() mutable {
                complete_task<result_type>(
                    token,
                    result_promise,
                    completion_promise,
                    *callable);
            });

        if (!accepted) {
            return std::nullopt;
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            futures_.push_back(std::move(completion));
        }

        return std::optional<std::future<result_type>>{std::move(result)};
    }

    void wait();
    bool wait_for(std::chrono::nanoseconds timeout);
    std::size_t clear_ready();
    std::size_t pending() const;
    cancellation_token token() const noexcept;
    bool stop_requested() const noexcept;
    void cancel() noexcept;

private:
    template <class Result, class F, class... Args>
    static auto make_group_callable(F&& f, Args&&... args) {
        return [func = std::forward<F>(f),
                tuple = std::make_tuple(std::forward<Args>(args)...)]() mutable -> Result {
            if constexpr (std::is_void<Result>::value) {
                std::apply(std::move(func), std::move(tuple));
            } else {
                return std::apply(std::move(func), std::move(tuple));
            }
        };
    }

    template <class Result, class F, class... Args>
    static auto make_group_token_callable(
        cancellation_token token,
        F&& f,
        Args&&... args) {
        return [token,
                func = std::forward<F>(f),
                tuple = std::make_tuple(std::forward<Args>(args)...)]() mutable -> Result {
            if constexpr (std::is_void<Result>::value) {
                std::apply(
                    [&](auto&&... values) {
                        func(token, std::forward<decltype(values)>(values)...);
                    },
                    std::move(tuple));
            } else {
                return std::apply(
                    [&](auto&&... values) -> Result {
                        return func(token, std::forward<decltype(values)>(values)...);
                    },
                    std::move(tuple));
            }
        };
    }

    template <class Result, class Callable>
    static void complete_task(
        cancellation_token token,
        const std::shared_ptr<std::promise<Result>>& result_promise,
        const std::shared_ptr<std::promise<void>>& completion_promise,
        Callable& callable) noexcept {
        try {
            if (token.stop_requested()) {
                throw task_cancelled("task group was cancelled before execution");
            }

            if constexpr (std::is_void<Result>::value) {
                callable();
                result_promise->set_value();
            } else {
                result_promise->set_value(callable());
            }
            completion_promise->set_value();
        } catch (...) {
            const auto error = std::current_exception();
            try {
                result_promise->set_exception(error);
            } catch (...) {
            }
            try {
                completion_promise->set_exception(error);
            } catch (...) {
            }
        }
    }

    executor executor_;
    cancellation_source cancellation_;
    mutable std::mutex mutex_;
    std::vector<std::future<void>> futures_;
};

class scoped_task_group {
public:
    explicit scoped_task_group(
        executor ex,
        scoped_task_group_policy policy = scoped_task_group_policy::wait)
        : group_(std::move(ex)),
          policy_(policy) {}

    ~scoped_task_group() noexcept {
        if (joined_ || policy_ == scoped_task_group_policy::detach) {
            return;
        }

        if (policy_ == scoped_task_group_policy::cancel_and_wait) {
            group_.cancel();
        }

        try {
            group_.wait();
        } catch (...) {
        }
    }

    scoped_task_group(const scoped_task_group&) = delete;
    scoped_task_group& operator=(const scoped_task_group&) = delete;

    task_group& group() noexcept {
        return group_;
    }

    const task_group& group() const noexcept {
        return group_;
    }

    template <class F>
    void run(F&& f) {
        group_.run(std::forward<F>(f));
    }

    template <class F>
    void run_with_token(F&& f) {
        group_.run_with_token(std::forward<F>(f));
    }

    template <class F, class... Args>
    auto submit(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {
        return group_.submit(std::forward<F>(f), std::forward<Args>(args)...);
    }

    template <class F, class... Args>
    auto submit_with_token(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, cancellation_token, Args...>> {
        return group_.submit_with_token(std::forward<F>(f), std::forward<Args>(args)...);
    }

    void wait() {
        group_.wait();
        joined_ = true;
    }

    bool wait_for(std::chrono::nanoseconds timeout) {
        if (!group_.wait_for(timeout)) {
            return false;
        }
        joined_ = true;
        return true;
    }

    void cancel() noexcept {
        group_.cancel();
    }

    cancellation_token token() const noexcept {
        return group_.token();
    }

    bool stop_requested() const noexcept {
        return group_.stop_requested();
    }

    std::size_t pending() const {
        return group_.pending();
    }

private:
    task_group group_;
    scoped_task_group_policy policy_ = scoped_task_group_policy::wait;
    bool joined_ = false;
};

} // namespace universal_thread_pool
