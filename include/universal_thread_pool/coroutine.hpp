#pragma once

#include "universal_thread_pool/executor.hpp"

namespace universal_thread_pool {

#if UNIVERSAL_THREAD_POOL_HAS_COROUTINE
class schedule_awaiter {
public:
    explicit schedule_awaiter(executor ex) noexcept
        : ex_(std::move(ex)) {}

    bool await_ready() const noexcept {
        return false;
    }

    bool await_suspend(std::coroutine_handle<> handle) {
        if (ex_.detach([handle] {
                handle.resume();
            })) {
            return true;
        }

        error_ = std::make_exception_ptr(
            thread_pool_closed("failed to schedule coroutine on executor"));
        return false;
    }

    void await_resume() const {
        if (error_) {
            std::rethrow_exception(error_);
        }
    }

private:
    executor ex_;
    std::exception_ptr error_;
};

template <class Callable, class Result>
class submit_awaiter {
public:
    submit_awaiter(executor ex, Callable callable)
        : ex_(std::move(ex)),
          callable_(std::move(callable)) {}

    bool await_ready() const noexcept {
        return false;
    }

    bool await_suspend(std::coroutine_handle<> handle) {
        const auto accepted = ex_.detach([this, handle] {
            try {
                result_.emplace(callable_());
            } catch (...) {
                error_ = std::current_exception();
            }
            handle.resume();
        });

        if (accepted) {
            return true;
        }

        error_ = std::make_exception_ptr(
            thread_pool_closed("failed to submit coroutine awaitable"));
        return false;
    }

    Result await_resume() {
        if (error_) {
            std::rethrow_exception(error_);
        }
        return std::move(*result_);
    }

private:
    executor ex_;
    Callable callable_;
    std::optional<Result> result_;
    std::exception_ptr error_;
};

template <class Callable>
class submit_awaiter<Callable, void> {
public:
    submit_awaiter(executor ex, Callable callable)
        : ex_(std::move(ex)),
          callable_(std::move(callable)) {}

    bool await_ready() const noexcept {
        return false;
    }

    bool await_suspend(std::coroutine_handle<> handle) {
        const auto accepted = ex_.detach([this, handle] {
            try {
                callable_();
            } catch (...) {
                error_ = std::current_exception();
            }
            handle.resume();
        });

        if (accepted) {
            return true;
        }

        error_ = std::make_exception_ptr(
            thread_pool_closed("failed to submit coroutine awaitable"));
        return false;
    }

    void await_resume() {
        if (error_) {
            std::rethrow_exception(error_);
        }
    }

private:
    executor ex_;
    Callable callable_;
    std::exception_ptr error_;
};

template <class T>
class future_awaiter {
public:
    future_awaiter(executor ex, std::future<T> future)
        : ex_(std::move(ex)),
          future_(std::move(future)) {}

    bool await_ready() const {
        return future_.valid() &&
            future_.wait_for(std::chrono::nanoseconds{0}) == std::future_status::ready;
    }

    bool await_suspend(std::coroutine_handle<> handle) {
        const auto accepted = ex_.detach([this, handle] {
            try {
                future_.wait();
            } catch (...) {
                error_ = std::current_exception();
            }
            handle.resume();
        });

        if (accepted) {
            return true;
        }

        error_ = std::make_exception_ptr(
            thread_pool_closed("failed to await future on executor"));
        return false;
    }

    T await_resume() {
        if (error_) {
            std::rethrow_exception(error_);
        }
        return future_.get();
    }

private:
    executor ex_;
    std::future<T> future_;
    std::exception_ptr error_;
};

template <>
class future_awaiter<void> {
public:
    future_awaiter(executor ex, std::future<void> future)
        : ex_(std::move(ex)),
          future_(std::move(future)) {}

    bool await_ready() const {
        return future_.valid() &&
            future_.wait_for(std::chrono::nanoseconds{0}) == std::future_status::ready;
    }

    bool await_suspend(std::coroutine_handle<> handle) {
        const auto accepted = ex_.detach([this, handle] {
            try {
                future_.wait();
            } catch (...) {
                error_ = std::current_exception();
            }
            handle.resume();
        });

        if (accepted) {
            return true;
        }

        error_ = std::make_exception_ptr(
            thread_pool_closed("failed to await future on executor"));
        return false;
    }

    void await_resume() {
        if (error_) {
            std::rethrow_exception(error_);
        }
        future_.get();
    }

private:
    executor ex_;
    std::future<void> future_;
    std::exception_ptr error_;
};
#endif

} // namespace universal_thread_pool
