#include "universal_thread_pool/thread_pool.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <future>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

namespace {

using clock_type = std::chrono::steady_clock;

template <class F>
void run_case(const std::string& name, std::size_t operations, F&& f) {
    const auto start = clock_type::now();
    f();
    const auto finish = clock_type::now();
    const auto elapsed_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count();
    const auto ns_per_op =
        operations == 0 ? 0.0 : static_cast<double>(elapsed_ns) / operations;

    std::cout << name << ": total_ns=" << elapsed_ns
              << " ns_per_op=" << ns_per_op << '\n';
}

} // namespace

int main(int argc, char** argv) {
    const std::size_t task_count =
        argc > 1 ? static_cast<std::size_t>(std::stoull(argv[1])) : 100000;
    const auto threads = universal_thread_pool::default_thread_count();

    universal_thread_pool::thread_pool_options options =
        universal_thread_pool::make_cpu_pool_options(threads);
    options.name = "bench_pool";
    options.worker_batch_size = 8;
    options.max_threads = threads;

    universal_thread_pool::thread_pool pool(options);

    run_case("submit_future", task_count, [&] {
        std::vector<std::future<std::size_t>> futures;
        futures.reserve(task_count);
        for (std::size_t i = 0; i < task_count; ++i) {
            futures.push_back(pool.submit([i] {
                return i + 1;
            }));
        }

        std::size_t sum = 0;
        for (auto& future : futures) {
            sum += future.get();
        }
        std::cout << "submit_future_sum=" << sum << '\n';
    });

    run_case("bulk_submit", task_count, [&] {
        auto futures = pool.bulk_submit(task_count, [](std::size_t i) {
            return i + 1;
        });

        std::size_t sum = 0;
        for (auto& future : futures) {
            sum += future.get();
        }
        std::cout << "bulk_submit_sum=" << sum << '\n';
    });

    run_case("parallel_for", task_count, [&] {
        std::vector<std::size_t> values(task_count);
        universal_thread_pool::loop_options loop;
        loop.schedule = universal_thread_pool::loop_schedule::guided_blocks;
        pool.parallel_for<std::size_t>(0, task_count, [&](std::size_t i) {
            values[i] = i + 1;
        }, loop);
        std::cout << "parallel_for_sum="
                  << std::accumulate(values.begin(), values.end(), std::size_t{0})
                  << '\n';
    });

    const auto async_count = std::min<std::size_t>(task_count, 10000);
    run_case("std_async", async_count, [&] {
        std::vector<std::future<std::size_t>> futures;
        futures.reserve(async_count);
        for (std::size_t i = 0; i < async_count; ++i) {
            futures.push_back(std::async(std::launch::async, [i] {
                return i + 1;
            }));
        }

        std::size_t sum = 0;
        for (auto& future : futures) {
            sum += future.get();
        }
        std::cout << "std_async_sum=" << sum << '\n';
    });

    pool.shutdown();
    pool.join();
    return 0;
}
