#include "universal_thread_pool/common.hpp"

#include <cctype>
#include <fstream>
#include <sstream>

namespace universal_thread_pool {

namespace {

std::string prometheus_escape_label_value(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (char ch : value) {
        switch (ch) {
        case '\\':
            escaped += "\\\\";
            break;
        case '"':
            escaped += "\\\"";
            break;
        case '\n':
            escaped += "\\n";
            break;
        default:
            escaped += ch;
            break;
        }
    }
    return escaped;
}

std::string json_escape(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (char ch : value) {
        switch (ch) {
        case '\\':
            escaped += "\\\\";
            break;
        case '"':
            escaped += "\\\"";
            break;
        case '\n':
            escaped += "\\n";
            break;
        case '\r':
            escaped += "\\r";
            break;
        case '\t':
            escaped += "\\t";
            break;
        default:
            escaped += ch;
            break;
        }
    }
    return escaped;
}

std::string latency_bucket_label(std::size_t index) {
    if (index >= metric_latency_bucket_upper_bounds_ns.size()) {
        return "+Inf";
    }
    return std::to_string(metric_latency_bucket_upper_bounds_ns[index]);
}

template <class Value>
void append_metric(
    std::ostringstream& out,
    const char* name,
    const std::string& pool_name,
    Value value) {
    out << name << "{pool=\"" << pool_name << "\"} " << value << '\n';
}

void append_latency_histogram(
    std::ostringstream& out,
    const char* name,
    const std::string& pool_name,
    const latency_bucket_array& buckets,
    std::uint64_t sum_ns) {
    std::uint64_t cumulative = 0;
    for (std::size_t i = 0; i < buckets.size(); ++i) {
        cumulative += buckets[i];
        out << name << "_bucket{pool=\"" << pool_name << "\",le=\""
            << latency_bucket_label(i) << "\"} " << cumulative << '\n';
    }
    out << name << "_count{pool=\"" << pool_name << "\"} " << cumulative << '\n';
    out << name << "_sum{pool=\"" << pool_name << "\"} " << sum_ns << '\n';
}

std::size_t parse_linux_node_online_count(const std::string& text) {
    std::size_t count = 0;
    std::size_t index = 0;

    while (index < text.size()) {
        while (index < text.size() &&
               !std::isdigit(static_cast<unsigned char>(text[index]))) {
            ++index;
        }
        if (index >= text.size()) {
            break;
        }

        std::size_t first = 0;
        while (index < text.size() &&
               std::isdigit(static_cast<unsigned char>(text[index]))) {
            first = first * 10 + static_cast<std::size_t>(text[index] - '0');
            ++index;
        }

        std::size_t last = first;
        if (index < text.size() && text[index] == '-') {
            ++index;
            last = 0;
            while (index < text.size() &&
                   std::isdigit(static_cast<unsigned char>(text[index]))) {
                last = last * 10 + static_cast<std::size_t>(text[index] - '0');
                ++index;
            }
        }

        if (last >= first) {
            count += last - first + 1;
        }
    }

    return count;
}

} // namespace

std::size_t default_thread_count() noexcept {
    const auto count = std::thread::hardware_concurrency();
    return count == 0 ? 1 : count;
}

double estimate_latency_percentile_ns(
    const latency_bucket_array& buckets,
    std::uint64_t sample_count,
    double percentile) noexcept {
    if (sample_count == 0) {
        return 0.0;
    }
    if (percentile <= 0.0) {
        percentile = 0.0;
    } else if (percentile > 1.0) {
        percentile = 1.0;
    }

    const auto target =
        std::max<std::uint64_t>(1, static_cast<std::uint64_t>(sample_count * percentile));
    std::uint64_t cumulative = 0;
    for (std::size_t i = 0; i < buckets.size(); ++i) {
        cumulative += buckets[i];
        if (cumulative >= target) {
            if (i >= metric_latency_bucket_upper_bounds_ns.size()) {
                return static_cast<double>(metric_latency_bucket_upper_bounds_ns.back());
            }
            return static_cast<double>(metric_latency_bucket_upper_bounds_ns[i]);
        }
    }
    return static_cast<double>(metric_latency_bucket_upper_bounds_ns.back());
}

std::string to_prometheus(const thread_pool_metrics& metrics, const std::string& pool_name) {
    const auto pool_label = prometheus_escape_label_value(pool_name);
    std::ostringstream out;

    append_metric(out, "universal_thread_pool_tasks_submitted_total", pool_label, metrics.submitted_tasks_total);
    append_metric(out, "universal_thread_pool_tasks_completed_total", pool_label, metrics.completed_tasks_total);
    append_metric(out, "universal_thread_pool_tasks_failed_total", pool_label, metrics.failed_tasks_total);
    append_metric(out, "universal_thread_pool_tasks_cancelled_total", pool_label, metrics.cancelled_tasks_total);
    append_metric(out, "universal_thread_pool_tasks_rejected_total", pool_label, metrics.rejected_tasks_total);
    append_metric(out, "universal_thread_pool_caller_runs_total", pool_label, metrics.caller_runs_total);
    append_metric(out, "universal_thread_pool_scheduled_tasks_total", pool_label, metrics.scheduled_tasks_total);
    append_metric(out, "universal_thread_pool_scheduled_tasks_cancelled_total", pool_label, metrics.scheduled_tasks_cancelled_total);
    append_metric(out, "universal_thread_pool_local_tasks_total", pool_label, metrics.local_tasks_total);
    append_metric(out, "universal_thread_pool_steal_success_total", pool_label, metrics.steal_success_total);
    append_metric(out, "universal_thread_pool_steal_fail_total", pool_label, metrics.steal_fail_total);
    append_metric(out, "universal_thread_pool_priority_fairness_picks_total", pool_label, metrics.priority_fairness_picks_total);
    append_metric(out, "universal_thread_pool_managed_blocking_total", pool_label, metrics.managed_blocking_total);
    append_metric(out, "universal_thread_pool_managed_blocking_compensations_total", pool_label, metrics.managed_blocking_compensations_total);
    append_metric(out, "universal_thread_pool_thread_affinity_applied_total", pool_label, metrics.thread_affinity_applied_total);
    append_metric(out, "universal_thread_pool_thread_affinity_failed_total", pool_label, metrics.thread_affinity_failed_total);
    append_metric(out, "universal_thread_pool_worker_wakeup_total", pool_label, metrics.worker_wakeup_total);
    append_metric(out, "universal_thread_pool_worker_idle_timeout_total", pool_label, metrics.worker_idle_timeout_total);
    append_metric(out, "universal_thread_pool_worker_retired_total", pool_label, metrics.worker_retired_total);

    append_metric(out, "universal_thread_pool_queued_tasks", pool_label, metrics.queued_tasks);
    append_metric(out, "universal_thread_pool_delayed_tasks", pool_label, metrics.delayed_tasks);
    append_metric(out, "universal_thread_pool_running_tasks", pool_label, metrics.running_tasks);
    append_metric(out, "universal_thread_pool_total_threads", pool_label, metrics.total_threads);
    append_metric(out, "universal_thread_pool_idle_threads", pool_label, metrics.idle_threads);
    append_metric(out, "universal_thread_pool_active_tasks", pool_label, metrics.active_tasks);
    append_metric(out, "universal_thread_pool_blocked_workers", pool_label, metrics.blocked_workers);
    append_metric(out, "universal_thread_pool_max_queue_size_seen", pool_label, metrics.max_queue_size_seen);

    append_latency_histogram(
        out,
        "universal_thread_pool_task_wait_time_ns",
        pool_label,
        metrics.task_wait_time_buckets,
        metrics.task_wait_time_ns_total);
    append_latency_histogram(
        out,
        "universal_thread_pool_task_run_time_ns",
        pool_label,
        metrics.task_run_time_buckets,
        metrics.task_run_time_ns_total);

    return out.str();
}

std::string to_json(const thread_pool_metrics& metrics, const std::string& pool_name) {
    std::ostringstream out;
    out << "{";
    out << "\"pool\":\"" << json_escape(pool_name) << "\",";
    out << "\"submitted_tasks_total\":" << metrics.submitted_tasks_total << ",";
    out << "\"completed_tasks_total\":" << metrics.completed_tasks_total << ",";
    out << "\"failed_tasks_total\":" << metrics.failed_tasks_total << ",";
    out << "\"cancelled_tasks_total\":" << metrics.cancelled_tasks_total << ",";
    out << "\"rejected_tasks_total\":" << metrics.rejected_tasks_total << ",";
    out << "\"caller_runs_total\":" << metrics.caller_runs_total << ",";
    out << "\"scheduled_tasks_total\":" << metrics.scheduled_tasks_total << ",";
    out << "\"scheduled_tasks_cancelled_total\":" << metrics.scheduled_tasks_cancelled_total << ",";
    out << "\"local_tasks_total\":" << metrics.local_tasks_total << ",";
    out << "\"steal_success_total\":" << metrics.steal_success_total << ",";
    out << "\"steal_fail_total\":" << metrics.steal_fail_total << ",";
    out << "\"priority_fairness_picks_total\":" << metrics.priority_fairness_picks_total << ",";
    out << "\"managed_blocking_total\":" << metrics.managed_blocking_total << ",";
    out << "\"managed_blocking_compensations_total\":"
        << metrics.managed_blocking_compensations_total << ",";
    out << "\"thread_affinity_applied_total\":" << metrics.thread_affinity_applied_total << ",";
    out << "\"thread_affinity_failed_total\":" << metrics.thread_affinity_failed_total << ",";
    out << "\"worker_wakeup_total\":" << metrics.worker_wakeup_total << ",";
    out << "\"worker_idle_timeout_total\":" << metrics.worker_idle_timeout_total << ",";
    out << "\"worker_retired_total\":" << metrics.worker_retired_total << ",";
    out << "\"queued_tasks\":" << metrics.queued_tasks << ",";
    out << "\"delayed_tasks\":" << metrics.delayed_tasks << ",";
    out << "\"running_tasks\":" << metrics.running_tasks << ",";
    out << "\"total_threads\":" << metrics.total_threads << ",";
    out << "\"idle_threads\":" << metrics.idle_threads << ",";
    out << "\"active_tasks\":" << metrics.active_tasks << ",";
    out << "\"blocked_workers\":" << metrics.blocked_workers << ",";
    out << "\"max_queue_size_seen\":" << metrics.max_queue_size_seen << ",";
    out << "\"average_wait_time_ns\":" << metrics.average_wait_time_ns() << ",";
    out << "\"average_run_time_ns\":" << metrics.average_run_time_ns() << ",";
    out << "\"wait_time_p95_ns\":" << metrics.wait_time_percentile_estimate_ns(0.95) << ",";
    out << "\"run_time_p95_ns\":" << metrics.run_time_percentile_estimate_ns(0.95) << ",";
    out << "\"accepting\":" << (metrics.accepting ? "true" : "false") << ",";
    out << "\"paused\":" << (metrics.paused ? "true" : "false") << ",";
    out << "\"stopping\":" << (metrics.stopping ? "true" : "false") << ",";
    out << "\"workers\":[";
    for (std::size_t i = 0; i < metrics.workers.size(); ++i) {
        const auto& worker = metrics.workers[i];
        if (i != 0) {
            out << ",";
        }
        out << "{";
        out << "\"id\":" << worker.id << ",";
        out << "\"numa_node\":" << worker.numa_node << ",";
        out << "\"completed_tasks_total\":" << worker.completed_tasks_total << ",";
        out << "\"failed_tasks_total\":" << worker.failed_tasks_total << ",";
        out << "\"cancelled_tasks_total\":" << worker.cancelled_tasks_total << ",";
        out << "\"local_queue_size\":" << worker.local_queue_size << ",";
        out << "\"finished\":" << (worker.finished ? "true" : "false");
        out << "}";
    }
    out << "]}";
    return out.str();
}

std::string to_opentelemetry_json(
    const thread_pool_metrics& metrics,
    const std::string& pool_name) {
    const auto escaped_pool = json_escape(pool_name);
    struct metric_value {
        const char* name;
        std::uint64_t value;
    };
    const metric_value values[] = {
        {"universal_thread_pool.tasks.submitted", metrics.submitted_tasks_total},
        {"universal_thread_pool.tasks.completed", metrics.completed_tasks_total},
        {"universal_thread_pool.tasks.failed", metrics.failed_tasks_total},
        {"universal_thread_pool.tasks.cancelled", metrics.cancelled_tasks_total},
        {"universal_thread_pool.tasks.rejected", metrics.rejected_tasks_total},
        {"universal_thread_pool.queue.size", static_cast<std::uint64_t>(metrics.queued_tasks)},
        {"universal_thread_pool.workers.total", static_cast<std::uint64_t>(metrics.total_threads)},
        {"universal_thread_pool.workers.blocked", static_cast<std::uint64_t>(metrics.blocked_workers)},
        {"universal_thread_pool.steal.success", metrics.steal_success_total},
        {"universal_thread_pool.steal.fail", metrics.steal_fail_total},
    };

    std::ostringstream out;
    out << "{\"resourceMetrics\":[{\"resource\":{\"attributes\":[";
    out << "{\"key\":\"pool\",\"value\":{\"stringValue\":\"" << escaped_pool << "\"}}";
    out << "]},\"scopeMetrics\":[{\"metrics\":[";
    for (std::size_t i = 0; i < sizeof(values) / sizeof(values[0]); ++i) {
        if (i != 0) {
            out << ",";
        }
        out << "{\"name\":\"" << values[i].name
            << "\",\"sum\":{\"dataPoints\":[{\"asInt\":\""
            << values[i].value << "\"}]}}";
    }
    out << "]}]}]}]}";
    return out.str();
}

thread_pool_options make_cpu_pool_options(std::size_t threads) {
    thread_pool_options options;
    options.name = "cpu_pool";
    options.initial_threads = std::max<std::size_t>(1, threads);
    options.max_threads = options.initial_threads;
    options.mode = pool_mode::work_stealing;
    options.scheduler = schedule_policy::work_stealing;
    options.enable_work_stealing = true;
    options.queue = queue_policy::bounded_caller_runs;
    options.max_queue_size = options.initial_threads * 4096;
    return options;
}

thread_pool_options make_io_pool_options(std::size_t threads) {
    thread_pool_options options;
    options.name = "io_pool";
    options.initial_threads = std::max<std::size_t>(1, threads);
    options.max_threads = options.initial_threads;
    options.mode = pool_mode::fixed;
    options.scheduler = schedule_policy::fifo;
    options.queue = queue_policy::bounded_block;
    options.max_queue_size = options.initial_threads * 4096;
    return options;
}

thread_pool_options make_background_pool_options(std::size_t threads) {
    thread_pool_options options;
    options.name = "background_pool";
    options.initial_threads = std::max<std::size_t>(1, threads);
    options.max_threads = options.initial_threads;
    options.mode = pool_mode::fixed;
    options.scheduler = schedule_policy::priority;
    options.enable_priority = true;
    options.queue = queue_policy::bounded_drop_oldest;
    options.max_queue_size = options.initial_threads * 2048;
    return options;
}

thread_pool_options make_cached_pool_options(std::size_t min_threads, std::size_t max_threads) {
    thread_pool_options options;
    options.name = "cached_pool";
    options.min_threads = min_threads;
    options.initial_threads = min_threads;
    options.max_threads = std::max<std::size_t>(1, std::max(min_threads, max_threads));
    options.mode = pool_mode::cached;
    options.scheduler = schedule_policy::fifo;
    options.queue = queue_policy::unbounded;
    options.queue_expand_threshold = 64;
    options.idle_timeout = std::chrono::milliseconds{30000};
    return options;
}

hardware_topology detect_hardware_topology() {
    hardware_topology topology;
    topology.logical_cpus = default_thread_count();
    topology.numa_nodes = 1;

#if defined(__linux__)
    {
        std::ifstream node_online("/sys/devices/system/node/online");
        std::string text;
        if (node_online && std::getline(node_online, text)) {
            topology.numa_nodes = std::max<std::size_t>(
                1,
                parse_linux_node_online_count(text));
        }
    }
#endif

    topology.cpu_ids.reserve(topology.logical_cpus);
    for (std::size_t cpu = 0; cpu < topology.logical_cpus; ++cpu) {
        topology.cpu_ids.push_back(cpu);
    }
    return topology;
}

thread_pool_options make_affinity_pool_options(
    std::size_t threads,
    std::vector<std::size_t> cpu_ids) {
    auto topology = detect_hardware_topology();
    if (cpu_ids.empty()) {
        cpu_ids = topology.cpu_ids;
    }

    auto options = make_cpu_pool_options(threads);
    options.name = "affinity_pool";
    options.numa_nodes = std::max<std::size_t>(1, topology.numa_nodes);
    options.enable_thread_affinity = !cpu_ids.empty();
    options.thread_affinity_cpu_ids = std::move(cpu_ids);
    return options;
}

} // namespace universal_thread_pool
