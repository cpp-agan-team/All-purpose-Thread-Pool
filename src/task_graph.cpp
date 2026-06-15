#include "universal_thread_pool/task_graph.hpp"

namespace universal_thread_pool {

void task_graph::precede(task_graph::node_id before, task_graph::node_id after) {
    validate_node(before);
    validate_node(after);
    nodes_[before].successors.push_back(node::edge{after, edge_kind::always});
    ++nodes_[after].predecessor_count;
}

void task_graph::precede_if(
    task_graph::node_id before,
    task_graph::node_id after,
    bool condition_result) {
    validate_node(before);
    validate_node(after);
    nodes_[before].successors.push_back(
        node::edge{after, condition_result ? edge_kind::on_true : edge_kind::on_false});
    ++nodes_[after].predecessor_count;
}

void task_graph::branch(
    task_graph::node_id condition,
    task_graph::node_id if_true,
    task_graph::node_id if_false) {
    precede_if(condition, if_true, true);
    precede_if(condition, if_false, false);
}

std::vector<task_graph::node_id> task_graph::append(const task_graph& other) {
    std::vector<node_id> mapping;
    mapping.reserve(other.nodes_.size());
    const auto offset = nodes_.size();

    for (const auto& source : other.nodes_) {
        node copy;
        copy.work = source.work;
        copy.condition = source.condition;
        copy.options = source.options;
        copy.predecessor_count = source.predecessor_count;
        nodes_.push_back(std::move(copy));
        mapping.push_back(nodes_.size() - 1);
    }

    for (node_id source_id = 0; source_id < other.nodes_.size(); ++source_id) {
        auto& target = nodes_[offset + source_id];
        target.successors.reserve(other.nodes_[source_id].successors.size());
        for (const auto& edge : other.nodes_[source_id].successors) {
            target.successors.push_back(node::edge{offset + edge.successor, edge.kind});
        }
    }

    return mapping;
}

std::size_t task_graph::size() const noexcept {
    return nodes_.size();
}

bool task_graph::empty() const noexcept {
    return nodes_.empty();
}

void task_graph::clear() {
    nodes_.clear();
}

std::future<void> task_graph::run(executor ex) const {
    if (!ex) {
        throw thread_pool_closed("task_graph requires a valid executor");
    }

    validate_acyclic();

    if (nodes_.empty()) {
        std::promise<void> promise;
        auto future = promise.get_future();
        promise.set_value();
        return future;
    }

    std::vector<task_graph::node_id> initial_ready;
    initial_ready.reserve(nodes_.size());

    std::vector<std::size_t> dependencies;
    dependencies.reserve(nodes_.size());
    for (task_graph::node_id id = 0; id < nodes_.size(); ++id) {
        dependencies.push_back(nodes_[id].predecessor_count);
        if (nodes_[id].predecessor_count == 0) {
            initial_ready.push_back(id);
        }
    }

    auto state = std::make_shared<graph_run_state>(
        std::move(ex),
        nodes_,
        std::move(dependencies));
    auto future = state->promise.get_future();

    for (const auto id : initial_ready) {
        state->schedule_node(id);
    }

    return future;
}

task_graph::graph_run_state::graph_run_state(
    executor run_executor,
    std::vector<task_graph::node> run_nodes,
    std::vector<std::size_t> run_dependencies)
    : ex(std::move(run_executor)),
      nodes(std::move(run_nodes)),
      dependencies(std::move(run_dependencies)),
      statuses(nodes.size(), node_status::pending),
      activated(nodes.size(), false),
      remaining_nodes(nodes.size()) {}

void task_graph::graph_run_state::schedule_node(task_graph::node_id id) {
    auto self = this->shared_from_this();
    task_options options = nodes[id].options;
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (done || statuses[id] != node_status::pending) {
            return;
        }
        statuses[id] = node_status::scheduled;
    }

    const auto use_plain_detach = options.name.empty() &&
        options.kind == task_kind::cpu_bound &&
        !options.priority &&
        !options.numa_node &&
        !options.cancellation &&
        !options.deadline &&
        (!options.source.file || options.source.file[0] == '\0') &&
        (!options.source.function || options.source.function[0] == '\0');
    bool accepted = false;
    try {
        accepted = use_plain_detach
            ? ex.detach([self, id] {
                self->run_node(id);
            })
            : ex.detach_with_options(std::move(options), [self, id] {
                self->run_node(id);
            });
    } catch (...) {
        fail(std::current_exception());
        return;
    }

    if (!accepted) {
        fail(std::make_exception_ptr(
            task_rejected("task_graph node was rejected by executor")));
    }
}

void task_graph::graph_run_state::run_node(task_graph::node_id id) noexcept {
    std::optional<bool> condition_result;
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (done) {
            if (statuses[id] == node_status::scheduled) {
                statuses[id] = node_status::skipped;
            }
            return;
        }
    }

    try {
        if (nodes[id].condition) {
            condition_result = nodes[id].condition();
        } else if (nodes[id].work) {
            nodes[id].work();
        }
    } catch (...) {
        fail(std::current_exception());
        return;
    }

    finish_node(id, condition_result);
}

void task_graph::graph_run_state::finish_node(
    task_graph::node_id id,
    std::optional<bool> condition_result) {
    std::vector<task_graph::node_id> ready;
    bool complete = false;

    {
        std::lock_guard<std::mutex> lock(mutex);
        if (done) {
            return;
        }

        statuses[id] = node_status::finished;

        for (const auto& edge : nodes[id].successors) {
            const bool active = edge.kind == edge_kind::always ||
                (edge.kind == edge_kind::on_true && condition_result.value_or(false)) ||
                (edge.kind == edge_kind::on_false && !condition_result.value_or(false));
            satisfy_dependency_locked(edge.successor, active, ready);
        }

        if (remaining_nodes > 0) {
            --remaining_nodes;
        }

        if (remaining_nodes == 0) {
            done = true;
            complete = true;
        }
    }

    if (complete) {
        promise.set_value();
        return;
    }

    for (const auto next : ready) {
        schedule_node(next);
    }
}

void task_graph::graph_run_state::skip_node_locked(
    task_graph::node_id id,
    std::vector<task_graph::node_id>& ready) {
    if (statuses[id] != node_status::pending) {
        return;
    }

    statuses[id] = node_status::skipped;
    if (remaining_nodes > 0) {
        --remaining_nodes;
    }

    for (const auto& edge : nodes[id].successors) {
        satisfy_dependency_locked(edge.successor, false, ready);
    }
}

void task_graph::graph_run_state::satisfy_dependency_locked(
    task_graph::node_id id,
    bool active,
    std::vector<task_graph::node_id>& ready) {
    if (statuses[id] != node_status::pending) {
        return;
    }

    if (active) {
        activated[id] = true;
    }
    if (dependencies[id] > 0) {
        --dependencies[id];
    }
    if (dependencies[id] != 0) {
        return;
    }

    if (activated[id]) {
        ready.push_back(id);
    } else {
        skip_node_locked(id, ready);
    }
}

void task_graph::graph_run_state::fail(std::exception_ptr error) noexcept {
    bool should_set = false;
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!done) {
            done = true;
            should_set = true;
        }
    }

    if (should_set) {
        try {
            promise.set_exception(error);
        } catch (...) {
        }
    }
}

void task_graph::validate_node(task_graph::node_id id) const {
    if (id >= nodes_.size()) {
        throw std::out_of_range("task_graph node id is out of range");
    }
}

void task_graph::validate_acyclic() const {
    std::vector<std::size_t> indegree;
    indegree.reserve(nodes_.size());
    std::vector<task_graph::node_id> ready;
    ready.reserve(nodes_.size());

    for (task_graph::node_id id = 0; id < nodes_.size(); ++id) {
        indegree.push_back(nodes_[id].predecessor_count);
        if (nodes_[id].predecessor_count == 0) {
            ready.push_back(id);
        }
    }

    std::size_t visited = 0;
    while (!ready.empty()) {
        const auto current = ready.back();
        ready.pop_back();
        ++visited;

        for (const auto& edge : nodes_[current].successors) {
            if (indegree[edge.successor] == 0) {
                continue;
            }
            --indegree[edge.successor];
            if (indegree[edge.successor] == 0) {
                ready.push_back(edge.successor);
            }
        }
    }

    if (visited != nodes_.size()) {
        throw std::runtime_error("task_graph contains a cycle");
    }
}

} // namespace universal_thread_pool
