#pragma once

#include "universal_thread_pool/thread_pool_core.hpp"

namespace universal_thread_pool {

class task_graph {
public:
    using node_id = std::size_t;

    enum class edge_kind {
        always,
        on_true,
        on_false
    };

    template <class F>
    node_id emplace(F&& f) {
        return emplace_with_options(task_options{}, std::forward<F>(f));
    }

    template <class F>
    node_id emplace_named(std::string name, F&& f) {
        task_options options;
        options.name = std::move(name);
        return emplace_with_options(std::move(options), std::forward<F>(f));
    }

    template <class F>
    node_id emplace_with_options(task_options options, F&& f) {
        node current;
        current.work = std::forward<F>(f);
        current.options = std::move(options);
        nodes_.push_back(std::move(current));
        return nodes_.size() - 1;
    }

    template <class F>
    node_id emplace_condition(task_options options, F&& f) {
        node current;
        current.condition = std::forward<F>(f);
        current.options = std::move(options);
        nodes_.push_back(std::move(current));
        return nodes_.size() - 1;
    }

    template <class F>
    node_id emplace_condition(F&& f) {
        return emplace_condition(task_options{}, std::forward<F>(f));
    }

    void precede(node_id before, node_id after);
    void precede_if(node_id before, node_id after, bool condition_result);
    void branch(node_id condition, node_id if_true, node_id if_false);
    std::vector<node_id> append(const task_graph& other);
    std::size_t size() const noexcept;
    bool empty() const noexcept;
    void clear();
    std::future<void> run(executor ex) const;

private:
    struct node {
        std::function<void()> work;
        std::function<bool()> condition;
        task_options options;
        struct edge {
            node_id successor = 0;
            edge_kind kind = edge_kind::always;
        };
        std::vector<edge> successors;
        std::size_t predecessor_count = 0;
    };

    enum class node_status {
        pending,
        scheduled,
        finished,
        skipped
    };

    struct graph_run_state : std::enable_shared_from_this<graph_run_state> {
        graph_run_state(
            executor run_executor,
            std::vector<node> run_nodes,
            std::vector<std::size_t> run_dependencies);

        void schedule_node(node_id id);
        void run_node(node_id id) noexcept;
        void finish_node(node_id id, std::optional<bool> condition_result);
        void skip_node_locked(node_id id, std::vector<node_id>& ready);
        void satisfy_dependency_locked(
            node_id id,
            bool activated,
            std::vector<node_id>& ready);
        void fail(std::exception_ptr error) noexcept;

        executor ex;
        std::vector<node> nodes;
        std::vector<std::size_t> dependencies;
        std::vector<node_status> statuses;
        std::vector<bool> activated;
        std::promise<void> promise;
        std::mutex mutex;
        std::size_t remaining_nodes = 0;
        bool done = false;
    };

    void validate_node(node_id id) const;
    void validate_acyclic() const;

    std::vector<node> nodes_;
};

} // namespace universal_thread_pool
