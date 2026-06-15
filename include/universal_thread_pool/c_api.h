#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct utp_pool utp_pool;

typedef void (*utp_task_fn)(void* user_data);
typedef void (*utp_error_fn)(const char* message, void* user_data);

typedef enum utp_shutdown_policy {
    UTP_SHUTDOWN_DRAIN = 0,
    UTP_SHUTDOWN_CANCEL_PENDING = 1,
    UTP_SHUTDOWN_STOP_IMMEDIATELY = 2
} utp_shutdown_policy;

typedef struct utp_pool_options {
    size_t threads;
    size_t max_queue_size;
    size_t worker_batch_size;
    int bounded_queue;
    int enable_work_stealing;
    int enable_priority;
} utp_pool_options;

typedef struct utp_metrics {
    uint64_t submitted_tasks_total;
    uint64_t completed_tasks_total;
    uint64_t failed_tasks_total;
    uint64_t cancelled_tasks_total;
    uint64_t rejected_tasks_total;
    uint64_t steal_success_total;
    uint64_t steal_fail_total;
    size_t queued_tasks;
    size_t running_tasks;
    size_t total_threads;
    size_t active_tasks;
    size_t blocked_workers;
} utp_metrics;

utp_pool_options utp_default_options(void);
utp_pool* utp_create(const utp_pool_options* options);
void utp_destroy(utp_pool* pool);

int utp_detach(utp_pool* pool, utp_task_fn task, void* user_data);
int utp_detach_checked(
    utp_pool* pool,
    utp_task_fn task,
    void* user_data,
    utp_error_fn on_error,
    void* error_user_data);

void utp_shutdown(utp_pool* pool, utp_shutdown_policy policy);
int utp_wait_for_idle_ms(utp_pool* pool, uint64_t timeout_ms);
utp_metrics utp_get_metrics(utp_pool* pool);
size_t utp_metrics_json(utp_pool* pool, char* buffer, size_t buffer_size);

#ifdef __cplusplus
}
#endif

