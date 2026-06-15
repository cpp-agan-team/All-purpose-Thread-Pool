# Universal Thread Pool

一个全能线程池设计与开发。它不仅是一个 `submit/detach` 的任务队列，而是把固定/弹性线程池、优先级与延迟调度、work-stealing、并行循环、任务编排（continuation / dataflow / DAG）、协作式取消、生产级可观测性，以及可选的 C ABI 和 C++20 协程支持整合到一套统一 API 之中。

对于普通异步任务走最短路径，CPU 密集、阻塞 I/O、优先级、延迟、批量并行等高级需求按需启用，互不拖累。

- 语言标准：C++17 必需，C++20 时自动启用协程与 `std::stop_token` 支持。
- 依赖：仅标准库 + 线程库（`Threads::Threads`）。无第三方依赖。


---

## 目录

- [整体架构](#整体架构)
- [代码结构](#代码结构)
- [适用场景及原因](#适用场景及原因)
- [编译与运行](#编译与运行)
- [快速上手示例](#快速上手示例)
- [可观测性](#可观测性)


---

## 整体架构

采用「facade + executor + scheduler + queue manager + worker + observability」的分层设计。使其对外是稳定的提交 API，对内是可替换的队列与调度策略。

```text
┌─────────────────────────────────────────────────────────────────────┐
│  应用代码                                                              │
│   thread_pool / executor / thread_pool_runtime / task_group /         │
│   task_graph / 协程 awaiter / C ABI (utp_*)                            │
└───────────────────────────────┬───────────────────────────────────────┘
                                │  submit / detach / post / dispatch
                                │  parallel_for / continue_with / dataflow
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  thread_pool（核心）                                                   │
│                                                                       │
│  提交层 ── 把 callable 打包成 task_item，绑定 promise / 取消 / 元数据   │
│     │                                                                 │
│     ▼                                                                 │
│  调度 / 队列层                                                         │
│   ├── 全局优先级队列  queues_[low|normal|high|critical]  (mutex_ 保护) │
│   ├── per-worker 本地 deque（work-stealing 模式）                      │
│   └── 延迟任务最小堆  multimap<time_point>  (delay_mutex_ 保护)        │
│        └── timer 线程到期后批量注入普通队列                            │
│     │                                                                 │
│     ▼                                                                 │
│  Worker 组                                                            │
│   ├── worker 线程：批量取任务 → 锁外执行 → 捕获异常 → 记统计           │
│   ├── 本地缓存 / 偷取 / 空闲休眠 / 退休回收                            │
│   └── thread init/exit hook、命名、CPU 亲和                            │
│     │                                                                 │
│     ▼                                                                 │
│  continuation 调度线程（独立小线程池）                                 │
│   └── 专门承载 continue_with / dataflow / when_all / when_any 的       │
│       「等前驱 future」逻辑，不占用普通 worker → 避免编排死锁          │
│                                                                       │
│  可观测层：原子计数器 + 等待/执行延迟直方图 + per-worker 快照          │
└─────────────────────────────────────────────────────────────────────┘
```

数据流要点：

- **简单任务**走全局队列 FIFO 快路径。
- **高并发小任务 / 递归任务**走 work-stealing：worker 自己提交的子任务进本地 deque 头部，自己从头部取（缓存局部性好），其他 worker 从尾部偷（减少争用）。
- **延迟任务**先进 delay 最小堆，由 timer 线程到期后批量转入普通队列。
- **任务编排**（依赖前驱 future）在独立的 continuation 线程上等待，绝不在普通 worker 上阻塞 `future.get()`——这是避免「N 个 worker 互相等待对方任务」死锁的关键设计。
- **CPU / I/O / 后台**任务由 `thread_pool_runtime` 路由到三个独立池，互不拖慢。


---

## 代码结构

```text
include/universal_thread_pool/
├── thread_pool.hpp          伞形头文件，应用代码包它一个就够
├── common.hpp               配置 options、枚举、metrics、异常、取消令牌、类型 trait
├── executor.hpp             轻量 executor 句柄（可拷贝，指向某个池/优先级）
├── thread_pool_core.hpp     thread_pool 类与全部模板提交/并行/编排实现
├── coroutine.hpp            C++20 协程 awaiter（schedule / submit / future）
├── task_group.hpp           task_group / scoped_task_group：一批相关任务的等待与取消
├── task_graph.hpp           轻量 DAG 调度器（依赖、条件分支、子图）
├── runtime.hpp              thread_pool_runtime：CPU / I/O / 后台 三池运行时
└── c_api.h                  可选 C ABI，用于 FFI 与 C 调用方

src/
├── thread_pool.cpp          生命周期、队列、worker 循环、timer、退休、指标快照
├── executor.cpp             executor 的非模板操作
├── task_group.cpp           分组等待与取消（worker 内 help-run）
├── task_graph.cpp           DAG 依赖调度内部逻辑
├── runtime.cpp              三池 facade 实现
├── common.cpp               option 工厂、默认线程数、指标序列化、硬件拓扑探测
└── c_api.cpp                C ABI 包装实现（边界处吞掉 C++ 异常）

tests/thread_pool_tests.cpp  示例，覆盖提交/异常/取消/优先级/延迟/resize/
                             work-stealing/DAG/编排/runtime/指标 等
examples/basic.cpp           端到端用法示例
benchmarks/thread_pool_bench.cpp  与 std::async 的吞吐对比
cmake/                       find_package 导出配置
```

## 适用场景及原因

| 场景 | 推荐用法 | 为什么适用 |
|---|---|---|
| **通用业务异步任务** | `thread_pool::submit` / `detach` + 默认固定池 | 最短路径、RAII 安全关闭、异常通过 future 传播，零额外配置即可用 |
| **CPU 密集计算**（图像、压缩、序列化） | `make_cpu_pool_options()`，线程数≈硬件并发，work-stealing | 分块并行 + 偷取负载均衡，避免计算被阻塞任务拖慢 |
| **阻塞 I/O**（文件、网络、同步 SDK） | `runtime.submit_io` 或 `bounded_block` 池 + `managed_blocking` | 独立 I/O 池 + 阻塞时补偿线程，不让 I/O 占满 CPU worker |
| **数据并行循环**（数值内核、批处理） | `parallel_for` / `parallel_reduce` / `parallel_for_nd` | 按 block 提交，调度开销低；guided 调度自适应不均负载 |
| **任务编排 / 流水线**（解码→处理→落盘） | `continue_with` / `dataflow` / `task_graph` | 在独立线程等前驱，避免编排死锁；DAG 表达复杂依赖无需手写 future 等待 |
| **优先级敏感**（关键请求优先） | `schedule_policy::priority` + `priority_fairness_interval` | 高优先级先执行，公平预算防止低优饥饿 |
| **定时 / 周期任务**（心跳、缓存刷新） | `submit_after` / `detach_periodic` | 独立 timer 线程管理延迟堆，到期批量注入，可取消 |
| **过载敏感的服务端** | `bounded_reject` / `bounded_caller_runs` + 指标 | 显式背压而非无限排队；指标实时反映过载 |
| **弹性负载**（突发流量） | `make_cached_pool_options()` cached 模式 | 按队列压力自动扩容到 max，空闲超时回收到 min |
| **GUI / 实时刷新** | `bounded_drop_oldest` | 过期刷新任务自动丢弃，只保留最新 |
| **C / 其他语言集成** | `c_api.h`（`utp_*`） | 稳定 C ABI，异常不跨边界，可从 FFI 安全调用 |
| **C++20 协程** | `co_await pool.schedule()` / `submit_awaitable` | 把任务池接入协程，`co_await` 池上任务无需手写回调 |
| **小工具 / 共享基础设施** | `global_thread_pool()` / `global_runtime()` | 进程级默认池，省去显式创建与传递 |

### 什么时候**不**适合

- **硬实时 / 纳秒级延迟**：本池不承诺实时调度，cv 唤醒有微秒级抖动。
- **完全替代 oneTBB / HPX / Taskflow 这类完整并行运行时**：本池定位是「全能但克制」，超大规模 dataflow runtime 不是目标。
- **单次、一过性的并行**（程序里只用一次）：直接 `std::async` 可能更省事；线程池的价值在于复用。

---

## 编译与运行

从项目根目录执行，先创建并进入 `build` 目录：

```bash
mkdir -p build
cd build
cmake ..
cmake --build .
ctest --output-on-failure
cd ..
```

### Sanitizer 构建（GCC / Clang）

CI 默认跑这三档矩阵。本地复现：

```bash
# AddressSanitizer + UndefinedBehaviorSanitizer
mkdir -p build-asan
cd build-asan
cmake .. -DCMAKE_BUILD_TYPE=Debug -DUNIVERSAL_THREAD_POOL_SANITIZER=address
cmake --build .
ctest --output-on-failure
cd ..

# ThreadSanitizer（验证并发正确性，无数据竞争）
mkdir -p build-tsan
cd build-tsan
cmake .. -DCMAKE_BUILD_TYPE=Debug -DUNIVERSAL_THREAD_POOL_SANITIZER=thread
cmake --build .
ctest --output-on-failure
cd ..
```

### Benchmark

```bash
mkdir -p build-bench
cd build-bench
cmake .. -DUNIVERSAL_THREAD_POOL_BUILD_BENCHMARKS=ON
cmake --build . --target thread_pool_bench
./thread_pool_bench 100000   # 参数为任务数
cd ..
```

### 作为依赖被其他 CMake 工程消费

```bash
cmake --install build --prefix /opt/universal_thread_pool
```

```cmake
find_package(universal_thread_pool CONFIG REQUIRED)
target_link_libraries(app PRIVATE universal_thread_pool::universal_thread_pool)
```

### 不用 CMake，直接 g++ 编译

```bash
g++ -std=c++17 -Wall -Wextra -Wpedantic -pthread -Iinclude \
    src/*.cpp tests/thread_pool_tests.cpp -o thread_pool_tests
./thread_pool_tests
```

C++20（启用协程 / `std::stop_token`）把 `-std=c++17` 换成 `-std=c++20` 即可。

---

## 快速上手示例

最小用法：

```cpp
#include "universal_thread_pool/thread_pool.hpp"
using namespace universal_thread_pool;

int main() {
    thread_pool pool;
    auto future = pool.submit([] { return 42; });
    return future.get() == 42 ? 0 : 1;
}
```

并行循环：

```cpp
loop_options opt;
opt.schedule = loop_schedule::guided_blocks;
pool.parallel_for<std::size_t>(0, values.size(), [&](std::size_t i) {
    values[i] = compute(i);
}, opt);
```

任务编排（不占用 worker 等待前驱）：

```cpp
auto bytes = runtime.submit_io([] { return read_file("in.bin"); });
auto parsed = runtime.continue_cpu(std::move(bytes), [](Bytes b) {
    return parse(std::move(b));   // I/O 在 io 池，解析在 cpu 池
});
```

DAG：

```cpp
task_graph g;
auto load = g.emplace([]{ load_input(); });
auto proc = g.emplace([]{ process(); });
auto save = g.emplace([]{ save_output(); });
g.precede(load, proc);
g.precede(proc, save);
g.run(pool.get_executor()).get();
```

> 更多示例（取消、重试、超时、周期任务、task_group、协程、C API、各类背压与调度）见 [examples/basic.cpp](examples/basic.cpp) 与 [tests/thread_pool_tests.cpp](tests/thread_pool_tests.cpp)。

---

## 可观测性

```cpp
auto m = pool.metrics();
m.average_wait_time_ns();                    // 平均排队等待
m.wait_time_percentile_estimate_ns(0.95);    // p95 等待延迟（直方图估算）
m.run_time_percentile_estimate_ns(0.99);     // p99 执行延迟
m.pending_tasks_total();                     // 待处理总量
for (auto& w : m.workers) { /* per-worker 计数 */ }

auto prom = to_prometheus(m, "service_pool");      // Prometheus 文本
auto json = to_json(m, "service_pool");            // JSON
auto otel = to_opentelemetry_json(m, "service");   // OpenTelemetry 风格 JSON
```

任务生命周期钩子：`on_task_queued` / `on_task_start` / `on_task_finish` / `on_task_cancel` / `on_task_error`，可用于接入 tracing 或自定义监控。

---

## 实现思路逻辑

这套线程池的实现遵循一条主线：**先把内核做对，再让能力按需生长**。

最底层是一个朴素但严谨的内核——一组按优先级分桶的全局队列，由单把互斥锁保护，配合条件变量让 worker 在无任务时休眠、有任务时被唤醒。每个提交进来的 callable 都被统一包装成 `task_item`，连同 `promise`、取消回调和可观测元数据一起入队；worker 则做三件事：在锁内批量取出任务、在锁外执行、执行后捕获异常并记录统计。「锁内取、锁外跑」是吞吐的基础，也保证了任务体里的任意代码（包括再次提交任务）不会和队列锁纠缠。生命周期上，提交、关闭、worker 执行这三股并发流被显式地一起考虑：状态标志都在同一把锁下读写，关闭用 `notify_all` 唤醒所有等待者，析构走 RAII 的 drain + join，被丢弃的任务一律兑现 `task_cancelled`，绝不让 `future.get()` 永久阻塞。

在这个稳定内核之上，所有「高级能力」都是**可插拔的旁路，而不是对内核的侵入式改写**。优先级调度只是把单队列换成多桶并加一个公平预算；work-stealing 给每个 worker 挂一条本地 deque，把「自己产生的子任务」留在本地、让空闲者从另一端偷取，从而在不动全局锁的前提下改善递归与不均负载；延迟任务交给独立的 timer 线程与最小堆，到期后再「降级」成普通任务注入主队列；弹性伸缩则把线程数收敛到一个绝对目标值上做幂等对账。这种「内核单一、策略外挂」的结构，让简单场景始终走最短路径，而复杂需求各自待在自己的旁路里，互不拖累。

真正体现设计取向的是对**编排与阻塞这两类「等待」的隔离**。任务编排（continuation / dataflow / when_all / when_any）天然要等待前驱 future，如果直接在 worker 上 `get()`，就可能让所有 worker 互相干等对方排在队里的任务而集体卡死；因此这些等待被统一搬到一组专用的 continuation 线程上完成，等前驱就绪后再把后继调度回普通池。阻塞 I/O 同理——它和 CPU 计算被分到不同的池，并辅以 managed_blocking 在 worker 真要阻塞时临时补偿一个线程。配合协作式取消（永不强杀线程，只靠 token 让任务自行退出）和全程一致的单向锁层级，整套实现把「正确性」放在「性能」和「特性丰富」之前：能力很全，但每一项能力的引入都以不破坏内核的可验证正确性为前提。

---


## 许可说明

知识星球： “奔跑中cpp / c++” 所有 ，

阿甘微信：LLqueww

商业使用前请联系我方授权 一旦发现侵权行为，将依法追究法律责任

（对于公司法律事务已有对接律师，敬请告知）
