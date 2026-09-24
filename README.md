# stackfull

一个 C++14 实现的高性能有栈协程库。栈切换基于 Boost.Context 的 fcontext 汇编（vendored，无 Boost 依赖），目标平台 Linux x86_64、Android（arm64-v8a / armeabi-v7a）、QNX 7.1 / 8.0（aarch64le）。

## 分层

```
┌───────────────────────────────────────────────────────────┐
│ runtime         defaultScheduler · go · async · blockOn       │  ✔
├───────────────────────────────────────────────────────────┤
│ L4b io          Poller(Driver) epoll/poll · 定时器 · fd 包装   │  ✔
├───────────────────────────────────────────────────────────┤
│ L4a sync        Semaphore · Mutex · CondVar · WaitGroup ·     │  ✔
│                 Channel<T>（任务与普通线程都能用）              │
│                 回调桥接：Completion · Stream · Mailbox · select│
├───────────────────────────────────────────────────────────┤
│ L3  sched       M:N 调度器：BWoS 本地队列 · BBQ 注入队列 ·     │  ✔
│                 直接交接 · park/wake · pin · JoinHandle       │
├───────────────────────────────────────────────────────────┤
│ L2  coro        Coroutine（resume/yield）· 栈顶控制块 ·        │  ✔
│                 异常搬运 · ForcedUnwind · eh_globals 交换      │
├───────────────────────────────────────────────────────────┤
│ L1  stack       StackAllocator · Mmap+guard page · 线程池化    │  ✔
├───────────────────────────────────────────────────────────┤
│ L0  fcontext    Boost 1.89 汇编（符号加前缀）+ C++ 声明        │  ✔
└───────────────────────────────────────────────────────────┘
```

每层一个 CMake target：`stackfull::fcontext`、`stackfull::stack`、`stackfull::coro`、`stackfull::queue`（无锁队列，header-only）、`stackfull::sched`、`stackfull::sync`、`stackfull::io`、`stackfull::runtime`；`stackfull::stackfull` 是聚合目标。

## 使用

```cpp
#include <stackfull/coro/Coroutine.h>
using namespace stackfull::coro;

auto created = makeCoroutine([] {
    doPartOne();
    Coroutine::yield();           // 回到 resume() 的调用者
    doPartTwo();
});
if (not created) { /* created.error: 栈分配失败或 stackSize 过小 */ }

Coroutine c = std::move(created.coroutine);
c.resume();                       // 执行到 yield()
c.resume();                       // 执行到结束
assert(c.isDone());
```

- `makeCoroutine` 永不抛异常，失败通过 `CoroutineCreation::error` 报告，两种异常模式下 API 一致。
- 创建一个协程只有一次栈分配：控制块和协程体对象都放在协程栈顶，句柄本身是一个指针。
- 挂起的协程可以在任意线程 `resume()`（调用需串行化），切换路径不读 TLS。
- 开启异常时：协程体逃逸的异常在 `resume()` 重抛；析构挂起中的协程会用 `ForcedUnwind` 展开其栈；协程内的 `catch` 块可以跨 `yield()` 甚至跨线程迁移。
- 关闭异常时（`-DSTACKFULL_EXCEPTIONS=OFF`）：析构挂起中的协程只释放栈、不跑栈上析构，请用 `requestStop()` / `Coroutine::stopRequested()` 协作退出。

`CoroutineOptions` 可指定 `stackSize`（默认 128 KiB）和 `StackAllocator*`（默认是 mmap + guard page 之上的线程本地池）。

## 调度器

```cpp
#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>
using namespace stackfull::sched;

auto scheduler = makeScheduler();          // workers = hardware_concurrency，返回即已在跑

scheduler->spawn([] {                      // detached
    this_task::yield();                    // 让其他任务先跑
    WakeToken const me = this_task::token();
    handOffToSomewhere(me);                // 任意线程稍后 me.wake()
    this_task::park();                     // 可能被虚假唤醒，按条件循环
});

auto job = scheduler->spawnJoinable([] { /* ... */ });
job.handle.join();                         // 任务内调用则 park，线程调用则阻塞；重抛任务异常

scheduler->stop();                         // 非阻塞：拒绝新 spawn，唤醒所有 parked 任务一次，
                                           // 仍 parked 的任务被 ForcedUnwind；析构函数 join 线程
```

想让调用线程自己当 worker：`SchedulerOptions::callerIsWorker = true`，其余 worker 立即启动，保留的那个由 `run()` 提供（直到 `stop()`）。应用层不必自己建调度器：

```cpp
#include <stackfull/runtime/Runtime.h>
using namespace stackfull;

int main() {
    return blockOn([] {                    // 在进程级默认调度器上跑，阻塞 main 线程直到返回
        go([] { backgroundWork(); });      // fire-and-forget
        auto answer = async([] { return compute(); });   // Completion<int>
        auto text = blocking([] { return readWholeFile("/etc/config"); });  // 阻塞调用交给阻塞线程池，worker 不被占住
        return answer.get().value;
    });
}
```

其他选项：

- `onWorkerStart(index)`：每个 worker 线程开始跑任务前调用，在这里绑核（`sched::pinCurrentThreadToCpus({4, 5, 6, 7})`，Linux/Android 用 `sched_setaffinity`，QNX 用 runmask）、设优先级或实时调度策略。worker 线程名为 `sf-worker-N`。
- `stallThreshold` / `stallSink`：某个 worker 在一个任务上连续这么久没有 yield/park/结束，就报告一次（默认打印到 stderr）；由一个每半个阈值醒一次的监视线程完成，默认关闭。
- `rampUpDelay`（默认 50 µs）：找到活且还有剩余时，下一个空闲 worker 延迟这么久、并且积压仍在才加入；一批短任务由已醒的 worker 清掉，不再逐个唤醒。为计时角色叫醒同伴也按同一间隔节流。设 0 恢复立即扩容。
- `blocking(fn)`：在任务里调用时，`fn` 在按需伸缩的阻塞线程池上执行（最多 64 个线程，空闲 10 秒退出），任务 park 等结果并接收其异常；普通线程里调用则就地执行。

可观测性：

```cpp
SchedulerStats stats = scheduler->stats();     // 存活/已创建/已结束任务、已触发定时器，每个 worker 的睡眠/窃取/yield 次数
for (std::size_t i = 0; i < kWakeLatencyBuckets; ++i) { /* stats.wakeLatency[i]：需 options.recordWakeLatency */ }

TaskOptions named;
named.name = "uplink-reader";                  // 字符串须比任务活得久（通常是字面量）
scheduler->spawn(body, named);
scheduler->forEachTask([](TaskInfo const &task) {   // 例如出问题时转储谁还挂着
    std::printf("slot %u %s %s\n", task.slot, task.name ? task.name : "-",
                task.status == TaskStatus::Parked ? "parked" : "runnable");
});
```

- 计数器都是各 worker 单写者的 relaxed 计数，`stats()` 逐个读取求和，不在热路径上引入共享写。
- 唤醒延迟直方图从任务变为可运行（spawn、被唤醒）到它再次运行，按 2 的幂分桶（微秒）；关闭时只多两个分支。
- `forEachTask` 按 `WakeToken` 的 pin 协议读取槽位，回调拿到的是当时的拷贝。

设计要点：

- **本地队列 BWoS**（OSDI'23，自 Tokio 参考实现移植，`queue/PROVENANCE.md`）：owner 快路径块内 relaxed 原子、零屏障；窃取按块。全局注入队列 **BBQ**（ATC'22）无锁 MPMC，容量绑定 `maxTasks` 故永不满；slab 空闲索引同样用 BBQ。`-DSTACKFULL_SCHED_QUEUE=RING` 切换到 Go 风格环形队列做对照。
- **直接交接**：`yield/park/结束` 在任务栈上取下一个本地任务并一次切换过去；`PostSwitchHook` 在到达方执行重入队 / park 状态迁移 / 释放栈，保证任务寄存器保存完毕后才可能被别的线程恢复。
- **park/wake 协议**：`Running → Parked → Notified → Running` 两侧各一次原子操作，无锁无自旋；wake 令牌粘滞，早到的 wake 让下一次 park 立即返回。park 一侧用 CAS `Running → Parked`，若已被唤醒（Notified）则直接回到 Running 并重排——Parked 哪怕短暂出现一瞬，第二个唤醒者也会再调度一次，任务被恢复两次（曾经的 bug，`ConcurrentWakesScheduleAParkingTaskOnce` 覆盖）。`WakeToken` 是 `(slot, generation)`，释放时等待 pin 计数归零，过期令牌安全。
- **放置策略**：pinned → 所属 worker 收件箱；有空闲 worker → 注入队列 + unpark 一个（BWoS 偷不到短队列，空闲者要喂而不是让它偷）；全忙 → 本地 LIFO 槽（连续 3 次后让队列）；外部线程 → 注入队列。计时线程触发定时器或轮询 Driver 时把唤醒的任务收成一批：第一个自己跑，其余进注入队列只唤醒一个同伴；最后一个搜索者只在还有剩余工作时才请求下一个加入（经计时线程延迟 `rampUpDelay`，届时积压仍在才真正加入）。孤立事件因此只唤醒一个线程，一批短任务也不再逐个唤醒整个线程池。
- **空闲自旋**：按时间限制（5 µs，而不是迭代次数——刚出深度空闲的核频率低，迭代预算会拖成几十微秒），同时最多 2 个 worker 自旋；连续落空后按 2、4…32 个空闲周期指数退避，被同伴 worker 交接唤醒时清零。生产者看到有搜索者就不发 futex。
- **每个 worker 一个定时器堆**：任务把截止时间加在当前 worker 的堆上，加定时器互不争锁；条目记住所在的堆，迁移后照样能取消。每个堆的大小和最早截止时间有原子镜像，另有一个“非空堆”位图，计时线程和各 worker 的周期检查只看非空、且确有到期的堆。计时线程扫描前公布“正在扫描”标记、扫描后公布自己的截止时间；加定时器的一方先写镜像再读这个值（全部 seq_cst），截止时间更早就唤醒它，不会漏。
- **计时角色不悬空**：只要还有挂起的定时器和空闲 worker，就必须有人持有计时角色；worker 带着角色空缺去跑任务前会叫醒一个空闲同伴接手，唤醒空闲 worker 时也优先绕开正在守定时器的那个。
- **栈池共享层**：任务栈通常由 spawn 方线程分配、由运行它的 worker 释放，纯线程本地池会退化成每任务一次 mmap；池化分配器增加了一层无锁共享池（BBQ）。
- **spawn 不碰共享缓存行**：每个 worker 在全局空闲槽位队列前缓存最多 64 个槽位，批量 32 个交换；存活任务数按 worker 分片成单写者的“已创建/已结束”计数，读取时先读全部“已结束”再读“已创建”，只会高估、不会在有任务时读成 0。全局队列空了会去偷别的 worker 缓存的槽位，`maxTasks` 仍然精确。

## 同步原语

```cpp
#include <stackfull/sync/Mutex.h>
#include <stackfull/sync/ConditionVariable.h>
#include <stackfull/sync/WaitGroup.h>
#include <stackfull/sync/Channel.h>
using namespace stackfull::sync;

Mutex mutex;  ConditionVariable notEmpty;  std::deque<int> queue;
WaitGroup done;  done.add(2);

scheduler->spawn([&] {                          // 生产者
    for (int i = 0; i < 100; ++i) {
        LockGuard const guard(mutex);           // 可跨 yield/park 持有：等待的是任务而非线程
        queue.push_back(i);
        notEmpty.notifyOne();
    }
    done.done();
});
scheduler->spawn([&] {                          // 消费者
    for (int i = 0; i < 100; ++i) {
        LockGuard const guard(mutex);
        notEmpty.wait(mutex, [&] { return not queue.empty(); });
        queue.pop_front();
    }
    done.done();
});
done.wait();                                    // 主线程：阻塞在线程 Parker 上，同一个 WaitGroup
```

- 等待者是侵入式节点，放在等待方自己的栈上；原语内部只有一把保护链表的自旋锁，从不跨 park 持有。
- 构造 `Waiter` 时绑定当前上下文：任务用 `WakeToken` park/wake，普通线程用线程本地 `Parker` 阻塞。`Mutex`、`Channel`、`WaitGroup` 因此在主线程和任务之间通用。
- `Semaphore` 是核心（快路径允许插队，多余许可按 FIFO 移交等待者），`Mutex` 是二元信号量。`Channel<T>` 有界 MPMC，`close()` 唤醒所有阻塞方。
- 语义与 std 一致：`ConditionVariable::wait` 可能虚假返回，按谓词循环；`Mutex` 非递归。

## 回调式 SDK 桥接

SDK 从自己的线程回调，任务在这里等：三种形状，三个类型。

```cpp
#include <stackfull/sync/Completion.h>
#include <stackfull/sync/Stream.h>
#include <stackfull/sync/Mailbox.h>
#include <stackfull/sync/Select.h>

// 1. 一次性结果：Completion 既是回调也是等待句柄，T 用 completionFor<签名>() 推导
auto read = completionFor<Sdk::OnRead>();       // OnRead = std::function<void(int, std::error_code)>
sdk.readAsync(handle, read);                    // 把它当回调传进去
auto outcome = read.getFor(std::chrono::seconds{2});   // 任务 park；超时 error == timed_out
if (outcome) use(std::get<0>(outcome.value));

// C 风格 (fn, void *user)：toRaw()/fromRaw() 在蹦床里穿越 void*
Completion<std::string> done;
c_read(fd, [](void *user, int rc, char const *data, size_t n) {
    Completion<std::string>::fromRaw(user).set(std::string(data, n));
}, done.toRaw());

// 2. 流式：Stream<T> 有界、生产者永不阻塞，满了默认丢最旧；Latest<T> 只留最新一帧
Latest<Frame> frames;                           // 30 ms 一帧的相机
sdk.onFrame([&](Frame f) { frames.push(std::move(f)); });
Frame frame;
while (frames.nextFor(std::chrono::milliseconds{200}, frame)) process(frame);

// 3. 监听器接口（虚函数在 SDK 线程被调）：每个虚函数往 Mailbox 投一个闭包，任务按序执行
struct Bridge : CameraListener {
    Mailbox inbox;
    void onFrame(Frame const &f) override { inbox.post([this, f] { handle(f); }); }
    void onStopped() override { inbox.post([this] { inbox.close(); }); }
};
bridge.inbox.run();                             // 闭包在任务上下文运行：可以 park、拿 Mutex、做 IO

// 多源：select 返回先就绪的下标，随后用对应源的 tryNext()/get()
switch (select(control, frames)) { case 0: ...; case 1: ...; }
```

- 三种类型都是"回调一侧持有、任务一侧等待"：状态里按值记录等待者的 `WakeToken`/`Parker`，从不指向任务栈。任务被 `stop()` 展开甚至（无异常构建）直接释放后回调迟到，只是往无人读的状态里写一次；`select` 让同一等待者挂在多个源上也因此无需额外协议。
- `Completion` 可拷贝、`set()`/`fail()` 首次生效、`get()` 单消费者移出结果；`Completion<void>` 接受并忽略任何回调参数。`Stream` 单消费者，`close()` 后 `next()` 排空再返回 false；`Mailbox` 无界（丢控制事件比增长更糟）。
- 任务里等回调不占 worker：`get()`/`next()` park 后 worker 去跑别的任务，回调线程只做一次 `wake()`。

## 海量任务

```cpp
stack::MmapStackOptions unguarded;
unguarded.guardPages = 0;                                  // 一批栈 = 一个映射，不再受 vm.max_map_count 限制
auto upstream = stack::makeMmapStackAllocator(unguarded);
auto pooled = stack::makePooledStackAllocator(*upstream);  // 两者都要活得比调度器久

SchedulerOptions options;
options.allocator = pooled.get();
options.taskStackSize = 16 * 1024;
options.maxTasks = 100000;
options.reserveStacks = 100000;    // 构造时批量映射并预留：突发 spawn 不再 mmap
options.checkStackCanary = true;   // 可选：无保护页时的溢出检测
auto scheduler = makeScheduler(options);
```

- `StackAllocator::allocateMany` 一次映射切出多个栈；`reserve(size, count)` 预留，池化分配器把它们放在第三层（批量转入线程缓存），预留数同时是下限：别的层都满时回收的栈先补回这里再还给系统。
- `checkStackCanary` 在栈底写一条缓存行的哨兵，任务每次切出时检查哨兵和保存的栈指针，越界即以 "stack overflow" 终止。代价是每个任务多碰栈底那一页（RSS +4 KiB）。
- 单个调度器的任务上限由构建选项 `STACKFULL_SCHED_TASK_CAPACITY` 决定（默认 126976，每单位容量约 12 字节）；`maxTasks` 不能超过它。

10 万个任务、16 KiB 栈（x86_64，`stackfull_scale_bench mem`）：

| 栈 | spawn | 每任务 RSS | 每任务映射 |
|---|---|---|---|
| 默认（保护页，冷池） | 2.6 µs | 3.9 KiB | 1.96 |
| 无保护页 + 预留 | 0.48 µs | 4.0 KiB | 0 |
| 无保护页 + 预留 + 哨兵 | 1.1 µs | 8.0 KiB | 0 |

预留本身约 0.85 µs/栈，发生在构造调度器时。

## 定时器与 IO

```cpp
#include <stackfull/runtime/Runtime.h>
#include <stackfull/io/TcpStream.h>
#include <stackfull/sched/Sleep.h>
using namespace stackfull;

int main() {
    blockOn([] {                                   // 默认调度器的空闲 worker 睡在 defaultPoller() 里
        this_task::sleepFor(std::chrono::milliseconds{10});   // 任务挂起，worker 空出来

        auto bound = io::TcpListener::bind(defaultPoller(), io::SocketAddress::any(8080));
        if (not bound) return;
        for (;;) {
            io::TcpStreamResult peer = bound.listener->accept();   // EAGAIN → park 到就绪
            if (not peer) continue;
            go([conn = std::shared_ptr<io::TcpStream>(std::move(peer.stream))] {
                char buffer[4096];
                for (;;) {
                    io::IoResult got = conn->readFor(std::chrono::seconds{30}, buffer, sizeof buffer);
                    if (not got or got.bytes == 0) return;            // 出错、超时或对端关闭
                    conn->writeAll(buffer, got.bytes);
                }
            });
        }
    });
}
```

- `TcpStream` / `TcpListener` 拥有 fd 和它的 `Registration`，析构时先从 Poller 摘除、再关闭 fd；`Poller` 显式传入——它必须是某个运行中的调度器在轮询的那个。自建调度器时 `options.driver = poller.get()`，再把同一个 poller 传给 IO 对象；默认调度器用 `defaultPoller()`。
- 带超时的版本：`readFor/readUntil`、`acceptFor/acceptUntil`、`TcpStream::connect(poller, addr, deadline)`、`writeAllUntil`，超时返回 `std::errc::timed_out`，流仍可继续使用。`writevAll` 做聚集写；`BufWriter` 把小写入攒起来，一次 `writev` 连同放不下的负载一起发出。
- 底层接口仍在：`io::Registration` + `io::read/write/accept/connect/readv/writev`，用于自定义 fd。
- **epoll 边沿触发**：fd 在 `add` 时一次注册读写两个方向，等待不调用 `epoll_ctl`；echo 往返的成功路径零 `epoll_ctl`。就绪按方向计数：操作在系统调用**之前**取计数快照，`EAGAIN` 后等待比快照新的报告，系统调用与等待之间到达的边沿不会丢。`poll` 后端（QNX）仍是一次性 arm，同一套计数协议成立。
- **timekeeper**：任一时刻只有一个空闲 worker 以最早的定时器为超时睡在 `Driver::wait()`（有 Driver 时即 `epoll_wait`）里，并负责触发到期定时器；其他空闲 worker 睡自己的 futex。忙碌的 worker 每 61 次分派做一次维护：触发到期定时器并对 Driver 做一次非阻塞轮询，所以全忙时 IO 也不会饿死。
- 分派在表锁下一趟收集整批事件的等待者、解锁后再唤醒；事件携带 fd 号而非指针，`Registration` 析构后的迟到事件只会查不到。等待者按值记为 `Waker`，从不指向等待方的栈。
- `Scheduler::stop()` 的 `ForcedUnwind` 会穿过所有阻塞点，每个 park 点都有摘除守卫。任何包含 `park()` 的函数都不能是 `noexcept`。

## 构建

```sh
cmake --preset release && cmake --build --preset release && ctest --preset release
```

| preset | 说明 |
|---|---|
| `debug` / `release` | Linux x86_64，gcc |
| `clang-release` | Linux x86_64，clang |
| `asan` | AddressSanitizer，切换路径带 fiber 注解 |
| `noexc` | `-fno-exceptions` 全量编译与测试 |
| `tsan` | ThreadSanitizer，全部测试（切换路径带 TSan fiber 注解） |
| `pthread-tls` | 强制 `pthread_key` TLS 后端 |
| `android-arm64` / `android-arm64-api24` / `android-armv7` | 需要 `ANDROID_NDK_HOME` |
| `qnx-aarch64le` / `qnx-x86_64` | 需要先 `source qnxsdp-env.sh` |

CMake 选项：`STACKFULL_EXCEPTIONS`、`STACKFULL_SWAP_EH_GLOBALS`、`STACKFULL_TLS_BACKEND`（AUTO / THREAD_LOCAL / PTHREAD_KEY，AUTO 在 Android API < 29 选 pthread_key 以避开 emutls）、`STACKFULL_SCHED_QUEUE`（BWOS / RING）、`STACKFULL_SANITIZE`。

Android 设备上跑测试：`cmake --preset android-arm64 -DSTACKFULL_BUILD_TESTS=ON && cmake --build --preset android-arm64`，把 `build/android-arm64/tests/*_test` `adb push` 到 `/data/local/tmp` 直接运行（`c++_static`，无额外依赖）。

## 性能

gcc 13 -O3；Android 列为小米 25091RP04C（arm64，Android 16）上 NDK r28 构建的实测。

| 操作 | x86_64 | Android arm64 |
|---|---|---|
| 裸 fcontext jump（每次切换） | 2.8 ns | 12 ns |
| `Coroutine::resume()` + `yield()`（每次切换） | 4.8 ns | 24 ns |
| 协程创建 + 运行 + 销毁（池化栈） | 26 ns | 84 ns |
| BWoS owner push+pop，无 thief | 2.6 ns | 9 ns |
| BWoS owner push+pop，3 个 thief 持续窃取 | 5.5 ns | 74 ns |
| Ring（Go 风格）owner push+pop，3 个 thief | 165 ns | 244 ns |
| 调度器 `yield` 两任务直接交接（每次切换） | 15.8 ns | 42 ns |
| 同 worker park/wake 交接 | 22.5 ns | 62 ns |
| 跨 worker（pinned）park/wake 交接 | 60 ns | 99 ns |
| spawn + 运行 + 释放，1 worker | 124 ns | 198 ns |
| spawn + 运行 + 释放，4 workers（放置到空闲 worker） | 258 ns | 419 ns |
| `Mutex` lock+unlock，无争用 | 14 ns | 29 ns |
| `Mutex` lock+unlock，8 任务 / 4 workers 争用 | 366 ns | 450 ns |
| `Channel<long>` 容量 64，1P/1C，2 workers | 15 ns | 51 ns |
| `Channel<long>` 容量 1，1P/1C，2 workers | 90 ns | 268 ns |
| TCP loopback echo 往返（1 字节，epoll，2 workers） | 4.0–5.4 µs | 未复测 |
| TCP loopback echo 往返（1 字节，epoll，1 worker） | 3.7–4.2 µs | 未复测 |
| TCP loopback echo 往返（poll 后端，2 workers） | 4.7–5.6 µs | 未复测 |
| 100 任务并发 `sleepFor(200µs)`，每个定时器分摊 | 2.6 µs | 2.8 µs |

### 多核扩展与 Tokio 对照

`bench/ScaleBench.cpp`（`stackfull_scale_bench [scale|yield|spawn|pingpong|mem|lat]`）；`bench/tokio-compare` 是同样用例的 Tokio 版（`cargo run --release -- [scale|mem|lat|chan|cpu]`，Tokio 1.53）。i7-13700KF（8P+8E，24 线程），同机：

| 用例 | stackfull | Tokio |
|---|---|---|
| `yield`，1 → 24 workers | 124 → 1638 M/s | 15 → 26 M/s |
| park/wake 乒乓，1 → 24 workers | 24 → 135 M handoffs/s | 9 → 68 M handoffs/s |
| 扇出 spawn（每 worker 4 个根任务），1 → 24 workers | 9.9 → 190 M/s | 8.0 → 7.5 M/s |
| 跨 worker channel，容量 1 / 64 | 约 100 / 17 ns | 258 / 59 ns |
| 外部线程唤醒任务，每 1 ms，p50 / p99 | 16 / 110 µs | 20 / 104–140 µs |
| 外部线程唤醒任务，每 20 µs，p50 / p99 | 2.5 / 3.6 µs | 2.8 / 10.6 µs |
| 每个 parked 任务 RSS | 约 4 KiB + 2 个映射 | 0.4 KiB |
| 冷栈池 spawn | 2.5–5.5 µs | 0.26 µs |

Tokio 的 `yield_now` 会把任务推迟到本轮之后，语义不同。

### 空闲与稀疏负载的 CPU 占用

`bench/CpuBench.cpp`（`stackfull_cpu_bench [每个用例秒数] [用例名子串]`）按线程读 `/proc/self/task/*/schedstat`，排除外部生产者线程，只算调度器自己；“裸线程”是同样事件落在一个 `std::thread` 上的下限。x86_64，24 核，默认 24 个 worker，百分比为占一个核：

| 场景 | 调度器 | 裸线程 |
|---|---|---|
| 无任务 / 1 万个 parked 任务 | 0% | — |
| 1 个任务循环 `sleepFor(1ms)` | 0.6%（每次 1 个线程唤醒） | 0.5–1.1% |
| 100 任务 × `sleepFor(10ms)` | 3.2–3.9% | — |
| 1000 任务 × `sleepFor(100ms)` | 7.7–8.2% | — |
| 外部线程每 1 ms spawn 一个空任务 | 0.6–1.6% | 0.65–1.0% |
| 外部线程每 10 µs spawn 一个空任务 | 16% | 12–14% |

这张表在系统较空闲时测得。同一台机器在不同时段、不同频率状态下可以差 2–5 倍（批量定时器用例尤其敏感），只有同一时段内交替运行的 A/B 对比才有意义。

切换路径上的所有函数（`resume/yield`、`switchTo/onArrival`、`this_task::yield/park`）强制内联：栈切换后返回地址预测器失效，每多一层 `ret` 就多一次约 5 ns 的错误预测；GCC 自己不会内联这些"太大"的函数，实测 34 → 15.8 ns。

## 使用约束

调度器：

- 无抢占。CPU 密集循环必须周期性 `this_task::yield()` 并检查 `stopRequested()`；只有 parked 的任务能被 `stop()` 从外部结束，一个永不 park 的任务会让 `stop()` 等不到 `liveTasks == 0`。
- 任务内阻塞系统调用会卡住整个 worker；先用 `pinToCurrentWorker()` 把线程亲和性相关代码钉住，IO 集成留给 L4。
- `WakeToken::wake()` 可从任意线程调用；`park()` 允许虚假返回，按条件循环。
- `Scheduler` 析构前不要求所有任务已结束，但要求 `run()` 已返回；析构函数 join 所有 worker 线程。
- 调度器要活得比可能到达的回调久：`WakeToken` 只对已死任务安全，对已析构的调度器不安全（默认调度器不析构）。

同步原语：

- 唤醒协议的 Dekker 配对必须"先宣告、后检查"：`Semaphore::acquireSlow` 若先看 `permits` 再加 `waiterCount`，与释放方的"先加许可再看 `waiterCount`"不构成 SB 对，大约每两次运行丢一次唤醒（已修复并有测试覆盖）。写新原语时保持这个顺序。

IO 与定时器：

- `Registration` 必须在 fd 关闭之前析构（声明顺序：先 `Fd`、后 `Registration`）。
- `ConditionVariable::wait` 被强制展开时只能 `tryLock` 尽力重新持锁；持锁方若也在被回收则互斥量状态未定义——这只发生在 `stop()`。
- `sleepFor` 只能在任务内调用；1 个 worker 且任务从不 `yield` 时定时器无法触发（协作式无抢占）。

协程层：

- 协程可能在线程间迁移，**不要跨 `yield()` 持有 `thread_local` 变量地址、`errno`、`pthread_self()`/`std::this_thread::get_id()` 的结果**：glibc 把 `pthread_self` 和 `__errno_location` 标记为 `const`，优化器会把它们跨 yield 合并。
- 协程内的 `catch (...)` 必须 `throw;` 重抛，否则会吞掉 `ForcedUnwind`。
- `yield()` 只能在协程内调用；对 Running / Done 的协程 `resume()`、协程析构自己，都会 `abort()` 并给出原因。
- 每个带 guard page 的栈占 2 个 VMA，Linux/Android 默认 `vm.max_map_count = 65530`，即约 3.2 万个任务。更多任务见下文“海量任务”：无保护页栈批量映射只占一个 VMA，代价是溢出不再立刻段错误（用 `checkStackCanary` 兜底）。

## 平台备注

- 汇编来自 Boost 1.89.0（`fcontext/asm/PROVENANCE.md`），符号通过预处理器改名为 `stackfull_*_fcontext`，可与 Boost.Context 共存。
- Android 16 KB 页设备：页大小运行时取 `sysconf(_SC_PAGESIZE)`。
- Android NDK 的 libc++abi 不在 `<cxxabi.h>` 中声明 `__cxa_get_globals`，库内自行按 Itanium ABI 声明。
- armv7（ARM EHABI）的 `__cxa_eh_globals` 多一个字段，`EhGlobals` 已按 `__ARM_DWARF_EH__` 区分。
- QNX 仅有交叉编译配置，`MAP_STACK` / `MAP_LAZY` 语义待真机验证。
- armv7 仅交叉编译验证（手头设备为 64 位 only）：Thumb 互操作与 EHABI 三字段 `__cxa_eh_globals` 尚未在真机上跑过。
- 调度器的 Dekker 配对（inject ↔ parkIdle、searching）用同一原子字上的 seq_cst RMW 而不是 fence：推导只依赖 modification order 与 reads-from，TSan 也能建模。
- arm64 汇编尚无 BTI/PAC 落地指令；开启 `-mbranch-protection` 且 `-z force-bti` 时链接器会警告并对该目标关闭 BTI。
