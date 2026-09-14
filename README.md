# stackfull

一个 C++14 实现的高性能有栈协程库。栈切换基于 Boost.Context 的 fcontext 汇编（vendored，无 Boost 依赖），目标平台 Linux x86_64、Android（arm64-v8a / armeabi-v7a）、QNX 7.1 / 8.0（aarch64le）。

## 分层

```
┌───────────────────────────────────────────────────────────┐
│ L4  sync / io   Mutex · CondVar · Channel<T> · Poller        │  规划中
├───────────────────────────────────────────────────────────┤
│ L3  sched       调度器（M:N，可迁移）                          │  规划中
├───────────────────────────────────────────────────────────┤
│ L2  coro        Coroutine（resume/yield）· 栈顶控制块 ·        │  ✔
│                 异常搬运 · ForcedUnwind · eh_globals 交换      │
├───────────────────────────────────────────────────────────┤
│ L1  stack       StackAllocator · Mmap+guard page · 线程池化    │  ✔
├───────────────────────────────────────────────────────────┤
│ L0  fcontext    Boost 1.89 汇编（符号加前缀）+ C++ 声明        │  ✔
└───────────────────────────────────────────────────────────┘
```

每层一个 CMake target：`stackfull::fcontext`、`stackfull::stack`、`stackfull::coro`；`stackfull::stackfull` 是聚合目标。

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
| `pthread-tls` | 强制 `pthread_key` TLS 后端 |
| `android-arm64` / `android-arm64-api24` / `android-armv7` | 需要 `ANDROID_NDK_HOME` |
| `qnx-aarch64le` / `qnx-x86_64` | 需要先 `source qnxsdp-env.sh` |

CMake 选项：`STACKFULL_EXCEPTIONS`、`STACKFULL_SWAP_EH_GLOBALS`、`STACKFULL_TLS_BACKEND`（AUTO / THREAD_LOCAL / PTHREAD_KEY，AUTO 在 Android API < 29 选 pthread_key 以避开 emutls）、`STACKFULL_SANITIZE`。

## 性能（Linux x86_64，gcc 13 -O3，`bench/stackfull_switch_bench`）

| 操作 | 耗时 |
|---|---|
| 裸 fcontext jump | 2.8 ns / 次切换 |
| `resume()` + `yield()` | 4.8 ns / 次切换 |
| 创建 + 运行 + 销毁（池化栈） | 27 ns |
| 创建 + 运行 + 销毁（裸 mmap） | 2.4 µs |

`resume()`/`yield()` 故意内联在头文件里：栈切换后返回地址预测器失效，每多一层 `ret` 就多一次约 5 ns 的错误预测。

## 使用约束

- 协程可能在线程间迁移，**不要跨 `yield()` 持有 `thread_local` 变量地址、`errno`、`pthread_self()`/`std::this_thread::get_id()` 的结果**：glibc 把 `pthread_self` 和 `__errno_location` 标记为 `const`，优化器会把它们跨 yield 合并。
- 协程内的 `catch (...)` 必须 `throw;` 重抛，否则会吞掉 `ForcedUnwind`。
- `yield()` 只能在协程内调用；对 Running / Done 的协程 `resume()`、协程析构自己，都会 `abort()` 并给出原因。
- 每个带 guard page 的栈占 2 个 VMA，Linux 默认 `vm.max_map_count = 65530`，海量协程需要无 guard 的大 slab 分配器（`StackAllocator` 留有接口）。

## 平台备注

- 汇编来自 Boost 1.89.0（`fcontext/asm/PROVENANCE.md`），符号通过预处理器改名为 `stackfull_*_fcontext`，可与 Boost.Context 共存。
- Android 16 KB 页设备：页大小运行时取 `sysconf(_SC_PAGESIZE)`。
- Android NDK 的 libc++abi 不在 `<cxxabi.h>` 中声明 `__cxa_get_globals`，库内自行按 Itanium ABI 声明。
- armv7（ARM EHABI）的 `__cxa_eh_globals` 多一个字段，`EhGlobals` 已按 `__ARM_DWARF_EH__` 区分。
- QNX 仅有交叉编译配置，`MAP_STACK` / `MAP_LAZY` 语义待真机验证。
- arm64 汇编尚无 BTI/PAC 落地指令；开启 `-mbranch-protection` 且 `-z force-bti` 时链接器会警告并对该目标关闭 BTI。
