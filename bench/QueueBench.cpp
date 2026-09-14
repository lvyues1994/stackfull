// Queue micro benchmarks.
//
//   stackfull_queue_bench [iterations]
//
// Owner throughput is what the scheduler's hot path pays for every
// dispatch; thieves are added to show how much the owner suffers from them.

#include <stackfull/queue/BbqQueue.h>
#include <stackfull/queue/BwosQueue.h>
#include <stackfull/queue/RingQueue.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

using namespace stackfull::queue;

namespace {

using Clock = std::chrono::steady_clock;

double nanosPerOp(Clock::time_point const start, Clock::time_point const end, long const ops) {
    auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    return static_cast<double>(ns) / static_cast<double>(ops);
}

void report(char const *const name, double const nsPerOp, char const *const note = "") {
    std::printf("%-52s %8.1f ns %s\n", name, nsPerOp, note);
}

// Owner alone: push 8 / pop 8, repeated. Measures one push+pop pair.
template <class Q>
void benchOwnerAlone(char const *const name, long const iterations) {
    Q queue;
    int value = 0;
    auto const start = Clock::now();
    for (long i = 0; i < iterations; i += 8) {
        for (int k = 0; k < 8; ++k) {
            queue.push(k);
        }
        for (int k = 0; k < 8; ++k) {
            queue.pop(value);
        }
    }
    auto const end = Clock::now();
    report(name, nanosPerOp(start, end, iterations), "per push+pop");
}

// Owner under continuous stealing. `highLevel` keeps the queue near capacity
// (push 4, pop 3, back off when full) so thieves always find blocks to take;
// otherwise the owner replenishes one item per drained pop and the level
// collapses to ~1 — for BWoS that puts everything into the consumer's block,
// which thieves cannot touch (the case the scheduler's placement policy
// handles by routing to idle workers instead of relying on steals).
template <class Q>
void benchOwnerWithThieves(char const *const name, long const iterations, int const thiefCount,
                           bool const highLevel) {
    Q queue;
    std::atomic<bool> running{true};
    std::atomic<long> stolen{0};
    std::vector<std::thread> thieves;
    for (int t = 0; t < thiefCount; ++t) {
        thieves.emplace_back([&] {
            int value = 0;
            long mine = 0;
            while (running.load(std::memory_order_relaxed)) {
                if (queue.steal(value)) {
                    ++mine;
                }
            }
            stolen.fetch_add(mine, std::memory_order_relaxed);
        });
    }

    int value = 0;
    long pushes = 0;
    auto const start = Clock::now();
    if (highLevel) {
        for (long i = 0; i < iterations; ++i) {
            if (queue.push(static_cast<int>(i))) {
                ++pushes;
            } else {
                queue.pop(value);
            }
            if ((i & 3) != 0) {
                queue.pop(value);
            }
        }
    } else {
        for (long i = 0; i < iterations; ++i) {
            queue.push(static_cast<int>(i));
            ++pushes;
            if (not queue.pop(value)) {
                queue.push(static_cast<int>(i));
                ++pushes;
            }
        }
    }
    auto const end = Clock::now();
    running.store(false, std::memory_order_relaxed);
    for (std::thread &thief : thieves) {
        thief.join();
    }
    char note[96];
    std::snprintf(note, sizeof note, "per owner op, %s level, %d thieves stole %5.1f%%",
                  highLevel ? "high" : "low ", thiefCount,
                  100.0 * static_cast<double>(stolen.load()) / static_cast<double>(pushes));
    report(name, nanosPerOp(start, end, iterations), note);
}

template <class Q>
void benchMpmc(char const *const name, long const itemsPerProducer, int const producers, int const consumers) {
    Q queue;
    std::atomic<long> remaining{itemsPerProducer * producers};
    std::vector<std::thread> threads;
    auto const start = Clock::now();
    for (int p = 0; p < producers; ++p) {
        threads.emplace_back([&] {
            for (long i = 0; i < itemsPerProducer; ++i) {
                while (queue.push(static_cast<int>(i)) != PushStatus::Ok) {
                }
            }
        });
    }
    for (int c = 0; c < consumers; ++c) {
        threads.emplace_back([&] {
            int value = 0;
            while (remaining.load(std::memory_order_relaxed) > 0) {
                if (queue.pop(value) == PopStatus::Ok) {
                    remaining.fetch_sub(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (std::thread &thread : threads) {
        thread.join();
    }
    auto const end = Clock::now();
    char note[64];
    std::snprintf(note, sizeof note, "per item, %dP/%dC", producers, consumers);
    report(name, nanosPerOp(start, end, itemsPerProducer * producers), note);
}

} // namespace

int main(int const argc, char **const argv) {
    long const iterations = argc > 1 ? std::atol(argv[1]) : 20'000'000L;
    std::printf("iterations: %ld\n", iterations);

    benchOwnerAlone<BwosQueue<int, 8, 32>>("BWoS 8x32  owner alone", iterations);
    benchOwnerAlone<RingQueue<int, 256>>("Ring 256   owner alone", iterations);

    for (bool highLevel : {false, true}) {
        for (int thieves : {1, 3}) {
            benchOwnerWithThieves<BwosQueue<int, 8, 32>>("BWoS 8x32  owner + thieves", iterations, thieves, highLevel);
            benchOwnerWithThieves<RingQueue<int, 256>>("Ring 256   owner + thieves", iterations, thieves, highLevel);
        }
    }

    benchMpmc<BbqQueue<int, 16, 4096>>("BBQ 16x4096 MPMC", iterations / 8, 1, 1);
    benchMpmc<BbqQueue<int, 16, 4096>>("BBQ 16x4096 MPMC", iterations / 8, 4, 4);
    return 0;
}
