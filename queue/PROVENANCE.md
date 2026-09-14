# Provenance of the queue algorithms

## BwosQueue.h — Block-based Work Stealing

| Item | Value |
|---|---|
| Paper | J. Wang et al., *BWoS: Formally Verified Block-based Work Stealing for Parallel Processing*, OSDI '23 |
| Reference implementation | Tokio PR [#5283](https://github.com/tokio-rs/tokio/pull/5283) — `jschwe/tokio-rs`, branch `bwos_master_lib`, commit `184556b2d753c9253c2feadee489b17a9563f43f`, files `tokio/src/runtime/scheduler/multi_thread/queue/bwosq/**` |
| License of the reference | MIT (Tokio) |
| Relationship | C++14 re-implementation following the Rust structure and memory orderings one-to-one (Owner `enqueue`/`dequeue`/`dequeue_block`/`enqueue_batch`, Stealer `steal`/`steal_block`/`is_empty`, block advancement and take-over). The reference was tested with loom and model-checked with GenMC. |
| Deviations | `push_batch` returns the number pushed instead of requiring an unchecked precondition; the queue is one class instead of Owner/Stealer handles; a failed thief CAS retries internally instead of returning to the caller. Block count and size are template parameters like the reference's const generics. |

## BbqQueue.h — Block-based Bounded Queue

| Item | Value |
|---|---|
| Paper | J. Wang et al., *BBQ: A Block-based Bounded Queue for Exchanging Data and Profiling*, ATC '22 |
| Source followed | Fig. 3 / Fig. 4 of the paper, retry-new mode only (drop-old lines 27–28, 63, 71–72, 82–84 omitted) |
| Memory orderings | Paper states 3 release + 3 acquire + 8 relaxed suffice after VSync optimisation but does not list them; this port uses acquire loads and release RMWs throughout, plus acq_rel on the producer's `allocated` FAA: with a relaxed FAA a producer entering a block another producer just recycled does not synchronize with the recycler, and its slot write races with the previous round's read (found by ThreadSanitizer on a 2×2 configuration). Conservative, not the paper's minimal set. |
| Atomic MAX | Emulated with a CAS loop (the paper's Armv8.1 LSE `MAX`); versions live in the high bits so integer comparison orders (version, offset). |

## RingQueue.h — reference ring

Go runtime `runq` / `runqgrab` shape (fixed power-of-two ring, owner tail,
shared head CAS, steal half). Written from the well-known design, not copied.
