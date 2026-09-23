// Tokio counterparts of bench/ScaleBench.cpp (scale, mem, lat), of the
// Channel cases in bench/SyncBench.cpp (chan) and of bench/CpuBench.cpp (cpu).
//
//   cargo run --release -- [scale|mem|lat|chan|cpu|all]
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering::*};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

fn rt(n: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(n).enable_all().build().unwrap()
}
fn leak<T>(v: T) -> &'static T {
    Box::leak(Box::new(v))
}

fn scale_yield(n: usize) {
    let rt = rt(n);
    let stop = leak(AtomicBool::new(false));
    let counting = leak(AtomicBool::new(false));
    let total = leak(AtomicU64::new(0));
    let hs: Vec<_> = (0..4 * n)
        .map(|_| {
            rt.spawn(async move {
                let mut k = 0u64;
                while !stop.load(Relaxed) {
                    tokio::task::yield_now().await;
                    k += counting.load(Relaxed) as u64;
                }
                total.fetch_add(k, Relaxed);
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(50));
    counting.store(true, Relaxed);
    let t0 = Instant::now();
    std::thread::sleep(Duration::from_millis(500));
    stop.store(true, Relaxed);
    let secs = t0.elapsed().as_secs_f64();
    rt.block_on(async {
        for h in hs {
            h.await.unwrap();
        }
    });
    let t = total.load(Relaxed) as f64;
    println!("yield        {:2} workers  {:8.1} M/s  ({:.1} M/s per worker)", n, t / secs / 1e6, t / secs / 1e6 / n as f64);
}

fn scale_spawn(n: usize) {
    let rt = rt(n);
    let stop = leak(AtomicBool::new(false));
    // Four roots per worker, at most 16 children outstanding each (as in ScaleBench).
    let done: Vec<&'static AtomicI64> = (0..4 * n).map(|_| leak(AtomicI64::new(0))).collect();
    let hs: Vec<_> = done
        .iter()
        .map(|&d| {
            rt.spawn(async move {
                let mut issued = 0i64;
                while !stop.load(Relaxed) {
                    while issued - d.load(Relaxed) >= 16 {
                        tokio::task::yield_now().await;
                    }
                    tokio::spawn(async move {
                        d.fetch_add(1, Relaxed);
                    });
                    issued += 1;
                }
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(50));
    let before: i64 = done.iter().map(|d| d.load(Relaxed)).sum();
    let t0 = Instant::now();
    std::thread::sleep(Duration::from_millis(500));
    let after: i64 = done.iter().map(|d| d.load(Relaxed)).sum();
    let secs = t0.elapsed().as_secs_f64();
    stop.store(true, Relaxed);
    rt.block_on(async {
        for h in hs {
            h.await.unwrap();
        }
    });
    let c = (after - before) as f64;
    println!("spawn fanout {:2} workers  {:8.2} M/s  ({:.0} ns per task per worker)", n, c / secs / 1e6, 1e9 * secs * n as f64 / c);
}

struct Pair {
    turn: AtomicUsize,
    n: [Notify; 2],
    trips: AtomicU64,
}

fn scale_pingpong(n: usize) {
    let rt = rt(n);
    let stop = leak(AtomicBool::new(false));
    let pairs: Vec<&'static Pair> = (0..2 * n)
        .map(|_| leak(Pair { turn: AtomicUsize::new(0), n: [Notify::new(), Notify::new()], trips: AtomicU64::new(0) }))
        .collect();
    let mut hs = vec![];
    for &p in &pairs {
        for me in 0..2usize {
            hs.push(rt.spawn(async move {
                let mut k = 0u64;
                loop {
                    while p.turn.load(Acquire) != me {
                        if stop.load(Relaxed) {
                            p.n[1 - me].notify_one();
                            if me == 0 {
                                p.trips.store(k, Relaxed);
                            }
                            return;
                        }
                        p.n[me].notified().await;
                    }
                    p.turn.store(1 - me, Release);
                    p.n[1 - me].notify_one();
                    k += 1;
                }
            }));
        }
    }
    std::thread::sleep(Duration::from_millis(50));
    let t0 = Instant::now();
    std::thread::sleep(Duration::from_millis(500));
    stop.store(true, Relaxed);
    let secs = t0.elapsed().as_secs_f64();
    rt.block_on(async {
        for h in hs {
            h.await.unwrap();
        }
    });
    let total: u64 = pairs.iter().map(|p| p.trips.load(Relaxed)).sum();
    println!("park/wake    {:2} workers  {:8.2} M handoffs/s over {} pairs", n, total as f64 / secs / 1e6, pairs.len());
}

fn rss_kib() -> i64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap();
    for l in s.lines() {
        if let Some(v) = l.strip_prefix("VmRSS:") {
            return v.trim().trim_end_matches(" kB").trim().parse().unwrap();
        }
    }
    0
}

fn footprint(count: usize) {
    let rt = rt(4);
    std::thread::sleep(Duration::from_millis(50));
    let gate = leak(Notify::new());
    let parked = leak(AtomicUsize::new(0));
    let rss0 = rss_kib();
    let t0 = Instant::now();
    let hs: Vec<_> = (0..count)
        .map(|_| {
            rt.spawn(async move {
                let wait = gate.notified();
                parked.fetch_add(1, Relaxed);
                wait.await;
            })
        })
        .collect();
    let spawn_secs = t0.elapsed().as_secs_f64();
    while parked.load(Relaxed) < count {
        std::thread::sleep(Duration::from_millis(1));
    }
    let rss1 = rss_kib();
    println!(
        "footprint    {:6} tasks: {:.0} ns/spawn, RSS +{:.2} KiB/task",
        count,
        1e9 * spawn_secs / count as f64,
        (rss1 - rss0) as f64 / count as f64
    );
    gate.notify_waiters();
    rt.block_on(async {
        for h in hs {
            h.await.unwrap();
        }
    });
}

fn report(name: &str, mut v: Vec<f64>) {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let q = |p: f64| v[((p * v.len() as f64) as usize).min(v.len() - 1)];
    println!(
        "{:<44} p50 {:7.1}  p99 {:7.1}  p99.9 {:7.1}  max {:8.1} us",
        name,
        q(0.5),
        q(0.99),
        q(0.999),
        v[v.len() - 1]
    );
}

fn wake_latency(workers: usize, period: Duration, samples: usize) {
    let rt = rt(workers);
    let base = Instant::now();
    let n = leak(Notify::new());
    let sent = leak(AtomicU64::new(0));
    let seq = leak(AtomicUsize::new(0));
    let received = leak(AtomicUsize::new(0));
    let h = rt.spawn(async move {
        let mut lat = Vec::with_capacity(samples);
        let mut seen = 0;
        while seen < samples {
            while seq.load(Acquire) == seen {
                n.notified().await;
            }
            let now = base.elapsed().as_nanos() as u64;
            lat.push((now - sent.load(Relaxed)) as f64 / 1000.0);
            seen += 1;
            received.store(seen, Release);
        }
        lat
    });
    std::thread::sleep(Duration::from_millis(20));
    let mut next = Instant::now();
    for i in 0..samples {
        next += period;
        if period >= Duration::from_millis(1) {
            std::thread::sleep(next.saturating_duration_since(Instant::now()));
        } else {
            while Instant::now() < next {}
        }
        sent.store(base.elapsed().as_nanos() as u64, Relaxed);
        seq.store(i + 1, Release);
        n.notify_one();
        while received.load(Acquire) <= i {
            if period >= Duration::from_millis(1) {
                std::thread::yield_now();
            }
        }
    }
    let lat = rt.block_on(h).unwrap();
    report(&format!("wake task from thread, every {}us, {}w", period.as_micros(), workers), lat);
}

fn channel_pingpong(cap: usize, items: usize) {
    let rt = rt(2);
    let t = rt.block_on(async move {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<i64>(cap);
        let consumer = tokio::spawn(async move {
            let mut s = 0i64;
            while let Some(v) = rx.recv().await {
                s += v;
            }
            s
        });
        let producer = tokio::spawn(async move {
            let t0 = Instant::now();
            for i in 0..items as i64 {
                tx.send(i).await.unwrap();
            }
            t0.elapsed()
        });
        let e = producer.await.unwrap();
        consumer.await.unwrap();
        e
    });
    println!("mpsc cap {:3}, 1P/1C on 2 workers          {:7.1} ns per item", cap, t.as_nanos() as f64 / items as f64);
}

// ---- CPU usage under sparse load (schedstat, producer excluded) ------------
fn thread_times() -> Vec<(String, u64)> {
    let mut out = vec![];
    for e in std::fs::read_dir("/proc/self/task").unwrap() {
        let e = e.unwrap();
        let name = e.file_name().into_string().unwrap();
        if let Ok(s) = std::fs::read_to_string(e.path().join("schedstat")) {
            out.push((name, s.split_whitespace().next().unwrap().parse().unwrap()));
        }
    }
    out
}
fn self_tid() -> String {
    let p = std::fs::read_link("/proc/thread-self").unwrap();
    p.file_name().unwrap().to_string_lossy().into_owned()
}
fn cpu_window(name: &str, run: impl FnOnce() -> (f64, String)) {
    let main = self_tid();
    let before = thread_times();
    let t0 = Instant::now();
    let (events, excluded) = run();
    let wall = t0.elapsed().as_secs_f64();
    let after = thread_times();
    let mut cpu = 0.0;
    for (tid, ns) in &after {
        if *tid == main || *tid == excluded {
            continue;
        }
        if let Some((_, b)) = before.iter().find(|(t, _)| t == tid) {
            cpu += (ns - b) as f64 * 1e-9;
        }
    }
    println!("{:<44} {:6.2}% {:9.0}/s {:8.2} us/event", name, 100.0 * cpu / wall, events / wall, 1e6 * cpu / events);
}

fn cpu_sleep_loop(workers: usize) {
    let rt = rt(workers);
    let stop = leak(AtomicBool::new(false));
    let count = leak(AtomicU64::new(0));
    let h = rt.spawn(async move {
        while !stop.load(Relaxed) {
            tokio::time::sleep(Duration::from_millis(1)).await;
            count.fetch_add(1, Relaxed);
        }
    });
    std::thread::sleep(Duration::from_millis(300));
    cpu_window(&format!("1 x sleep(1ms) loop, {} workers", workers), || {
        let c0 = count.load(Relaxed);
        std::thread::sleep(Duration::from_millis(1500));
        ((count.load(Relaxed) - c0) as f64, String::new())
    });
    stop.store(true, Relaxed);
    rt.block_on(h).unwrap();
}

fn cpu_trickle(workers: usize, period: Duration) {
    let rt = rt(workers);
    let handle = rt.handle().clone();
    let stop = leak(AtomicBool::new(false));
    let spawned = leak(AtomicU64::new(0));
    let tid = leak(std::sync::Mutex::new(String::new()));
    let producer = std::thread::spawn(move || {
        *tid.lock().unwrap() = self_tid();
        let mut next = Instant::now();
        while !stop.load(Relaxed) {
            next += period;
            if period >= Duration::from_millis(1) {
                std::thread::sleep(next.saturating_duration_since(Instant::now()));
            } else {
                while Instant::now() < next {}
            }
            handle.spawn(async {});
            spawned.fetch_add(1, Relaxed);
        }
    });
    std::thread::sleep(Duration::from_millis(300));
    cpu_window(&format!("empty task every {}us (foreign), {} workers", period.as_micros(), workers), || {
        let c0 = spawned.load(Relaxed);
        std::thread::sleep(Duration::from_millis(1500));
        ((spawned.load(Relaxed) - c0) as f64, tid.lock().unwrap().clone())
    });
    stop.store(true, Relaxed);
    producer.join().unwrap();
}

fn main() {
    let what = std::env::args().nth(1).unwrap_or_else(|| "all".into());
    let ns = [1usize, 2, 4, 8, 16, 24];
    if what == "all" || what == "scale" {
        for &n in &ns {
            scale_yield(n);
        }
        for &n in &ns {
            scale_spawn(n);
        }
        for &n in &ns {
            scale_pingpong(n);
        }
    }
    if what == "all" || what == "mem" {
        footprint(10000);
        footprint(100000);
    }
    if what == "all" || what == "lat" {
        wake_latency(4, Duration::from_micros(1000), 3000);
        wake_latency(24, Duration::from_micros(1000), 3000);
        wake_latency(4, Duration::from_micros(20), 20000);
    }
    if what == "all" || what == "chan" {
        channel_pingpong(1, 200000);
        channel_pingpong(64, 2000000);
    }
    if what == "all" || what == "cpu" {
        cpu_sleep_loop(24);
        for p in [10000u64, 1000, 100, 10] {
            cpu_trickle(24, Duration::from_micros(p));
        }
    }
}
