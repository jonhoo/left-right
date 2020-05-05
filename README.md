# evmap

[![Crates.io](https://img.shields.io/crates/v/evmap.svg)](https://crates.io/crates/evmap)
[![Documentation](https://docs.rs/evmap/badge.svg)](https://docs.rs/evmap/)
[![Build Status](https://dev.azure.com/jonhoo/jonhoo/_apis/build/status/evmap?branchName=master)](https://dev.azure.com/jonhoo/jonhoo/_build/latest?definitionId=8&branchName=master)
[![Codecov](https://codecov.io/github/jonhoo/rust-evmap/coverage.svg?branch=master)](https://codecov.io/gh/jonhoo/rust-evmap)

A lock-free, eventually consistent, concurrent multi-value map.

This map implementation allows reads and writes to execute entirely in parallel, with no
implicit synchronization overhead. Reads never take locks on their critical path, and neither
do writes assuming there is a single writer (multi-writer is possible using a `Mutex`), which
significantly improves performance under contention.

The trade-off exposed by this module is one of eventual consistency: writes are not visible to
readers except following explicit synchronization. Specifically, readers only see the
operations that preceeded the last call to `WriteHandle::refresh` by a writer. This lets
writers decide how stale they are willing to let reads get. They can refresh the map after
every write to emulate a regular concurrent `HashMap`, or they can refresh only occasionally to
reduce the synchronization overhead at the cost of stale reads.

For read-heavy workloads, the scheme used by this module is particularly useful. Writers can
afford to refresh after every write, which provides up-to-date reads, and readers remain fast
as they do not need to ever take locks.

The map is multi-value, meaning that every key maps to a *collection* of values. This
introduces some memory cost by adding a layer of indirection through a `Vec` for each value,
but enables more advanced use. This choice was made as it would not be possible to emulate such
functionality on top of the semantics of this map (think about it -- what would the operational
log contain?).

To faciliate more advanced use-cases, each of the two maps also carry some customizeable
meta-information. The writers may update this at will, and when a refresh happens, the current
meta will also be made visible to readers. This could be useful, for example, to indicate what
time the refresh happened.

## Performance

I've run some benchmarks of evmap against a standard Rust `HashMap` protected
by a [reader-writer
lock](https://doc.rust-lang.org/std/sync/struct.RwLock.html), as well as
against [chashmap](https://crates.io/crates/chashmap) — a crate which provides
"concurrent hash maps, based on bucket-level multi-reader locks". The
benchmarks were run using the binary in [benchmark/](benchmark/src/main.rs) on
a 40-core machine with Intel(R) Xeon(R) CPU E5-2660 v3 @ 2.60GHz CPUs.

The benchmark runs a number of reader and writer threads in tight loops, each
of which does a read or write to a random key in the map respectively. Results
for both uniform and skewed distributions are provided below. The benchmark
measures the average number of reads and writes per second as the number of
readers and writers increases.

Preliminary results show that `evmap` performs well under contention,
especially on the read side. This benchmark represents the worst-case usage of
`evmap` in which every write also does a `refresh`. If the map is refreshed
less often, performance increases (see bottom plot).

![Read throughput](benchmark/read-throughput.png)
![Write throughput](benchmark/write-throughput.png)
![Write throughput](benchmark/write-with-refresh.png)
