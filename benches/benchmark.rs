use std::collections::{BTreeMap, HashMap};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
mod utilities;
use left_right::*;
use utilities::*;

// Number of ops to insert/publish in total
const LEN: usize = 1 << 16;
// Number of ops per extend
const CHUNK_LEN: usize = 1 << 6;
// Number of ops between publishes
const FREQ: usize = 1 << 10;

fn hash_max(c: &mut Criterion) {
    c.bench_function("hash_max", |b| {
        b.iter_batched(
            || {
                let ops = random_ops(LEN);
                let (w, _) = new::<HashMap<_, _>, MapOp<{ usize::MAX }>>();
                (ops, w)
            },
            |(mut ops, mut w)| {
                let mut log_len = 0;
                while !ops.is_empty() {
                    w.extend(ops.drain(0..CHUNK_LEN));
                    log_len += CHUNK_LEN;
                    if log_len >= FREQ {
                        log_len -= FREQ;
                        w.publish();
                    }
                }
            },
            BatchSize::LargeInput,
        )
    });
}

fn btree_max(c: &mut Criterion) {
    c.bench_function("btree_max", |b| {
        b.iter_batched(
            || {
                let ops = random_ops(LEN);
                let (w, _) = new::<BTreeMap<_, _>, MapOp<{ usize::MAX }>>();
                (ops, w)
            },
            |(mut ops, mut w)| {
                let mut log_len = 0;
                while !ops.is_empty() {
                    w.extend(ops.drain(0..CHUNK_LEN));
                    log_len += CHUNK_LEN;
                    if log_len >= FREQ {
                        log_len -= FREQ;
                        w.publish();
                    }
                }
            },
            BatchSize::LargeInput,
        )
    });
}
fn hash_1(c: &mut Criterion) {
    c.bench_function("hash_1", |b| {
        b.iter_batched(
            || {
                let ops = random_ops(LEN);
                let (w, _) = new::<HashMap<_, _>, MapOp<1>>();
                (ops, w)
            },
            |(mut ops, mut w)| {
                let mut log_len = 0;
                while !ops.is_empty() {
                    w.extend(ops.drain(0..CHUNK_LEN));
                    log_len += CHUNK_LEN;
                    if log_len >= FREQ {
                        log_len -= FREQ;
                        w.publish();
                    }
                }
            },
            BatchSize::LargeInput,
        )
    });
}

fn btree_1(c: &mut Criterion) {
    c.bench_function("btree_1", |b| {
        b.iter_batched(
            || {
                let ops = random_ops(LEN);
                let (w, _) = new::<BTreeMap<_, _>, MapOp<1>>();
                (ops, w)
            },
            |(mut ops, mut w)| {
                let mut log_len = 0;
                while !ops.is_empty() {
                    w.extend(ops.drain(0..CHUNK_LEN));
                    log_len += CHUNK_LEN;
                    if log_len >= FREQ {
                        log_len -= FREQ;
                        w.publish();
                    }
                }
            },
            BatchSize::LargeInput,
        )
    });
}
fn hash_16(c: &mut Criterion) {
    c.bench_function("hash_16", |b| {
        b.iter_batched(
            || {
                let ops = random_ops(LEN);
                let (w, _) = new::<HashMap<_, _>, MapOp<16>>();
                (ops, w)
            },
            |(mut ops, mut w)| {
                let mut log_len = 0;
                while !ops.is_empty() {
                    w.extend(ops.drain(0..CHUNK_LEN));
                    log_len += CHUNK_LEN;
                    if log_len >= FREQ {
                        log_len -= FREQ;
                        w.publish();
                    }
                }
            },
            BatchSize::LargeInput,
        )
    });
}

fn btree_16(c: &mut Criterion) {
    c.bench_function("btree_16", |b| {
        b.iter_batched(
            || {
                let ops = random_ops(LEN);
                let (w, _) = new::<BTreeMap<_, _>, MapOp<16>>();
                (ops, w)
            },
            |(mut ops, mut w)| {
                let mut log_len = 0;
                while !ops.is_empty() {
                    w.extend(ops.drain(0..CHUNK_LEN));
                    log_len += CHUNK_LEN;
                    if log_len >= FREQ {
                        log_len -= FREQ;
                        w.publish();
                    }
                }
            },
            BatchSize::LargeInput,
        )
    });
}
fn hash_none(c: &mut Criterion) {
    c.bench_function("hash_none", |b| {
        b.iter_batched(
            || {
                let ops = random_ops(LEN);
                let (w, _) = new::<HashMap<_, _>, MapOp<0>>();
                (ops, w)
            },
            |(mut ops, mut w)| {
                let mut log_len = 0;
                while !ops.is_empty() {
                    w.extend(ops.drain(0..CHUNK_LEN));
                    log_len += CHUNK_LEN;
                    if log_len >= FREQ {
                        log_len -= FREQ;
                        w.publish();
                    }
                }
            },
            BatchSize::LargeInput,
        )
    });
}

fn btree_none(c: &mut Criterion) {
    c.bench_function("btree_none", |b| {
        b.iter_batched(
            || {
                let ops = random_ops(LEN);
                let (w, _) = new::<BTreeMap<_, _>, MapOp<0>>();
                (ops, w)
            },
            |(mut ops, mut w)| {
                let mut log_len = 0;
                while !ops.is_empty() {
                    w.extend(ops.drain(0..CHUNK_LEN));
                    log_len += CHUNK_LEN;
                    if log_len >= FREQ {
                        log_len -= FREQ;
                        w.publish();
                    }
                }
            },
            BatchSize::LargeInput,
        )
    });
}

criterion_group!(
    benches, btree_max, btree_16, btree_1, btree_none, hash_max, hash_16, hash_1, hash_none,
);
criterion_main!(benches);
