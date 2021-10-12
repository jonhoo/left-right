use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use left_right::*;
use rand::{distributions::Uniform, Rng};
use std::collections::VecDeque;

macro_rules! bench_instance {
    (
        name: $name: ident,
        range: $range: literal,
        delays: {
            absorb_set: $absorb_set: literal,
            absorb_clear: $absorb_clear: literal,
            compress_set: $compress_set: literal,
            compress_clear: $compress_clear: literal,
        },
        ops: {
            key_bits: $key_bits: literal,
            clear_bits: $clear_bits: literal,
        },
        iteration: {
            len: $len: literal,
            chunk_len: $chunk_len: literal,
            publish_len: $publish_len: literal,
        }
    ) => {
        fn $name(c: &mut Criterion) {
            run::<$range>(
                c,
                stringify!($name),
                $key_bits,
                $clear_bits,
                $absorb_set,
                $absorb_clear,
                $compress_set,
                $compress_clear,
                $len,
                $chunk_len,
                $publish_len,
            )
        }
    };
}

/* # About the values used:
 * delays: number of iterations inside a black-box spin-loop
 *  absorb_set: Higher values MASSIVELY benefit compression. 500 is about a lookup in a small Map (n~=64, Hash ~= BTree) followed by a non-trivial String operation.
 *  absorb_clear: Higher values MASSIVELY benefit compression. 12000 is about clearing a small Map of Strings (n~=64).
 *  compress_set: Lower values benefit compression. 125 is about a non-trivial String operation.
 *  compress_clear: Lower values benefit compression. slightly lower than compress_set because we just discard a String instead of combining two.
 *  !compress_not: No setting for Independent/Dependent delay because even 0 would be far greater than a simple key-mem-inequality check and would grind compression to a screeching halt.
 * ops:
 *  key_bits: Lower values MASSIVELY benefit high-range compression. 6 is equivalent to 64 entries, (very) low for big Maps, but high for non-map values.
 *  clear_bits: Lower values MASSIVELY benefit low-range compression. 11 is equivalent to clearing a Map every 2048 operations, (VERY) low for big Maps, maybe reasonable for an arena of some kind?
 * iteration:
 *  len: No performance benefit either way.
 *  chunk_len: Higher values slightly benefit high-range compression by amortizing linear none-removal. TODO: Maybe implement buffered appends?
 *  publish_len: Higher values greatly benefit compressions memory savings, but shouldn't have a noticeable performance impact either way.
 */

bench_instance!(
    name: none_favorable,
    range: 0,
    delays: {
        absorb_set: 1000,
        absorb_clear: 15000,
        compress_set: 125,
        compress_clear: 100,
    },
    ops: {
        key_bits: 6,
        clear_bits: 11,
    },
    iteration: {
        len: 0x20000,
        chunk_len: 0x80,
        publish_len: 0x800,
    }
);
bench_instance!(
    name: max_favorable,
    range: 0xFFFFFFFFFFFFFFFF,
    delays: {
        absorb_set: 1000,
        absorb_clear: 15000,
        compress_set: 125,
        compress_clear: 100,
    },
    ops: {
        key_bits: 6,
        clear_bits: 11,
    },
    iteration: {
        len: 0x20000,
        chunk_len: 0x80,
        publish_len: 0x800,
    }
);
bench_instance!(
    name: r1_favorable,
    range: 1,
    delays: {
        absorb_set: 1000,
        absorb_clear: 15000,
        compress_set: 125,
        compress_clear: 100,
    },
    ops: {
        key_bits: 6,
        clear_bits: 11,
    },
    iteration: {
        len: 0x20000,
        chunk_len: 0x80,
        publish_len: 0x800,
    }
);
bench_instance!(
    name: r16_favorable,
    range: 16,
    delays: {
        absorb_set: 1000,
        absorb_clear: 15000,
        compress_set: 125,
        compress_clear: 100,
    },
    ops: {
        key_bits: 6,
        clear_bits: 11,
    },
    iteration: {
        len: 0x20000,
        chunk_len: 0x80,
        publish_len: 0x800,
    }
);
bench_instance!(
    name: r64_favorable,
    range: 64,
    delays: {
        absorb_set: 1000,
        absorb_clear: 15000,
        compress_set: 125,
        compress_clear: 100,
    },
    ops: {
        key_bits: 6,
        clear_bits: 11,
    },
    iteration: {
        len: 0x20000,
        chunk_len: 0x80,
        publish_len: 0x800,
    }
);

bench_instance!(
    name: none_unfavorable,
    range: 0,
    delays: {
        absorb_set: 500,
        absorb_clear: 10000,
        compress_set: 125,
        compress_clear: 100,
    },
    ops: {
        key_bits: 8,
        clear_bits: 14,
    },
    iteration: {
        len: 0x20000,
        chunk_len: 0x80,
        publish_len: 0x800,
    }
);
bench_instance!(
    name: max_unfavorable,
    range: 0xFFFFFFFFFFFFFFFF,
    delays: {
        absorb_set: 500,
        absorb_clear: 10000,
        compress_set: 125,
        compress_clear: 100,
    },
    ops: {
        key_bits: 8,
        clear_bits: 14,
    },
    iteration: {
        len: 0x20000,
        chunk_len: 0x80,
        publish_len: 0x800,
    }
);
bench_instance!(
    name: r1_unfavorable,
    range: 1,
    delays: {
        absorb_set: 500,
        absorb_clear: 10000,
        compress_set: 125,
        compress_clear: 100,
    },
    ops: {
        key_bits: 8,
        clear_bits: 14,
    },
    iteration: {
        len: 0x20000,
        chunk_len: 0x80,
        publish_len: 0x800,
    }
);
bench_instance!(
    name: r16_unfavorable,
    range: 16,
    delays: {
        absorb_set: 500,
        absorb_clear: 10000,
        compress_set: 125,
        compress_clear: 100,
    },
    ops: {
        key_bits: 8,
        clear_bits: 14,
    },
    iteration: {
        len: 0x20000,
        chunk_len: 0x80,
        publish_len: 0x800,
    }
);
bench_instance!(
    name: r64_unfavorable,
    range: 64,
    delays: {
        absorb_set: 500,
        absorb_clear: 10000,
        compress_set: 125,
        compress_clear: 100,
    },
    ops: {
        key_bits: 8,
        clear_bits: 14,
    },
    iteration: {
        len: 0x20000,
        chunk_len: 0x80,
        publish_len: 0x800,
    }
);

bench_instance!(
    name: none_no_clear,
    range: 0,
    delays: {
        absorb_set: 500,
        absorb_clear: 10000,
        compress_set: 125,
        compress_clear: 100,
    },
    ops: {
        key_bits: 8,
        clear_bits: 63,
    },
    iteration: {
        len: 0x20000,
        chunk_len: 0x80,
        publish_len: 0x800,
    }
);
bench_instance!(
    name: r1_no_clear,
    range: 1,
    delays: {
        absorb_set: 500,
        absorb_clear: 10000,
        compress_set: 125,
        compress_clear: 100,
    },
    ops: {
        key_bits: 8,
        clear_bits: 63,
    },
    iteration: {
        len: 0x20000,
        chunk_len: 0x80,
        publish_len: 0x800,
    }
);

criterion_group!(
    benches,
    none_favorable,
    max_favorable,
    r1_favorable,
    r16_favorable,
    r64_favorable,
    none_unfavorable,
    max_unfavorable,
    r1_unfavorable,
    r16_unfavorable,
    r64_unfavorable,
    none_no_clear,
    r1_no_clear
);
criterion_main!(benches);

fn run<const RANGE: usize>(
    c: &mut Criterion,
    name: &str,
    key_bits: u8,
    clear_bits: u8,
    absorb_set: u16,
    absorb_clear: u16,
    compress_set: u8,
    compress_clear: u8,
    len: usize,
    chunk_len: usize,
    publish_len: usize,
) {
    c.bench_function(name, |b| {
        b.iter_batched(
            || {
                let ops = random_ops(key_bits, clear_bits, compress_set, compress_clear, len);
                let (w, _) = new_from_empty::<_, FakeMapOp>(FakeMap::<RANGE> {
                    absorb_set,
                    absorb_clear,
                });
                (ops, w)
            },
            |(mut ops, mut w)| {
                let mut log_len = 0;
                while !ops.is_empty() {
                    w.extend(ops.drain(0..black_box(chunk_len)));
                    log_len += chunk_len;
                    if log_len >= publish_len {
                        log_len -= publish_len;
                        w.publish();
                    }
                }
            },
            BatchSize::LargeInput,
        )
    });
}

pub(crate) fn random_ops(
    key_bits: u8,
    clear_bits: u8,
    compress_set: u8,
    compress_clear: u8,
    len: usize,
) -> VecDeque<FakeMapOp> {
    let rng = rand::thread_rng();
    let dist = Uniform::new(0, usize::MAX);
    rng.sample_iter(&dist)
        .take(len)
        .map(|x| {
            let key = (x & !((!0) << key_bits)) as u16;
            if x & !((!0) << clear_bits) == 0 {
                FakeMapOp::Clear {
                    compress_set,
                    compress_clear,
                }
            } else {
                FakeMapOp::Set {
                    key,
                    compress_set,
                    compress_clear,
                }
            }
        })
        .collect()
}

fn black_box_spin(spin_count: usize) {
    let mut counter = 1;
    for spin_count in 0..black_box(spin_count) {
        counter += black_box(spin_count);
    }
    if black_box(counter) > 0 {
        Some(())
    } else {
        None
    }
    .unwrap()
}

pub(crate) enum FakeMapOp {
    Set {
        key: u16,
        compress_set: u8,
        compress_clear: u8,
    },
    Clear {
        compress_set: u8,
        compress_clear: u8,
    },
}
#[derive(Clone, Debug, Default)]
pub(crate) struct FakeMap<const RANGE: usize> {
    absorb_set: u16,
    absorb_clear: u16,
}
impl<const RANGE: usize> Absorb<FakeMapOp> for FakeMap<RANGE> {
    fn absorb_first(&mut self, operation: &mut FakeMapOp, _: &Self) {
        black_box_spin(match operation {
            FakeMapOp::Set { .. } => self.absorb_set,
            FakeMapOp::Clear { .. } => self.absorb_clear,
        } as usize);
    }
    fn sync_with(&mut self, first: &Self) {
        *self = first.clone();
    }

    const MAX_COMPRESS_RANGE: usize = RANGE;
    fn try_compress(mut prev: &mut FakeMapOp, next: FakeMapOp) -> TryCompressResult<FakeMapOp> {
        match (&mut prev, next) {
            (
                FakeMapOp::Set { key: prev_key, .. },
                FakeMapOp::Set {
                    key,
                    compress_set,
                    compress_clear,
                },
            ) => {
                if *prev_key == key {
                    black_box_spin(compress_set as usize);
                    TryCompressResult::Compressed
                } else {
                    TryCompressResult::Independent(FakeMapOp::Set {
                        key,
                        compress_set,
                        compress_clear,
                    })
                }
            }
            (
                _,
                FakeMapOp::Clear {
                    compress_set,
                    compress_clear,
                },
            ) => {
                black_box_spin(compress_clear as usize);
                *prev = FakeMapOp::Clear {
                    compress_set,
                    compress_clear,
                };
                TryCompressResult::Compressed
            }
            (FakeMapOp::Clear { .. }, next @ FakeMapOp::Set { .. }) => {
                TryCompressResult::Dependent(next)
            }
        }
    }
}
