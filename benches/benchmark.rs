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
            run::<
                $key_bits,
                $clear_bits,
                $len,
                $chunk_len,
                $publish_len,
                $range,
                $absorb_set,
                $absorb_clear,
                $compress_set,
                $compress_clear,
            >(c, stringify!($name))
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
    name: none,
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
    name: max,
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
    name: r1,
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
    name: r16,
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
    name: r64,
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

criterion_group!(benches, none, max, r1, r16, r64);
criterion_main!(benches);

fn run<
    const KEY_BITS: usize,
    const CLEAR_BITS: usize,
    const LEN: usize,
    const CHUNK_LEN: usize,
    const PUBLISH_LEN: usize,
    const RANGE: usize,
    const ABSORB_SET: usize,
    const ABSORB_CLEAR: usize,
    const COMPRESS_SET: usize,
    const COMPRESS_CLEAR: usize,
>(
    c: &mut Criterion,
    name: &str,
) {
    c.bench_function(name, |b| {
        b.iter_batched(
            || {
                let ops = random_ops::<KEY_BITS, CLEAR_BITS, LEN, CHUNK_LEN, PUBLISH_LEN, RANGE>();
                let (w, _) = new::<
                    FakeMap<ABSORB_SET, ABSORB_CLEAR, COMPRESS_SET, COMPRESS_CLEAR>,
                    FakeMapOp<RANGE>,
                >();
                (ops, w)
            },
            |(mut ops, mut w)| {
                let mut log_len = 0;
                while !ops.is_empty() {
                    w.extend(ops.drain(0..black_box(CHUNK_LEN)));
                    log_len += CHUNK_LEN;
                    if log_len >= PUBLISH_LEN {
                        log_len -= PUBLISH_LEN;
                        w.publish();
                    }
                }
            },
            BatchSize::LargeInput,
        )
    });
}

pub(crate) fn random_ops<
    const KEY_BITS: usize,
    const CLEAR_BITS: usize,
    const LEN: usize,
    const CHUNK_LEN: usize,
    const PUBLISH_LEN: usize,
    const RANGE: usize,
>() -> VecDeque<FakeMapOp<RANGE>> {
    let rng = rand::thread_rng();
    let dist = Uniform::new(0, usize::MAX);
    rng.sample_iter(&dist)
        .take(LEN)
        .map(|x| {
            let key = x & !((!1) << KEY_BITS);
            if x & !((!1) << CLEAR_BITS) == 0 {
                FakeMapOp::Clear
            } else {
                FakeMapOp::Set(key)
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

pub(crate) enum FakeMapOp<const RANGE: usize> {
    Set(usize),
    Clear,
}
#[derive(Clone, Debug, Default)]
pub(crate) struct FakeMap<
    const ABSORB_SET: usize,
    const ABSORB_CLEAR: usize,
    const COMPRESS_SET: usize,
    const COMPRESS_CLEAR: usize,
>;
impl<
        const RANGE: usize,
        const ABSORB_SET: usize,
        const ABSORB_CLEAR: usize,
        const COMPRESS_SET: usize,
        const COMPRESS_CLEAR: usize,
    > Absorb<FakeMapOp<RANGE>> for FakeMap<ABSORB_SET, ABSORB_CLEAR, COMPRESS_SET, COMPRESS_CLEAR>
{
    fn absorb_first(&mut self, operation: &mut FakeMapOp<RANGE>, _: &Self) {
        black_box_spin(match operation {
            FakeMapOp::Set(_) => ABSORB_SET,
            FakeMapOp::Clear => ABSORB_CLEAR,
        });
    }
    fn sync_with(&mut self, first: &Self) {
        *self = first.clone();
    }

    const MAX_COMPRESS_RANGE: usize = RANGE;
    fn try_compress(
        mut prev: &mut FakeMapOp<RANGE>,
        next: FakeMapOp<RANGE>,
    ) -> TryCompressResult<FakeMapOp<RANGE>> {
        match (&mut prev, next) {
            (FakeMapOp::Set(prev_key), FakeMapOp::Set(key)) => {
                if *prev_key == key {
                    black_box_spin(COMPRESS_SET);
                    TryCompressResult::Compressed
                } else {
                    TryCompressResult::Independent(FakeMapOp::Set(key))
                }
            }
            (_, FakeMapOp::Clear) => {
                black_box_spin(COMPRESS_CLEAR);
                *prev = FakeMapOp::Clear;
                TryCompressResult::Compressed
            }
            (FakeMapOp::Clear, next @ FakeMapOp::Set(_)) => TryCompressResult::Dependent(next),
        }
    }
}
