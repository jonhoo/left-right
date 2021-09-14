#[cfg(test)]
#[derive(Debug)]
pub struct CounterAddOp(pub i32);

#[cfg(test)]
impl Absorb<CounterAddOp> for i32 {
    fn absorb_first(&mut self, operation: &mut CounterAddOp, _: &Self) {
        *self += operation.0;
    }

    fn sync_with(&mut self, first: &Self) {
        *self = *first
    }
}

#[cfg(test)]
#[derive(Debug)]
pub enum CompressibleCounterOp<const MAX_COMPRESS_RANGE: usize> {
    Set(i32),
    Add(i32),
    Sub(i32),
}

#[cfg(test)]
impl<const MAX_COMPRESS_RANGE: usize> Absorb<CompressibleCounterOp<MAX_COMPRESS_RANGE>> for i32 {
    fn absorb_first(
        &mut self,
        operation: &mut CompressibleCounterOp<MAX_COMPRESS_RANGE>,
        _: &Self,
    ) {
        match operation {
            CompressibleCounterOp::Set(v) => *self = *v,
            CompressibleCounterOp::Add(v) => *self += *v,
            CompressibleCounterOp::Sub(v) => *self -= *v,
        }
    }

    fn sync_with(&mut self, first: &Self) {
        *self = *first
    }

    fn max_compress_range() -> &'static usize {
        &MAX_COMPRESS_RANGE
    }

    fn try_compress(
        prev: CompressibleCounterOp<MAX_COMPRESS_RANGE>,
        next: CompressibleCounterOp<MAX_COMPRESS_RANGE>,
    ) -> TryCompressResult<CompressibleCounterOp<MAX_COMPRESS_RANGE>> {
        match (prev, next) {
            (CompressibleCounterOp::Add(prev), CompressibleCounterOp::Add(next)) => {
                TryCompressResult::Compressed {
                    result: CompressibleCounterOp::Add(prev + next),
                }
            }
            (CompressibleCounterOp::Sub(prev), CompressibleCounterOp::Sub(next)) => {
                TryCompressResult::Compressed {
                    result: CompressibleCounterOp::Sub(prev + next),
                }
            }
            (CompressibleCounterOp::Add(prev), CompressibleCounterOp::Sub(next)) => {
                TryCompressResult::Independent {
                    prev: CompressibleCounterOp::Add(prev),
                    next: CompressibleCounterOp::Sub(next),
                }
            }
            (CompressibleCounterOp::Sub(prev), CompressibleCounterOp::Add(next)) => {
                TryCompressResult::Independent {
                    prev: CompressibleCounterOp::Sub(prev),
                    next: CompressibleCounterOp::Add(next),
                }
            }
            (CompressibleCounterOp::Set(prev), next) => TryCompressResult::Dependent {
                prev: CompressibleCounterOp::Set(prev),
                next,
            },
            (_, CompressibleCounterOp::Set(next)) => TryCompressResult::Compressed {
                result: CompressibleCounterOp::Set(next),
            },
        }
    }
}
