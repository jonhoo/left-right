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
#[derive(Debug, Eq, PartialEq)]
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

    const MAX_COMPRESS_RANGE: usize = MAX_COMPRESS_RANGE;

    fn try_compress(
        prev: &mut CompressibleCounterOp<MAX_COMPRESS_RANGE>,
        next: CompressibleCounterOp<MAX_COMPRESS_RANGE>,
    ) -> TryCompressResult<CompressibleCounterOp<MAX_COMPRESS_RANGE>> {
        match (prev, next) {
            (CompressibleCounterOp::Add(prev), CompressibleCounterOp::Add(next)) => {
                *prev += next;
                TryCompressResult::Compressed
            }
            (CompressibleCounterOp::Sub(prev), CompressibleCounterOp::Sub(next)) => {
                *prev += next;
                TryCompressResult::Compressed
            }
            (CompressibleCounterOp::Add(_), next @ CompressibleCounterOp::Sub(_)) => {
                TryCompressResult::Independent(next)
            }
            (CompressibleCounterOp::Sub(_), CompressibleCounterOp::Add(next)) => {
                TryCompressResult::Independent(CompressibleCounterOp::Add(next))
            }
            (CompressibleCounterOp::Set(_), next) => TryCompressResult::Dependent(next),
            (prev, CompressibleCounterOp::Set(next)) => {
                *prev = CompressibleCounterOp::Set(next);
                TryCompressResult::Compressed
            }
        }
    }
}
