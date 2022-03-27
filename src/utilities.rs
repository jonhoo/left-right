#[cfg(test)]
use alloc::boxed::Box;

#[cfg(test)]
#[derive(Debug)]
pub struct CounterAddOp(pub i32);

#[cfg(test)]
impl Absorb<CounterAddOp> for i32 {
    fn absorb_first(&mut self, operation: &mut CounterAddOp, _: &Self) {
        *self += operation.0;
    }

    fn absorb_second(&mut self, operation: CounterAddOp, _: &Self) {
        *self += operation.0;
    }

    fn drop_first(self: Box<Self>) {}

    fn sync_with(&mut self, first: &Self) {
        *self = *first
    }
}
