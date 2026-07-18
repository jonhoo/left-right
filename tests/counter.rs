use left_right::Absorb;
use std::thread;

struct CounterAddOp(i32);
impl Absorb<CounterAddOp> for i32 {
    fn absorb_first(&mut self, operation: &mut CounterAddOp, _: &Self) {
        *self += operation.0;
    }
    fn absorb_second(&mut self, operation: CounterAddOp, _: &Self) {
        *self += operation.0;
    }
    fn sync_with(&mut self, first: &Self) {
        *self = *first
    }
}

#[test]
fn reentrant_enter_during_drop() {
    let (w, r) = left_right::new::<i32, CounterAddOp>();

    let _guard = r.enter().unwrap();
    thread::spawn(move || drop(w));

    while !r.was_dropped() {
        thread::yield_now();
    }

    assert!(r.enter().is_none());
}
