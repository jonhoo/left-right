#[cfg(loom)]
#[cfg(test)]
mod loom_tests {
    use left_right::{Absorb, ReadGuard};
    use loom::sync::atomic::AtomicUsize;
    use loom::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use loom::sync::Arc;
    use loom::thread;

    // TODO Would love to not re-define this!
    struct CounterAddOp(i32);

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

    #[test]
    fn read_before_publish() {
        loom::model(|| {
            let (mut w, r) = left_right::new::<i32, _>();
            w.append(CounterAddOp(1));
            w.publish();

            let jh = thread::spawn(move || {
                w.publish();
                w.append(CounterAddOp(1));
            });

            let val = *r.enter().unwrap();

            jh.join().unwrap();

            assert_eq!(1, val);
        });
    }
}
