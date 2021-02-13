#[cfg(loom)]
#[cfg(test)]
mod loom_tests {
    // Evil hack to share CounterAddOp between
    // unit tests and integration tests.
    use left_right::Absorb;
    include!("../src/utilities.rs");

    use left_right::ReadGuard;
    use loom::sync::atomic::AtomicUsize;
    use loom::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use loom::sync::Arc;
    use loom::thread;

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
