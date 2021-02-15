#[cfg(loom)]
#[cfg(test)]
mod loom_tests {
    // Evil hack to share CounterAddOp between
    // unit tests and integration tests.
    use left_right::Absorb;
    include!("../src/utilities.rs");

    use loom::sync::atomic::{AtomicI32, Ordering};
    use loom::sync::Arc;
    use loom::thread;

    #[test]
    fn read_before_publish() {
        loom::model(|| {
            let (mut w, r) = left_right::new::<i32, _>();
            let answer = Arc::new(AtomicI32::new(0));
            let answer_clone = answer.clone();

            w.append(CounterAddOp(1));
            w.publish();

            let jh = thread::spawn(move || {
                let val = *r.enter().unwrap();
                answer_clone.store(val, Ordering::SeqCst);
            });
            w.publish();
            w.append(CounterAddOp(1));

            jh.join().unwrap();

            // This is just to ensure the write handle is not dropped before the reader reads
            w.flush();

            assert_eq!(1, answer.load(Ordering::SeqCst));
        });
    }
}
