#[cfg(all(test, loom))]
mod loom_tests {
    // Evil hack to share CounterAddOp between
    // unit tests and integration tests.
    use left_right::{Absorb, TryCompressResult};
    include!("../src/utilities.rs");

    use loom::thread;

    #[test]
    fn read_before_publish() {
        loom::model(|| {
            let (mut w, r) = left_right::new::<i32, _>();

            w.append(CounterAddOp(1));
            w.publish();

            let jh = thread::spawn(move || *r.enter().unwrap());

            w.publish();
            w.append(CounterAddOp(1));

            let val = jh.join().unwrap();

            assert_eq!(1, val);
        });
    }
}
