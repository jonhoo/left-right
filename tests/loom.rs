#[cfg(loom)]
#[cfg(test)]
mod loom_tests {
    // Evil hack to share CounterAddOp between
    // unit tests and integration tests.
    use left_right::Absorb;
    include!("../src/utilities.rs");

    use loom::thread;

    #[test]
    fn read_before_publish() {
        loom::model(|| {
            let (mut w, r) = left_right::new::<i32, _>();

            w.append(CounterAddOp(1));
            assert!(w.try_publish());

            let jh = thread::spawn(move || *r.enter().unwrap());

            w.publish();
            w.append(CounterAddOp(1));

            let val = jh.join().unwrap();

            assert_eq!(1, val);
        });
    }

    #[test]
    fn create_handle_while_writer_waits() {
        loom::model(|| {
            let (mut w, r) = left_right::new::<i32, CounterAddOp>();
            w.publish();

            let factory = r.factory();
            let guard = r.enter().unwrap();
            w.publish();

            let writer = thread::spawn(move || {
                w.publish();
            });
            thread::yield_now();

            let creator = thread::spawn(move || factory.handle());
            let handle = creator.join().unwrap();
            drop(handle);

            drop(guard);
            writer.join().unwrap();
        });
    }
}
