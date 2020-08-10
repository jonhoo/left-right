#[cfg(loom)]
#[cfg(test)]
mod loom_tests {
    use loom::sync::atomic::AtomicUsize;
    use loom::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use loom::sync::Arc;
    use loom::thread;

    #[test]
    #[should_panic]
    fn buggy_concurrent_inc() {
        loom::model(|| {
            let num = Arc::new(AtomicUsize::new(0));

            let ths: Vec<_> = (0..2)
                .map(|_| {
                    let num = num.clone();
                    thread::spawn(move || {
                        let curr = num.load(Acquire);
                        num.store(curr + 1, Release);
                    })
                })
                .collect();

            for th in ths {
                th.join().unwrap();
            }

            assert_eq!(2, num.load(Relaxed));
        });
    }
}
