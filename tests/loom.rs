#[cfg(loom)]
#[cfg(test)]
mod loom_tests {
    extern crate evmap;

    use loom::sync::atomic::AtomicUsize;
    use loom::thread;

    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;

    #[test]
    fn evmap_read_while_remove() {
        loom::model(|| {
            let (r, mut w) = evmap::new();
            w.insert(1, 2);
            w.refresh();

            let val = Arc::new(AtomicUsize::new(0));
            let val_copy = Arc::clone(&val);

            let read_thread = thread::spawn(move || {
                val.store(*r.get_one(&1).as_deref().unwrap(), SeqCst);
            });

            w.remove_entry(1);

            read_thread.join().unwrap();

            assert_eq!(val_copy.load(SeqCst), 2);
        });
    }

    #[test]
    fn evmap_read_while_insert() {
        loom::model(|| {
            let (r, mut w) = evmap::new();

            let val = Arc::new(AtomicUsize::new(0));
            let val_copy = Arc::clone(&val);

            let read_thread = thread::spawn(move || {
                val.store(*r.get_one(&1).as_deref().unwrap_or(&1), SeqCst);
            });

            w.insert(1, 2);

            read_thread.join().unwrap();

            assert_eq!(val_copy.load(SeqCst), 1);
        });
    }

    #[test]
    fn evmap_read_while_purge() {
        loom::model(|| {
            let (r, mut w) = evmap::new();
            w.insert(1, 2);
            w.refresh();

            let val = Arc::new(AtomicUsize::new(0));
            let val_copy = Arc::clone(&val);

            let read_thread = thread::spawn(move || {
                val.store(*r.get_one(&1).as_deref().unwrap(), SeqCst);
            });

            w.purge();

            read_thread.join().unwrap();

            assert_eq!(val_copy.load(SeqCst), 2);
        });
    }
}
