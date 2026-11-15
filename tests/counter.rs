use left_right::{Absorb, ReadGuard};
use std::sync::mpsc::{channel, Receiver, Sender};
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

fn drop_guard_and_wait(guard: ReadGuard<'_, i32>, dropped: Sender<()>, resume: Receiver<()>) {
    assert_eq!(*guard, 1);
    drop(guard);
    dropped.send(()).unwrap();
    resume.recv().unwrap();
}

#[test]
fn publish_after_guard_drop() {
    let (mut w, r) = left_right::new::<i32, CounterAddOp>();
    w.append(CounterAddOp(1));
    w.publish();

    let (dropped_tx, dropped_rx) = channel();
    let (resume_tx, resume_rx) = channel();
    let reader = thread::spawn(move || {
        drop_guard_and_wait(r.enter().unwrap(), dropped_tx, resume_rx);
    });

    dropped_rx.recv().unwrap();
    w.append(CounterAddOp(1));
    w.publish();
    w.publish();
    resume_tx.send(()).unwrap();
    reader.join().unwrap();
}
