extern crate evmap;

use std::iter::FromIterator;

macro_rules! assert_match {
    ($x:expr, $p:pat) => {
        if let $p = $x {
        } else {
            panic!(concat!(stringify!($x), " did not match ", stringify!($p)));
        }
    };
}

#[test]
fn it_works() {
    let x = ('x', 42);

    let (r, mut w) = evmap::new();

    // the map is uninitialized, so all lookups should return None
    assert_match!(r.get(&x.0), None);

    w.refresh();

    // after the first refresh, it is empty, but ready
    assert_match!(r.get(&x.0), None);
    // since we're not using `meta`, we get ()
    assert_match!(r.meta_get(&x.0), Some((None, ())));

    w.insert(x.0, x);

    // it is empty even after an add (we haven't refresh yet)
    assert_match!(r.get(&x.0), None);
    assert_match!(r.meta_get(&x.0), Some((None, ())));

    w.refresh();

    // but after the swap, the record is there!
    assert_match!(r.get(&x.0).map(|rs| rs.len()), Some(1));
    assert_match!(
        r.meta_get(&x.0).map(|(rs, m)| (rs.map(|rs| rs.len()), m)),
        Some((Some(1), ()))
    );
    assert_match!(
        r.get(&x.0)
            .map(|rs| rs.iter().any(|v| v.0 == x.0 && v.1 == x.1)),
        Some(true)
    );

    // non-existing records return None
    assert_match!(r.get(&'y').map(|rs| rs.len()), None);
    assert_match!(
        r.meta_get(&'y').map(|(rs, m)| (rs.map(|rs| rs.len()), m)),
        Some((None, ()))
    );

    // if we purge, the readers still see the values
    w.purge();
    assert_match!(
        r.get(&x.0)
            .map(|rs| rs.iter().any(|v| v.0 == x.0 && v.1 == x.1)),
        Some(true)
    );

    // but once we refresh, things will be empty
    w.refresh();
    assert_match!(r.get(&x.0).map(|rs| rs.len()), None);
    assert_match!(
        r.meta_get(&x.0).map(|(rs, m)| (rs.map(|rs| rs.len()), m)),
        Some((None, ()))
    );
}

#[test]
fn mapref() {
    let x = ('x', 42);

    let (r, mut w) = evmap::new();

    // get a read ref to the map
    // scope to ensure it gets dropped and doesn't stall refresh
    {
        assert!(r.read().is_none());
    }

    w.refresh();

    {
        let map = r.read().unwrap();
        // after the first refresh, it is empty, but ready
        assert!(map.is_empty());
        assert!(!map.contains_key(&x.0));
        assert!(map.get(&x.0).is_none());
        // since we're not using `meta`, we get ()
        assert_eq!(map.meta(), &());
    }

    w.insert(x.0, x);

    {
        let map = r.read().unwrap();
        // it is empty even after an add (we haven't refresh yet)
        assert!(map.is_empty());
        assert!(!map.contains_key(&x.0));
        assert!(map.get(&x.0).is_none());
        assert_eq!(map.meta(), &());
    }

    w.refresh();

    {
        let map = r.read().unwrap();

        // but after the swap, the record is there!
        assert!(!map.is_empty());
        assert!(map.contains_key(&x.0));
        assert_eq!(map.get(&x.0).unwrap().len(), 1);
        assert_eq!(map[&x.0].len(), 1);
        assert_eq!(map.meta(), &());
        assert!(map
            .get(&x.0)
            .unwrap()
            .iter()
            .any(|v| v.0 == x.0 && v.1 == x.1));

        // non-existing records return None
        assert!(map.get(&'y').is_none());
        assert_eq!(map.meta(), &());

        // if we purge, the readers still see the values
        w.purge();

        assert!(map
            .get(&x.0)
            .unwrap()
            .iter()
            .any(|v| v.0 == x.0 && v.1 == x.1));
    }

    // but once we refresh, things will be empty
    w.refresh();

    {
        let map = r.read().unwrap();
        assert!(map.is_empty());
        assert!(!map.contains_key(&x.0));
        assert!(map.get(&x.0).is_none());
        assert_eq!(map.meta(), &());
    }

    drop(w);
    {
        let map = r.read();
        assert!(map.is_none(), "the map should have been destroyed");
    }
}

#[test]
#[cfg_attr(miri, ignore)]
// https://github.com/rust-lang/miri/issues/658
fn paniced_reader_doesnt_block_writer() {
    let (r, mut w) = evmap::new();
    w.insert(1, "a");
    w.refresh();

    // reader panics
    let r = std::panic::catch_unwind(move || r.get(&1).map(|_| panic!()));
    assert!(r.is_err());

    // writer should still be able to continue
    w.insert(1, "b");
    w.refresh();
    w.refresh();
}

#[test]
fn flush_noblock() {
    let x = ('x', 42);

    let (r, mut w) = evmap::new();
    w.insert(x.0, x);
    w.refresh();
    assert_eq!(r.get(&x.0).map(|rs| rs.len()), Some(1));

    // pin the epoch
    let _map = r.read();
    // refresh would hang here, but flush won't
    assert!(w.pending().is_empty());
    w.flush();
}

#[test]
fn read_after_drop() {
    let x = ('x', 42);

    let (r, mut w) = evmap::new();
    w.insert(x.0, x);
    w.refresh();
    assert_eq!(r.get(&x.0).map(|rs| rs.len()), Some(1));

    // once we drop the writer, the readers should see empty maps
    drop(w);
    assert_eq!(r.get(&x.0).map(|rs| rs.len()), None);
    assert_eq!(
        r.meta_get(&x.0).map(|(rs, m)| (rs.map(|rs| rs.len()), m)),
        None
    );
}

#[test]
fn clone_types() {
    let x = evmap::shallow_copy::CopyValue::from(b"xyz");

    let (r, mut w) = evmap::new();
    w.insert(&*x, x);
    w.refresh();

    assert_eq!(r.get(&*x).map(|rs| rs.len()), Some(1));
    assert_eq!(
        r.meta_get(&*x).map(|(rs, m)| (rs.map(|rs| rs.len()), m)),
        Some((Some(1), ()))
    );
    assert_eq!(r.get(&*x).map(|rs| rs.iter().any(|v| *v == x)), Some(true));
}

#[test]
#[cfg_attr(miri, ignore)]
fn busybusybusy_fast() {
    busybusybusy_inner(false);
}
#[test]
#[cfg_attr(miri, ignore)]
fn busybusybusy_slow() {
    busybusybusy_inner(true);
}

fn busybusybusy_inner(slow: bool) {
    use std::thread;
    use std::time;

    let threads = 4;
    let mut n = 1000;
    if !slow {
        n *= 100;
    }
    let (r, mut w) = evmap::new();
    w.refresh();

    let rs: Vec<_> = (0..threads)
        .map(|_| {
            let r = r.clone();
            thread::spawn(move || {
                // rustfmt
                for i in 0..n {
                    let i = i.into();
                    loop {
                        let map = r.read().unwrap();
                        let rs = map.get(&i);
                        if rs.is_some() && slow {
                            thread::sleep(time::Duration::from_millis(2));
                        }
                        match rs {
                            Some(rs) => {
                                assert_eq!(rs.len(), 1);
                                assert!(rs.capacity() >= rs.len());
                                assert_eq!(rs.iter().next().unwrap(), &i);
                                break;
                            }
                            None => {
                                thread::yield_now();
                            }
                        }
                    }
                }
            })
        })
        .collect();

    for i in 0..n {
        w.insert(i, i);
        w.refresh();
    }

    for r in rs {
        r.join().unwrap();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn busybusybusy_heap() {
    use std::thread;

    let threads = 2;
    let n = 1000;
    let (r, mut w) = evmap::new::<_, Vec<_>>();
    w.refresh();

    let rs: Vec<_> = (0..threads)
        .map(|_| {
            let r = r.clone();
            thread::spawn(move || {
                for i in 0..n {
                    let i = i.into();
                    loop {
                        let map = r.read().unwrap();
                        let rs = map.get(&i);
                        match rs {
                            Some(rs) => {
                                assert_eq!(rs.len(), 1);
                                assert_eq!(rs.iter().next().unwrap().len(), i);
                                break;
                            }
                            None => {
                                thread::yield_now();
                            }
                        }
                    }
                }
            })
        })
        .collect();

    for i in 0..n {
        w.insert(i, (0..i).collect());
        w.refresh();
    }

    for r in rs {
        r.join().unwrap();
    }
}

#[test]
fn minimal_query() {
    let (r, mut w) = evmap::new();
    w.insert(1, "a");
    w.refresh();
    w.insert(1, "b");

    assert_eq!(r.get(&1).map(|rs| rs.len()), Some(1));
    assert!(r.get(&1).map(|rs| rs.iter().any(|r| r == &"a")).unwrap());
}

#[test]
fn clear_vs_empty() {
    let (r, mut w) = evmap::new::<_, ()>();
    w.refresh();
    assert_eq!(r.get(&1).map(|rs| rs.len()), None);
    w.clear(1);
    w.refresh();
    assert_eq!(r.get(&1).map(|rs| rs.len()), Some(0));
    w.empty(1);
    w.refresh();
    assert_eq!(r.get(&1).map(|rs| rs.len()), None);
    // and again to test both apply_first and apply_second
    w.clear(1);
    w.refresh();
    assert_eq!(r.get(&1).map(|rs| rs.len()), Some(0));
    w.empty(1);
    w.refresh();
    assert_eq!(r.get(&1).map(|rs| rs.len()), None);
}

#[test]
fn non_copy_values() {
    let (r, mut w) = evmap::new();
    w.insert(1, "a".to_string());
    w.refresh();
    w.insert(1, "b".to_string());

    assert_eq!(r.get(&1).map(|rs| rs.len()), Some(1));
    assert!(r.get(&1).map(|rs| rs.iter().any(|r| r == "a")).unwrap());
}

#[test]
fn non_minimal_query() {
    let (r, mut w) = evmap::new();
    w.insert(1, "a");
    w.insert(1, "b");
    w.refresh();
    w.insert(1, "c");

    assert_eq!(r.get(&1).map(|rs| rs.len()), Some(2));
    assert!(r.get(&1).map(|rs| rs.iter().any(|r| r == &"a")).unwrap());
    assert!(r.get(&1).map(|rs| rs.iter().any(|r| r == &"b")).unwrap());
}

#[test]
fn absorb_negative_immediate() {
    let (r, mut w) = evmap::new();
    w.insert(1, "a");
    w.insert(1, "b");
    w.remove(1, "a");
    w.refresh();

    assert_eq!(r.get(&1).map(|rs| rs.len()), Some(1));
    assert!(r.get(&1).map(|rs| rs.iter().any(|r| r == &"b")).unwrap());
}

#[test]
fn absorb_negative_later() {
    let (r, mut w) = evmap::new();
    w.insert(1, "a");
    w.insert(1, "b");
    w.refresh();
    w.remove(1, "a");
    w.refresh();

    assert_eq!(r.get(&1).map(|rs| rs.len()), Some(1));
    assert!(r.get(&1).map(|rs| rs.iter().any(|r| r == &"b")).unwrap());
}

#[test]
fn absorb_multi() {
    let (r, mut w) = evmap::new();
    w.extend(vec![(1, "a"), (1, "b")]);
    w.refresh();

    assert_eq!(r.get(&1).map(|rs| rs.len()), Some(2));
    assert!(r.get(&1).map(|rs| rs.iter().any(|r| r == &"a")).unwrap());
    assert!(r.get(&1).map(|rs| rs.iter().any(|r| r == &"b")).unwrap());

    w.remove(1, "a");
    w.insert(1, "c");
    w.remove(1, "c");
    w.refresh();

    assert_eq!(r.get(&1).map(|rs| rs.len()), Some(1));
    assert!(r.get(&1).map(|rs| rs.iter().any(|r| r == &"b")).unwrap());
}

#[test]
fn empty() {
    let (r, mut w) = evmap::new();
    w.insert(1, "a");
    w.insert(1, "b");
    w.insert(2, "c");
    w.empty(1);
    w.refresh();

    assert_eq!(r.get(&1).map(|rs| rs.len()), None);
    assert_eq!(r.get(&2).map(|rs| rs.len()), Some(1));
    assert!(r.get(&2).map(|rs| rs.iter().any(|r| r == &"c")).unwrap());
}

#[test]
#[cfg(feature = "eviction")]
fn empty_random() {
    let (r, mut w) = evmap::new();
    w.insert(1, "a");
    w.insert(1, "b");
    w.insert(2, "c");

    let mut rng = rand::thread_rng();
    let removed: Vec<_> = w.empty_random(&mut rng, 1).map(|(&k, _)| k).collect();
    w.refresh();

    // should only have one value set left
    assert_eq!(removed.len(), 1);
    let kept = 3 - removed[0];
    // one of them must have gone away
    assert_eq!(r.len(), 1);
    assert!(!r.contains_key(&removed[0]));
    assert!(r.contains_key(&kept));

    // remove the other one
    let removed: Vec<_> = w.empty_random(&mut rng, 100).map(|(&k, _)| k).collect();
    w.refresh();

    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0], kept);
    assert!(r.is_empty());
}

#[test]
#[cfg(feature = "eviction")]
fn empty_random_multiple() {
    let (r, mut w) = evmap::new();
    w.insert(1, "a");
    w.insert(1, "b");
    w.insert(2, "c");

    let mut rng = rand::thread_rng();
    let removed1: Vec<_> = w.empty_random(&mut rng, 1).map(|(&k, _)| k).collect();
    let removed2: Vec<_> = w.empty_random(&mut rng, 1).map(|(&k, _)| k).collect();
    w.refresh();

    assert_eq!(removed1.len(), 1);
    assert_eq!(removed2.len(), 1);
    assert_ne!(removed1[0], removed2[0]);
    assert!(r.is_empty());
}

#[test]
fn empty_post_refresh() {
    let (r, mut w) = evmap::new();
    w.insert(1, "a");
    w.insert(1, "b");
    w.insert(2, "c");
    w.refresh();
    w.empty(1);
    w.refresh();

    assert_eq!(r.get(&1).map(|rs| rs.len()), None);
    assert_eq!(r.get(&2).map(|rs| rs.len()), Some(1));
    assert!(r.get(&2).map(|rs| rs.iter().any(|r| r == &"c")).unwrap());
}

#[test]
fn clear() {
    let (r, mut w) = evmap::new();
    w.insert(1, "a");
    w.insert(1, "b");
    w.insert(2, "c");
    w.clear(1);
    w.refresh();

    assert_eq!(r.get(&1).map(|rs| rs.len()), Some(0));
    assert_eq!(r.get(&2).map(|rs| rs.len()), Some(1));

    w.clear(2);
    w.refresh();

    assert_eq!(r.get(&1).map(|rs| rs.len()), Some(0));
    assert_eq!(r.get(&2).map(|rs| rs.len()), Some(0));

    w.empty(1);
    w.refresh();

    assert_eq!(r.get(&1).map(|rs| rs.len()), None);
    assert_eq!(r.get(&2).map(|rs| rs.len()), Some(0));
}

#[test]
fn replace() {
    let (r, mut w) = evmap::new();
    w.insert(1, "a");
    w.insert(1, "b");
    w.insert(2, "c");
    w.update(1, "x");
    w.refresh();

    assert_eq!(r.get(&1).map(|rs| rs.len()), Some(1));
    assert!(r.get(&1).map(|rs| rs.iter().any(|r| r == &"x")).unwrap());
    assert_eq!(r.get(&2).map(|rs| rs.len()), Some(1));
    assert!(r.get(&2).map(|rs| rs.iter().any(|r| r == &"c")).unwrap());
}

#[test]
fn replace_post_refresh() {
    let (r, mut w) = evmap::new();
    w.insert(1, "a");
    w.insert(1, "b");
    w.insert(2, "c");
    w.refresh();
    w.update(1, "x");
    w.refresh();

    assert_eq!(r.get(&1).map(|rs| rs.len()), Some(1));
    assert!(r.get(&1).map(|rs| rs.iter().any(|r| r == &"x")).unwrap());
    assert_eq!(r.get(&2).map(|rs| rs.len()), Some(1));
    assert!(r.get(&2).map(|rs| rs.iter().any(|r| r == &"c")).unwrap());
}

#[test]
fn with_meta() {
    let (r, mut w) = evmap::with_meta::<usize, usize, _>(42);
    assert_eq!(
        r.meta_get(&1).map(|(rs, m)| (rs.map(|rs| rs.len()), m)),
        None
    );
    w.refresh();
    assert_eq!(
        r.meta_get(&1).map(|(rs, m)| (rs.map(|rs| rs.len()), m)),
        Some((None, 42))
    );
    w.set_meta(43);
    assert_eq!(
        r.meta_get(&1).map(|(rs, m)| (rs.map(|rs| rs.len()), m)),
        Some((None, 42))
    );
    w.refresh();
    assert_eq!(
        r.meta_get(&1).map(|(rs, m)| (rs.map(|rs| rs.len()), m)),
        Some((None, 43))
    );
}

#[test]
fn map_into() {
    let (r, mut w) = evmap::new();
    w.insert(1, "a");
    w.insert(1, "b");
    w.insert(2, "c");
    w.refresh();
    w.insert(1, "x");

    use std::collections::HashMap;
    let copy: HashMap<_, Vec<_>> = r.map_into(|&k, vs| (k, Vec::from_iter(vs.iter().cloned())));

    assert_eq!(copy.len(), 2);
    assert!(copy.contains_key(&1));
    assert!(copy.contains_key(&2));
    assert_eq!(copy[&1].len(), 2);
    assert_eq!(copy[&2].len(), 1);
    assert!(copy[&1].contains(&"a"));
    assert!(copy[&1].contains(&"b"));
    assert!(copy[&2].contains(&"c"));
}

#[test]
#[cfg_attr(miri, ignore)]
fn clone_churn() {
    use std::thread;
    let (r, mut w) = evmap::new();

    thread::spawn(move || loop {
        let r = r.clone();
        if r.get(&1).is_some() {
            thread::yield_now();
        }
    });

    for i in 0..1000 {
        w.insert(1, i);
        w.refresh();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn bigbag() {
    use std::thread;
    let (r, mut w) = evmap::new();
    w.refresh();

    let ndistinct = 32;

    let jh = thread::spawn(move || loop {
        let map = if let Some(map) = r.read() { map } else { break };
        if let Some(rs) = map.get(&1) {
            assert!(rs.len() <= ndistinct * (ndistinct - 1));
            let mut found = true;
            for i in 0..ndistinct {
                if found {
                    if !rs.contains(&[i][..]) {
                        found = false;
                    }
                } else {
                    assert!(!found);
                }
            }
            assert_eq!(rs.into_iter().count(), rs.len());
            drop(map);
            thread::yield_now();
        }
    });

    for _ in 0..64 {
        for i in 1..ndistinct {
            // add some duplicates too
            // total:
            for _ in 0..i {
                w.insert(1, vec![i]);
                w.refresh();
            }
        }
        for i in (1..ndistinct).rev() {
            for _ in 0..i {
                w.remove(1, vec![i]);
                w.fit(1);
                w.refresh();
            }
        }
        w.empty(1);
    }

    drop(w);
    jh.join().unwrap();
}

#[test]
fn foreach() {
    let (r, mut w) = evmap::new();
    w.insert(1, "a");
    w.insert(1, "b");
    w.insert(2, "c");
    w.refresh();
    w.insert(1, "x");

    let r = r.read().unwrap();
    for (k, vs) in &r {
        match k {
            1 => {
                assert_eq!(vs.len(), 2);
                assert!(vs.contains(&"a"));
                assert!(vs.contains(&"b"));
            }
            2 => {
                assert_eq!(vs.len(), 1);
                assert!(vs.contains(&"c"));
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn retain() {
    // do same operations on a plain vector
    // to verify retain implementation
    let mut v = Vec::new();
    let (r, mut w) = evmap::new();

    for i in 0..50 {
        w.insert(0, i);
        v.push(i);
    }

    fn is_even(num: &i32, _: bool) -> bool {
        num % 2 == 0
    }

    unsafe { w.retain(0, is_even) }.refresh();
    v.retain(|i| is_even(i, false));

    let mut vs = r
        .get(&0)
        .map(|nums| Vec::from_iter(nums.iter().cloned()))
        .unwrap();
    vs.sort();
    assert_eq!(v, &*vs);
}

#[test]
fn get_one() {
    let x = ('x', 42);

    let (r, mut w) = evmap::new();

    w.insert(x.0, x);
    w.insert(x.0, x);

    assert_match!(r.get_one(&x.0), None);

    w.refresh();

    assert_match!(r.get_one(&x.0).as_deref(), Some(('x', 42)));
}
