extern crate evmap;

use evmap::{ReadHandle, WriteHandle};

extern crate quickcheck;
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

use quickcheck::Arbitrary;
use quickcheck::Gen;

use rand::Rng;
use std::cmp::{min, Ord};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash};
use std::ops::Deref;

fn set<'a, T: 'a, I>(iter: I) -> HashSet<T>
where
    I: IntoIterator<Item = &'a T>,
    T: Copy + Hash + Eq,
{
    iter.into_iter().cloned().collect()
}

#[quickcheck]
fn contains(insert: Vec<u32>) -> bool {
    let (r, mut w) = evmap::new();
    for &key in &insert {
        w.insert(key, ());
    }
    w.refresh();

    insert.iter().all(|&key| r.get(&key).is_some())
}

#[quickcheck]
fn contains_not(insert: Vec<u8>, not: Vec<u8>) -> bool {
    let (r, mut w) = evmap::new();
    for &key in &insert {
        w.insert(key, ());
    }
    w.refresh();

    let nots = &set(&not) - &set(&insert);
    nots.iter().all(|&key| r.get(&key).is_none())
}

#[quickcheck]
fn insert_empty(insert: Vec<u8>, remove: Vec<u8>) -> bool {
    let (r, mut w) = evmap::new();
    for &key in &insert {
        w.insert(key, ());
    }
    w.refresh();
    for &key in &remove {
        w.empty(key);
    }
    w.refresh();
    let elements = &set(&insert) - &set(&remove);
    r.len() == elements.len()
        && r.map_into::<_, Vec<()>, _>(|_, _| ()).len() == elements.len()
        && elements.iter().all(|k| r.get(k).is_some())
}

use Op::*;
#[derive(Copy, Clone, Debug)]
enum Op<K, V> {
    Add(K, V),
    Remove(K),
    RemoveValue(K, V),
    Refresh,
}

impl<K, V> Arbitrary for Op<K, V>
where
    K: Arbitrary,
    V: Arbitrary,
{
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        match g.gen::<u32>() % 4 {
            0 => Add(K::arbitrary(g), V::arbitrary(g)),
            1 => Remove(K::arbitrary(g)),
            2 => RemoveValue(K::arbitrary(g), V::arbitrary(g)),
            _ => Refresh,
        }
    }
}

fn do_ops<K, V, S>(
    ops: &[Op<K, V>],
    evmap: &mut WriteHandle<K, V, (), S>,
    write_ref: &mut HashMap<K, Vec<V>>,
    read_ref: &mut HashMap<K, Vec<V>>,
) where
    K: Hash + Eq + Clone,
    V: Clone + evmap::ShallowCopy + Eq + Hash,
    S: BuildHasher + Clone,
{
    for op in ops {
        match *op {
            Add(ref k, ref v) => {
                evmap.insert(k.clone(), v.clone());
                write_ref
                    .entry(k.clone())
                    .or_insert_with(Vec::new)
                    .push(v.clone());
            }
            Remove(ref k) => {
                evmap.empty(k.clone());
                write_ref.remove(k);
            }
            RemoveValue(ref k, ref v) => {
                evmap.remove(k.clone(), v.clone());
                write_ref.get_mut(k).and_then(|values| {
                    values
                        .iter_mut()
                        .position(|value| value == v)
                        .and_then(|pos| Some(values.remove(pos)))
                });
            }
            Refresh => {
                evmap.refresh();
                *read_ref = write_ref.clone();
            }
        }
    }
}

fn assert_maps_equivalent<K, V, S>(a: &ReadHandle<K, V, (), S>, b: &HashMap<K, Vec<V>>) -> bool
where
    K: Clone + Hash + Eq + Debug,
    V: Hash + Eq + Debug + Ord + Copy,
    S: BuildHasher,
{
    assert_eq!(a.len(), b.len());
    let keys = a.map_into::<_, Vec<K>, _>(|k, _| k.clone());
    for key in &keys {
        assert!(b.contains_key(key), "b does not contain {:?}", key);
    }
    for key in b.keys() {
        assert!(a.get(key).is_some(), "a does not contain {:?}", key);
    }
    for key in &keys {
        let mut ev_map_values: Vec<V> = a.get(key).unwrap().iter().copied().collect();
        ev_map_values.sort();
        let mut map_values = b[key].clone();
        map_values.sort();
        assert_eq!(ev_map_values, map_values);
    }
    true
}

/// quickcheck Arbitrary adaptor -- make a larger vec
#[derive(Clone, Debug)]
struct Large<T>(T);

impl<T> Deref for Large<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> Arbitrary for Large<Vec<T>>
where
    T: Arbitrary,
{
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        let len = g.next_u32() % (g.size() * 10) as u32;
        Large((0..len).map(|_| T::arbitrary(g)).collect())
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new((**self).shrink().map(Large))
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Alphabet(String);

impl Deref for Alphabet {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}

const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz";

impl Arbitrary for Alphabet {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        let len = g.next_u32() % g.size() as u32;
        let len = min(len, 16);
        Alphabet(
            (0..len)
                .map(|_| ALPHABET[g.next_u32() as usize % ALPHABET.len()] as char)
                .collect(),
        )
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new((**self).shrink().map(Alphabet))
    }
}

#[quickcheck]
fn operations_i8(ops: Large<Vec<Op<i8, i8>>>) -> bool {
    let (r, mut w) = evmap::new();
    let mut write_ref = HashMap::new();
    let mut read_ref = HashMap::new();
    do_ops(&ops, &mut w, &mut write_ref, &mut read_ref);
    assert_maps_equivalent(&r, &read_ref);

    w.refresh();
    assert_maps_equivalent(&r, &write_ref)
}

#[quickcheck]
fn operations_string(ops: Vec<Op<Alphabet, i8>>) -> bool {
    let (r, mut w) = evmap::new();
    let mut write_ref = HashMap::new();
    let mut read_ref = HashMap::new();
    do_ops(&ops, &mut w, &mut write_ref, &mut read_ref);
    assert_maps_equivalent(&r, &read_ref);

    w.refresh();
    assert_maps_equivalent(&r, &write_ref)
}
