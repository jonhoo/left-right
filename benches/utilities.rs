#![allow(dead_code)]
use left_right::*;
use rand::{distributions::Uniform, Rng};
use std::collections::{BTreeMap, HashMap, VecDeque};

pub(crate) fn random_ops<const RANGE: usize>(len: usize) -> VecDeque<MapOp<RANGE>> {
    let rng = rand::thread_rng();
    let dist = Uniform::new(0, usize::MAX);
    rng.sample_iter(&dist)
        .take(len)
        .map(|x| {
            // 64 keys, low(current) favors heavy compression, higher favors low/no compression
            let key = x & !((!1) << 6);
            // rest value
            let value = x >> 6;
            // One in 1024 is MapOp::Clear, low(current) favors heavy compression, higher favors low/no compression
            if x & !((!1) << 10) == 0 {
                MapOp::Clear
            } else {
                // We are using a Map of Strings to have non-trivial operations.
                MapOp::Set(key, format!("value of {:?} is: {:?}", key, value))
            }
        })
        .collect()
}

pub(crate) enum MapOp<const RANGE: usize> {
    Set(usize, String),
    Clear,
}
impl<const RANGE: usize> Absorb<MapOp<RANGE>> for HashMap<usize, String> {
    fn absorb_first(&mut self, operation: &mut MapOp<RANGE>, _: &Self) {
        match operation {
            MapOp::Set(key, value) => {
                if let Some(loc) = self.get_mut(key) {
                    *loc = value.clone();
                } else {
                    self.insert(*key, value.clone());
                }
            }
            MapOp::Clear => {
                self.clear();
            }
        }
    }

    fn sync_with(&mut self, first: &Self) {
        *self = first.clone();
    }

    const MAX_COMPRESS_RANGE: usize = RANGE;
    fn try_compress(
        mut prev: &mut MapOp<RANGE>,
        next: MapOp<RANGE>,
    ) -> TryCompressResult<MapOp<RANGE>> {
        match (&mut prev, next) {
            (MapOp::Set(prev_key, prev_value), MapOp::Set(key, value)) => {
                if *prev_key == key {
                    *prev_value = value;
                    TryCompressResult::Compressed
                } else {
                    TryCompressResult::Independent(MapOp::Set(key, value))
                }
            }
            (_, MapOp::Clear) => {
                *prev = MapOp::Clear;
                TryCompressResult::Compressed
            }
            (MapOp::Clear, next @ MapOp::Set(_, _)) => TryCompressResult::Dependent(next),
        }
    }
}
impl<const RANGE: usize> Absorb<MapOp<RANGE>> for BTreeMap<usize, String> {
    fn absorb_first(&mut self, operation: &mut MapOp<RANGE>, _: &Self) {
        match operation {
            MapOp::Set(key, value) => {
                if let Some(loc) = self.get_mut(key) {
                    *loc = value.clone();
                } else {
                    self.insert(*key, value.clone());
                }
            }
            MapOp::Clear => {
                self.clear();
            }
        }
    }

    fn sync_with(&mut self, first: &Self) {
        *self = first.clone();
    }

    const MAX_COMPRESS_RANGE: usize = RANGE;
    fn try_compress(
        mut prev: &mut MapOp<RANGE>,
        next: MapOp<RANGE>,
    ) -> TryCompressResult<MapOp<RANGE>> {
        match (&mut prev, next) {
            (MapOp::Set(prev_key, prev_value), MapOp::Set(key, value)) => {
                if *prev_key == key {
                    *prev_value = value;
                    TryCompressResult::Compressed
                } else {
                    TryCompressResult::Independent(MapOp::Set(key, value))
                }
            }
            (_, MapOp::Clear) => {
                *prev = MapOp::Clear;
                TryCompressResult::Compressed
            }
            (MapOp::Clear, next @ MapOp::Set(_, _)) => TryCompressResult::Dependent(next),
        }
    }
}
