use chashmap::CHashMap;
use clap::{crate_version, value_t};
use clap::{App, Arg};
use rand::prelude::*;
use std::collections::HashMap;
use std::sync;
use std::thread;
use std::time;

fn main() {
    let matches = App::new("Concurrent HashMap Benchmarker")
        .version(crate_version!())
        .author("Jon Gjengset <jon@thesquareplanet.com>")
        .about(
            "Benchmark multiple implementations of concurrent HashMaps with varying read/write load",
        )
        .arg(
            Arg::with_name("readers")
                .short("r")
                .long("readers")
                .help("Set the number of readers")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("compare")
                .short("c")
                .help("Also benchmark Arc<RwLock<HashMap>> and CHashMap"),
        )
        .arg(
            Arg::with_name("eventual")
                .short("e")
                .takes_value(true)
                .default_value("1")
                .value_name("N")
                .help("Refresh evmap every N writes"),
        )
        .arg(
            Arg::with_name("writers")
                .short("w")
                .long("writers")
                .required(true)
                .help("Set the number of writers")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("distribution")
                .short("d")
                .long("dist")
                .possible_values(&["uniform", "skewed"])
                .default_value("uniform")
                .help("Set the distribution for reads and writes")
                .takes_value(true),
        )
        .get_matches();

    let refresh = value_t!(matches, "eventual", usize).unwrap_or_else(|e| e.exit());
    let readers = value_t!(matches, "readers", usize).unwrap_or_else(|e| e.exit());
    let writers = value_t!(matches, "writers", usize).unwrap_or_else(|e| e.exit());
    let dist = matches.value_of("distribution").unwrap_or("uniform");
    let dur = time::Duration::from_secs(5);
    let dur_in_ns = dur.as_secs() * 1_000_000_000_u64 + dur.subsec_nanos() as u64;
    let dur_in_s = dur_in_ns as f64 / 1_000_000_000_f64;
    let span = 10000;

    let stat = |var: &str, op, results: Vec<(_, usize)>| {
        for (i, res) in results.into_iter().enumerate() {
            println!(
                "{:2} {:2} {:10} {:10} {:8.0} ops/s {} {}",
                readers,
                writers,
                dist,
                var,
                res.1 as f64 / dur_in_s as f64,
                op,
                i
            )
        }
    };

    let mut join = Vec::with_capacity(readers + writers);
    // first, benchmark Arc<RwLock<HashMap>>
    if matches.is_present("compare") {
        let map: HashMap<usize, usize> = HashMap::with_capacity(5_000_000);
        let map = sync::Arc::new(parking_lot::RwLock::new(map));
        let start = time::Instant::now();
        let end = start + dur;
        join.extend((0..readers).into_iter().map(|_| {
            let map = map.clone();
            let dist = dist.to_owned();
            thread::spawn(move || drive(map, end, dist, false, span))
        }));
        join.extend((0..writers).into_iter().map(|_| {
            let map = map.clone();
            let dist = dist.to_owned();
            thread::spawn(move || drive(map, end, dist, true, span))
        }));
        let (wres, rres): (Vec<_>, _) = join
            .drain(..)
            .map(|jh| jh.join().unwrap())
            .partition(|&(write, _)| write);
        stat("std", "write", wres);
        stat("std", "read", rres);
    }

    // then, benchmark Arc<CHashMap>
    if matches.is_present("compare") {
        let map: CHashMap<usize, usize> = CHashMap::with_capacity(5_000_000);
        let map = sync::Arc::new(map);
        let start = time::Instant::now();
        let end = start + dur;
        join.extend((0..readers).into_iter().map(|_| {
            let map = map.clone();
            let dist = dist.to_owned();
            thread::spawn(move || drive(map, end, dist, false, span))
        }));
        join.extend((0..writers).into_iter().map(|_| {
            let map = map.clone();
            let dist = dist.to_owned();
            thread::spawn(move || drive(map, end, dist, true, span))
        }));
        let (wres, rres): (Vec<_>, _) = join
            .drain(..)
            .map(|jh| jh.join().unwrap())
            .partition(|&(write, _)| write);
        stat("chashmap", "write", wres);
        stat("chashmap", "read", rres);
    }

    // finally, benchmark evmap
    {
        let (r, w) = evmap::Options::default()
            .with_capacity(5_000_000)
            .construct();
        let w = sync::Arc::new(parking_lot::Mutex::new((w, 0, refresh)));
        let start = time::Instant::now();
        let end = start + dur;
        join.extend((0..readers).into_iter().map(|_| {
            let map = EvHandle::Read(r.clone());
            let dist = dist.to_owned();
            thread::spawn(move || drive(map, end, dist, false, span))
        }));
        join.extend((0..writers).into_iter().map(|_| {
            let map = EvHandle::Write(w.clone());
            let dist = dist.to_owned();
            thread::spawn(move || drive(map, end, dist, true, span))
        }));
        let (wres, rres): (Vec<_>, _) = join
            .drain(..)
            .map(|jh| jh.join().unwrap())
            .partition(|&(write, _)| write);

        let n = if refresh == 1 {
            "evmap".to_owned()
        } else {
            format!("evmap-refresh{}", refresh)
        };
        stat(&n, "write", wres);
        stat(&n, "read", rres);
    }
}

trait Backend {
    fn b_get(&self, key: usize) -> usize;
    fn b_put(&mut self, key: usize, value: usize);
}

fn drive<B: Backend>(
    mut backend: B,
    end: time::Instant,
    dist: String,
    write: bool,
    span: usize,
) -> (bool, usize) {
    let mut ops = 0;
    let skewed = dist == "skewed";
    let mut t_rng = rand::thread_rng();
    let zipf = zipf::ZipfDistribution::new(span, 1.03).unwrap();
    while time::Instant::now() < end {
        // generate both so that overhead is always the same
        let id_uniform: usize = t_rng.gen_range(0, span as usize);
        let id_skewed: usize = zipf.sample(&mut t_rng);
        let id = if skewed { id_skewed } else { id_uniform };
        if write {
            backend.b_put(id, t_rng.gen());
        } else {
            backend.b_get(id);
        }
        ops += 1;
    }

    (write, ops)
}

impl Backend for sync::Arc<CHashMap<usize, usize>> {
    fn b_get(&self, key: usize) -> usize {
        self.get(&key).map(|v| *v).unwrap_or(0)
    }

    fn b_put(&mut self, key: usize, value: usize) {
        self.insert(key, value);
    }
}

impl Backend for sync::Arc<parking_lot::RwLock<HashMap<usize, usize>>> {
    fn b_get(&self, key: usize) -> usize {
        self.read().get(&key).map(|&v| v).unwrap_or(0)
    }

    fn b_put(&mut self, key: usize, value: usize) {
        self.write().insert(key, value);
    }
}

enum EvHandle {
    Read(evmap::ReadHandle<usize, usize>),
    Write(sync::Arc<parking_lot::Mutex<(evmap::WriteHandle<usize, usize>, usize, usize)>>),
}

impl Backend for EvHandle {
    fn b_get(&self, key: usize) -> usize {
        if let EvHandle::Read(ref r) = *self {
            r.get(&key)
                .map(|v| v.iter().next().cloned().unwrap())
                .unwrap_or(0)
        } else {
            unreachable!();
        }
    }

    fn b_put(&mut self, key: usize, value: usize) {
        if let EvHandle::Write(ref w) = *self {
            let mut w = w.lock();
            w.0.update(key, value);
            w.1 += 1;
            if w.1 == w.2 {
                w.1 = 0;
                w.0.refresh();
            }
        } else {
            unreachable!();
        }
    }
}
