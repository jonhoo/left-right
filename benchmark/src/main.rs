extern crate evmap;
extern crate chashmap;
extern crate zipf;
#[macro_use]
extern crate clap;
extern crate rand;

use chashmap::CHashMap;
use std::collections::HashMap;
use clap::{Arg, App};

use std::time;
use std::sync;
use std::thread;

fn main() {
    let matches = App::new("Concurrent HashMap Benchmarker")
        .version(crate_version!())
        .author("Jon Gjengset <jon@thesquareplanet.com>")
        .about("Benchmark multiple implementations of concurrent HashMaps with varying read/write load")
        .arg(Arg::with_name("readers")
            .short("r")
            .long("readers")
            .help("Set the number of readers")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("writers")
            .short("w")
            .long("writers")
            .required(true)
            .help("Set the number of writers")
            .takes_value(true))
        .arg(Arg::with_name("distribution")
            .short("d")
            .long("dist")
            .possible_values(&["uniform", "skewed"])
            .default_value("uniform")
            .help("Set the distribution for reads and writes")
            .takes_value(true))
        .get_matches();

    let readers = value_t!(matches, "readers", usize).unwrap_or_else(|e| e.exit());
    let writers = value_t!(matches, "writers", usize).unwrap_or_else(|e| e.exit());
    let dist = matches.value_of("distribution").unwrap_or("uniform");
    let dur = time::Duration::from_secs(1);
    let dur_in_ns = dur.as_secs() * 1_000_000_000_u64 + dur.subsec_nanos() as u64;
    let dur_in_s = dur_in_ns as f64 / 1_000_000_000_f64;
    let span = 10000;

    let stat = |var, op, results: Vec<(_, usize)>| for (i, res) in results.into_iter()
        .enumerate() {
        println!("{} {} {} {:10} {:8.0} ops/s {} {}",
                 readers,
                 writers,
                 dist,
                 var,
                 res.1 as f64 / dur_in_s as f64,
                 op,
                 i)
    };

    let mut join = Vec::with_capacity(readers + writers);
    // first, benchmark Arc<RwLock<HashMap>>
    {
        let start = time::Instant::now();
        let end = start + dur;
        let map: sync::Arc<sync::RwLock<HashMap<u64, u64>>> = sync::Arc::default();
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
        let (wres, rres): (Vec<_>, _) =
            join.drain(..).map(|jh| jh.join().unwrap()).partition(|&(write, _)| write);
        stat("std", "write", wres);
        stat("std", "read", rres);
    }

    // then, benchmark Arc<CHashMap>
    {
        let start = time::Instant::now();
        let end = start + dur;
        let map: sync::Arc<CHashMap<u64, u64>> = sync::Arc::default();
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
        let (wres, rres): (Vec<_>, _) =
            join.drain(..).map(|jh| jh.join().unwrap()).partition(|&(write, _)| write);
        stat("chashmap", "write", wres);
        stat("chashmap", "read", rres);
    }

    // finally, benchmark evmap
    {
        let start = time::Instant::now();
        let end = start + dur;
        let (r, w) = evmap::new();
        let w = sync::Arc::new(sync::Mutex::new(w));
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
        let (wres, rres): (Vec<_>, _) =
            join.drain(..).map(|jh| jh.join().unwrap()).partition(|&(write, _)| write);
        stat("evmap", "write", wres);
        stat("evmap", "read", rres);
    }
}

trait Backend {
    fn b_get(&self, key: u64) -> u64;
    fn b_put(&mut self, key: u64, value: u64);
}

fn drive<B: Backend>(mut backend: B,
                     end: time::Instant,
                     dist: String,
                     write: bool,
                     span: usize)
                     -> (bool, usize) {
    use rand::Rng;

    let mut ops = 0;
    let skewed = dist == "skewed";
    let mut t_rng = rand::thread_rng();
    let mut zipf = zipf::ZipfDistribution::new(rand::thread_rng(), span, 1.03).unwrap();
    while time::Instant::now() < end {
        // generate both so that overhead is always the same
        let id_uniform: u64 = t_rng.gen_range(0, span as u64);
        let id_skewed = zipf.next_u64();
        let id = if skewed { id_skewed } else { id_uniform };
        if write {
            backend.b_put(id, t_rng.next_u64());
        } else {
            backend.b_get(id);
        }
        ops += 1;
    }

    (write, ops)
}

impl Backend for sync::Arc<CHashMap<u64, u64>> {
    fn b_get(&self, key: u64) -> u64 {
        self.get(&key).map(|v| *v).unwrap_or(0)
    }

    fn b_put(&mut self, key: u64, value: u64) {
        self.insert(key, value);
    }
}

impl Backend for sync::Arc<sync::RwLock<HashMap<u64, u64>>> {
    fn b_get(&self, key: u64) -> u64 {
        self.read().unwrap().get(&key).map(|&v| v).unwrap_or(0)
    }

    fn b_put(&mut self, key: u64, value: u64) {
        self.write().unwrap().insert(key, value);
    }
}

enum EvHandle {
    Read(evmap::ReadHandle<u64, u64>),
    Write(sync::Arc<sync::Mutex<evmap::WriteHandle<u64, u64>>>),
}

impl Backend for EvHandle {
    fn b_get(&self, key: u64) -> u64 {
        if let EvHandle::Read(ref r) = *self {
            r.get_and(&key, |v| v[0]).unwrap_or(0)
        } else {
            unreachable!();
        }
    }

    fn b_put(&mut self, key: u64, value: u64) {
        if let EvHandle::Write(ref w) = *self {
            let mut w = w.lock().unwrap();
            w.update(key, value);
            w.refresh();
        } else {
            unreachable!();
        }
    }
}
