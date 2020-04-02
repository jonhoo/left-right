use evmap_derive::ShallowCopy;
use evmap::shallow_copy::ShallowCopy as _;

#[derive(ShallowCopy)]
struct Broken {
	f1: i32,
	f2: std::cell::Cell<()>,
}

#[derive(ShallowCopy)]
struct AlsoBroken(i32, std::cell::Cell<()>);

#[derive(ShallowCopy)]
struct Generic<T> {
	f1: T
}

fn main() {
	let broken = Broken { f1: 0, f2: std::cell::Cell::new(()) };
	broken.shallow_copy();
	let also_broken = AlsoBroken(0, std::cell::Cell::new(()));
	broken.shallow_copy();
	let generic = Generic { f1: std::cell::Cell::new(()) };
	generic.shallow_copy();
}
