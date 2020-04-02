use evmap::shallow_copy::ShallowCopy as _;
use evmap_derive::ShallowCopy;
use std::sync::Arc;

#[derive(ShallowCopy)]
enum Message {
    Quit,
    ChangeColor(i32, i32, i32),
    Move { x: i32, y: i32 },
    Write(String),
}

#[derive(ShallowCopy)]
struct Shallow;

#[derive(ShallowCopy)]
struct Tuple(i32, String, Shallow);

#[derive(ShallowCopy)]
struct Test {
    f1: i32,
    f2: (i32, i32),
    f3: String,
    f4: Arc<String>,
    f5: Shallow,
    f6: evmap::shallow_copy::CopyValue<i32>,
    f7: Tuple,
}

#[derive(ShallowCopy)]
struct Generic<T> {
    f1: T,
}

#[test]
fn test() {
    unsafe {
        let message = Message::Quit;
        message.shallow_copy();
        let shallow = Shallow;
        shallow.shallow_copy();
        let tuple = Tuple(0, "test".to_owned(), Shallow);
        tuple.shallow_copy();
        let test = Test {
            f1: 0,
            f2: (1, 2),
            f3: "test".to_owned(),
            f4: Arc::new("test".to_owned()),
            f5: shallow,
            f6: 3.into(),
            f7: tuple,
        };
        test.shallow_copy();
        let generic = Generic {
            f1: "test".to_owned(),
        };
        generic.shallow_copy();
    }
}

#[test]
fn failing() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/failing/lib.rs");
}
