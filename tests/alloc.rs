use orderbooks::feed::wire::{Depth, Scales};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct Counting;

thread_local! {
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

fn allocs() -> usize {
    ALLOCS.with(Cell::get)
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
}

#[global_allocator]
static A: Counting = Counting;

fn sample(levels: usize) -> String {
    let side = |base: f64, step: f64| {
        (0..levels).map(|i| format!(r#"["{:.2}","{:.3}"]"#, base + step * i as f64, 1.5 + i as f64)).collect::<Vec<_>>().join(",")
    };
    format!(
        r#"{{"stream":"ethusdt@depth@100ms","data":{{"e":"depthUpdate","E":1792431094434,"T":1792431094400,"s":"ETHUSDT","U":11863880631016,"u":11863880640008,"pu":11863880630988,"b":[{}],"a":[{}]}}}}"#,
        side(2401.67, -0.01),
        side(2401.68, 0.01)
    )
}

#[test]
fn wire_parse_does_not_allocate() {
    let json = sample(100);
    let mut d = Depth::default();
    let sc = Scales { px: 2, qty: 3 };
    d.parse(&json, sc).unwrap();
    let before = allocs();
    for _ in 0..1000 {
        d.parse(&json, sc).unwrap();
    }
    let per_parse = (allocs() - before) as f64 / 1000.0;
    assert_eq!(per_parse, 0.0, "allocations per parse: {per_parse}");
}

#[test]
fn vec_batch_merge_does_not_allocate() {
    use orderbooks::book::{Level, OrderBook, Side, VecBook};
    let mut book = VecBook::default();
    let mut snap: Vec<Level> = (0..2000).map(|i| Level { px: 100_000 + i * 2, qty: 1 }).collect();
    book.apply_batch(Side::Bid, &mut snap);
    let mut rng = fastrand::Rng::with_seed(7);
    let mut batches: Vec<Vec<Level>> = (0..200)
        .map(|_| {
            let mut b: Vec<Level> = (0..130).map(|_| Level { px: 100_000 + rng.i64(0..4000), qty: rng.i64(0..3) }).collect();
            b.sort_by_key(|l| l.px);
            b.dedup_by_key(|l| l.px);
            b
        })
        .collect();
    let before = allocs();
    for b in &mut batches {
        book.apply_batch(Side::Bid, b);
    }
    assert_eq!(allocs() - before, 0);
}
