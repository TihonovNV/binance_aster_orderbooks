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
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCS.try_with(|count| count.set(count.get() + 1));
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn sample(levels: usize) -> String {
    let side = |best: f64, tick: f64| {
        (0..levels).map(|i| format!(r#"["{:.2}","{:.3}"]"#, best + tick * i as f64, 1.5 + i as f64)).collect::<Vec<_>>().join(",")
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
    let mut depth = Depth::default();
    let scales = Scales { price: 2, quantity: 3 };
    depth.parse(&json, scales).unwrap();
    let before = allocs();
    for _ in 0..1000 {
        depth.parse(&json, scales).unwrap();
    }
    let per_parse = (allocs() - before) as f64 / 1000.0;
    assert_eq!(per_parse, 0.0, "allocations per parse: {per_parse}");
}

#[test]
fn vec_batch_merge_does_not_allocate() {
    use orderbooks::book::{Level, OrderBook, Side, VecBook};
    let mut book = VecBook::default();
    let mut snapshot: Vec<Level> = (0..2000).map(|i| Level { price: 100_000 + i * 2, quantity: 1 }).collect();
    book.apply_batch(Side::Bid, &mut snapshot);
    let mut rng = fastrand::Rng::with_seed(7);
    let mut batches: Vec<Vec<Level>> = (0..200)
        .map(|_| {
            let mut batch: Vec<Level> = (0..130).map(|_| Level { price: 100_000 + rng.i64(0..4000), quantity: rng.i64(0..3) }).collect();
            batch.sort_by_key(|level| level.price);
            batch.dedup_by_key(|level| level.price);
            batch
        })
        .collect();
    let before = allocs();
    for batch in &mut batches {
        book.apply_batch(Side::Bid, batch);
    }
    assert_eq!(allocs() - before, 0);
}
