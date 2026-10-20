use orderbooks::book::{BTreeBook, Level, OrderBook, Side, VecBook};
use orderbooks::config::Venue;
use orderbooks::feed::wire::Depth;
use orderbooks::fixture::{self, Msg, Recording, SCALES};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::path::Path;

fn apply<B: OrderBook>(b: &mut B, m: &Msg) {
    m.bids.iter().for_each(|l| b.apply(Side::Bid, l.px, l.qty));
    m.asks.iter().for_each(|l| b.apply(Side::Ask, l.px, l.qty));
}

/// What the engine does: copy each side into a reused buffer, then one merge per side.
fn apply_merge<B: OrderBook>(b: &mut B, m: &Msg, buf: &mut Vec<Level>) {
    for (side, lv) in [(Side::Bid, &m.bids), (Side::Ask, &m.asks)] {
        buf.clear();
        buf.extend_from_slice(lv);
        b.apply_batch(side, buf);
    }
}

fn seeded<B: OrderBook>(r: &Recording) -> B {
    let mut b = B::default();
    apply(&mut b, &r.snapshot);
    b
}

/// Where updates land, in levels from the best price, plus whether diff levels arrive sorted.
fn profile(v: Venue, r: &Recording) {
    let mut sides: [BTreeMap<i64, i64>; 2] = Default::default();
    fn put(m: &mut BTreeMap<i64, i64>, l: &Level) {
        if l.qty == 0 {
            m.remove(&l.px)
        } else {
            m.insert(l.px, l.qty)
        };
    }
    r.snapshot.bids.iter().for_each(|l| put(&mut sides[0], l));
    r.snapshot.asks.iter().for_each(|l| put(&mut sides[1], l));
    let edges = [0usize, 1, 5, 20, 100, 500, usize::MAX];
    let mut hist = [0u64; 6];
    let (mut sorted, mut total) = (0, 0);
    for m in &r.diffs {
        for (s, lv) in [(0, &m.bids), (1, &m.asks)] {
            total += 1;
            sorted += lv.windows(2).all(|w| w[0].px < w[1].px) as u32;
            for l in lv.iter() {
                let depth = if s == 0 { sides[0].range(l.px + 1..).count() } else { sides[1].range(..l.px).count() };
                hist[edges.windows(2).position(|e| depth >= e[0] && depth < e[1]).unwrap()] += 1;
                put(&mut sides[s], l);
            }
        }
    }
    let n: u64 = hist.iter().sum();
    let pct = |i: usize| 100.0 * hist[i] as f64 / n as f64;
    eprintln!(
        "{:>8}: {} updates in {} diffs | depth from best: 0: {:.1}%  1-4: {:.1}%  5-19: {:.1}%  20-99: {:.1}%  100-499: {:.1}%  500+: {:.1}% | sides sorted ascending: {sorted}/{total}",
        v.name(),
        n,
        r.diffs.len(),
        pct(0),
        pct(1),
        pct(2),
        pct(3),
        pct(4),
        pct(5)
    );
}

fn benches(c: &mut Criterion) {
    for v in Venue::ALL {
        let r = fixture::load(Path::new("fixtures"), v).expect("fixtures: run `orderbooks record` first");
        profile(v, &r);
        let updates: usize = r.diffs.iter().map(|m| m.bids.len() + m.asks.len()).sum();

        let mut g = c.benchmark_group(format!("{}/parse", v.name()));
        g.throughput(Throughput::Elements(r.frames.len() as u64));
        let mut d = Depth::default();
        g.bench_function("frames", |b| b.iter(|| r.frames.iter().for_each(|f| d.parse(black_box(f), SCALES).unwrap())));
        g.finish();

        let mut g = c.benchmark_group(format!("{}/apply", v.name()));
        g.throughput(Throughput::Elements(updates as u64));
        g.bench_function("btree", |b| {
            b.iter_batched_ref(|| seeded::<BTreeBook>(&r), |bk| r.diffs.iter().for_each(|m| apply(bk, m)), BatchSize::LargeInput)
        });
        g.bench_function("vec", |b| {
            b.iter_batched_ref(|| seeded::<VecBook>(&r), |bk| r.diffs.iter().for_each(|m| apply(bk, m)), BatchSize::LargeInput)
        });
        let mut buf = Vec::with_capacity(4096);
        g.bench_function("vec-merge", |b| {
            b.iter_batched_ref(
                || seeded::<VecBook>(&r),
                |bk| r.diffs.iter().for_each(|m| apply_merge(bk, m, &mut buf)),
                BatchSize::LargeInput,
            )
        });
        g.finish();

        let mut g = c.benchmark_group(format!("{}/apply+top5", v.name()));
        g.throughput(Throughput::Elements(r.diffs.len() as u64));
        fn step<B: OrderBook>(bk: &mut B, r: &Recording, merge: bool, buf: &mut Vec<Level>) {
            let mut out = [Level::default(); 5];
            for m in &r.diffs {
                if merge {
                    apply_merge(bk, m, buf)
                } else {
                    apply(bk, m)
                }
                black_box(bk.top_n(Side::Bid, &mut out) + bk.top_n(Side::Ask, &mut out));
            }
        }
        let mut buf = Vec::with_capacity(4096);
        g.bench_function("btree", |b| {
            b.iter_batched_ref(|| seeded::<BTreeBook>(&r), |bk| step(bk, &r, false, &mut buf), BatchSize::LargeInput)
        });
        g.bench_function("vec", |b| {
            b.iter_batched_ref(|| seeded::<VecBook>(&r), |bk| step(bk, &r, false, &mut buf), BatchSize::LargeInput)
        });
        g.bench_function("vec-merge", |b| {
            b.iter_batched_ref(|| seeded::<VecBook>(&r), |bk| step(bk, &r, true, &mut buf), BatchSize::LargeInput)
        });
        g.finish();

        let mut g = c.benchmark_group(format!("{}/top20", v.name()));
        let (bt, vb) = (seeded::<BTreeBook>(&r), seeded::<VecBook>(&r));
        let mut out = [Level::default(); 20];
        g.bench_function("btree", |b| b.iter(|| black_box(bt.top_n(Side::Bid, &mut out) + bt.top_n(Side::Ask, &mut out))));
        g.bench_function("vec", |b| b.iter(|| black_box(vb.top_n(Side::Bid, &mut out) + vb.top_n(Side::Ask, &mut out))));
        g.finish();
    }
}

criterion_group!(name = book; config = Criterion::default().sample_size(30); targets = benches);
criterion_main!(book);
