use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use orderbooks::book::{BTreeBook, Level, OrderBook, Side, VecBook};
use orderbooks::config::Venue;
use orderbooks::feed::wire::Depth;
use orderbooks::fixture::{self, RecordedMessage, Recording, SCALES};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::path::Path;

fn apply<B: OrderBook>(book: &mut B, message: &RecordedMessage) {
    message.bids.iter().for_each(|level| book.apply(Side::Bid, level.price, level.quantity));
    message.asks.iter().for_each(|level| book.apply(Side::Ask, level.price, level.quantity));
}

/// What the engine does: copy each side into a reused buffer, then one merge per side.
fn apply_merge<B: OrderBook>(book: &mut B, message: &RecordedMessage, buffer: &mut Vec<Level>) {
    for (side, levels) in [(Side::Bid, &message.bids), (Side::Ask, &message.asks)] {
        buffer.clear();
        buffer.extend_from_slice(levels);
        book.apply_batch(side, buffer);
    }
}

fn seeded<B: OrderBook>(recording: &Recording) -> B {
    let mut book = B::default();
    apply(&mut book, &recording.snapshot);
    book
}

/// Where updates land, in levels from the best price, plus whether diff levels arrive sorted.
fn profile(venue: Venue, recording: &Recording) {
    let mut sides: [BTreeMap<i64, i64>; 2] = Default::default();
    fn put(levels: &mut BTreeMap<i64, i64>, level: &Level) {
        if level.quantity == 0 {
            levels.remove(&level.price)
        } else {
            levels.insert(level.price, level.quantity)
        };
    }
    recording.snapshot.bids.iter().for_each(|level| put(&mut sides[0], level));
    recording.snapshot.asks.iter().for_each(|level| put(&mut sides[1], level));
    let bucket_edges = [0usize, 1, 5, 20, 100, 500, usize::MAX];
    let mut bucket_counts = [0u64; 6];
    let (mut sorted_sides, mut total_sides) = (0, 0);
    for diff in &recording.diffs {
        for (side_index, levels) in [(0, &diff.bids), (1, &diff.asks)] {
            total_sides += 1;
            sorted_sides += levels.windows(2).all(|pair| pair[0].price < pair[1].price) as u32;
            for level in levels.iter() {
                let levels_ahead =
                    if side_index == 0 { sides[0].range(level.price + 1..).count() } else { sides[1].range(..level.price).count() };
                let bucket = bucket_edges.windows(2).position(|edge| levels_ahead >= edge[0] && levels_ahead < edge[1]).unwrap();
                bucket_counts[bucket] += 1;
                put(&mut sides[side_index], level);
            }
        }
    }
    let updates: u64 = bucket_counts.iter().sum();
    let percent = |bucket: usize| 100.0 * bucket_counts[bucket] as f64 / updates as f64;
    eprintln!(
        "{:>8}: {} updates in {} diffs | depth from best: 0: {:.1}%  1-4: {:.1}%  5-19: {:.1}%  20-99: {:.1}%  100-499: {:.1}%  500+: {:.1}% | sides sorted ascending: {sorted_sides}/{total_sides}",
        venue.name(),
        updates,
        recording.diffs.len(),
        percent(0),
        percent(1),
        percent(2),
        percent(3),
        percent(4),
        percent(5)
    );
}

fn benches(c: &mut Criterion) {
    for venue in Venue::ALL {
        let recording = fixture::load(Path::new("fixtures"), venue).expect("fixtures: run `orderbooks record` first");
        profile(venue, &recording);
        let updates: usize = recording.diffs.iter().map(|diff| diff.bids.len() + diff.asks.len()).sum();

        let mut group = c.benchmark_group(format!("{}/parse", venue.name()));
        group.throughput(Throughput::Elements(recording.frames.len() as u64));
        let mut depth = Depth::default();
        group.bench_function("frames", |b| {
            b.iter(|| recording.frames.iter().for_each(|frame| depth.parse(black_box(frame), SCALES).unwrap()))
        });
        group.finish();

        let mut group = c.benchmark_group(format!("{}/apply", venue.name()));
        group.throughput(Throughput::Elements(updates as u64));
        group.bench_function("btree", |b| {
            b.iter_batched_ref(
                || seeded::<BTreeBook>(&recording),
                |book| recording.diffs.iter().for_each(|diff| apply(book, diff)),
                BatchSize::LargeInput,
            )
        });
        group.bench_function("vec", |b| {
            b.iter_batched_ref(
                || seeded::<VecBook>(&recording),
                |book| recording.diffs.iter().for_each(|diff| apply(book, diff)),
                BatchSize::LargeInput,
            )
        });
        let mut buffer = Vec::with_capacity(4096);
        group.bench_function("vec-merge", |b| {
            b.iter_batched_ref(
                || seeded::<VecBook>(&recording),
                |book| recording.diffs.iter().for_each(|diff| apply_merge(book, diff, &mut buffer)),
                BatchSize::LargeInput,
            )
        });
        group.finish();

        let mut group = c.benchmark_group(format!("{}/apply+top5", venue.name()));
        group.throughput(Throughput::Elements(recording.diffs.len() as u64));
        fn apply_and_read_top<B: OrderBook>(book: &mut B, recording: &Recording, merge: bool, buffer: &mut Vec<Level>) {
            let mut top = [Level::default(); 5];
            for diff in &recording.diffs {
                if merge {
                    apply_merge(book, diff, buffer)
                } else {
                    apply(book, diff)
                }
                black_box(book.top_n(Side::Bid, &mut top) + book.top_n(Side::Ask, &mut top));
            }
        }
        let mut buffer = Vec::with_capacity(4096);
        group.bench_function("btree", |b| {
            b.iter_batched_ref(
                || seeded::<BTreeBook>(&recording),
                |book| apply_and_read_top(book, &recording, false, &mut buffer),
                BatchSize::LargeInput,
            )
        });
        group.bench_function("vec", |b| {
            b.iter_batched_ref(
                || seeded::<VecBook>(&recording),
                |book| apply_and_read_top(book, &recording, false, &mut buffer),
                BatchSize::LargeInput,
            )
        });
        group.bench_function("vec-merge", |b| {
            b.iter_batched_ref(
                || seeded::<VecBook>(&recording),
                |book| apply_and_read_top(book, &recording, true, &mut buffer),
                BatchSize::LargeInput,
            )
        });
        group.finish();

        let mut group = c.benchmark_group(format!("{}/top20", venue.name()));
        let (btree, vec) = (seeded::<BTreeBook>(&recording), seeded::<VecBook>(&recording));
        let mut top = [Level::default(); 20];
        group.bench_function("btree", |b| b.iter(|| black_box(btree.top_n(Side::Bid, &mut top) + btree.top_n(Side::Ask, &mut top))));
        group.bench_function("vec", |b| b.iter(|| black_box(vec.top_n(Side::Bid, &mut top) + vec.top_n(Side::Ask, &mut top))));
        group.finish();
    }
}

criterion_group!(name = book; config = Criterion::default().sample_size(30); targets = benches);
criterion_main!(book);
