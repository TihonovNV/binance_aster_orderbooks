use orderbooks::book::{BTreeBook, Level, OrderBook, Side, VecBook};
use orderbooks::config::Venue;
use orderbooks::fixture::{self, Msg};
use std::path::Path;

fn apply<B: OrderBook>(b: &mut B, m: &Msg) {
    m.bids.iter().for_each(|l| b.apply(Side::Bid, l.px, l.qty));
    m.asks.iter().for_each(|l| b.apply(Side::Ask, l.px, l.qty));
}

fn apply_batch<B: OrderBook>(b: &mut B, m: &Msg) {
    b.apply_batch(Side::Bid, &mut m.bids.clone());
    b.apply_batch(Side::Ask, &mut m.asks.clone());
}

#[test]
fn recorded_streams_replay_cleanly() {
    for v in Venue::ALL {
        let rec = fixture::load(Path::new("fixtures"), v).unwrap();
        assert!(rec.diffs.len() > 100, "{v:?}: only {} diffs", rec.diffs.len());
        let (mut a, mut b, mut c) = (BTreeBook::default(), VecBook::default(), VecBook::default());
        apply(&mut a, &rec.snapshot);
        apply(&mut b, &rec.snapshot);
        apply_batch(&mut c, &rec.snapshot);
        for (i, m) in rec.diffs.iter().enumerate() {
            if i > 0 {
                assert_eq!(m.hdr.pu, rec.diffs[i - 1].hdr.u, "{v:?}: chain");
            }
            apply(&mut a, m);
            apply(&mut b, m);
            apply_batch(&mut c, m);
            let (bid, ask) = (b.best(Side::Bid).unwrap(), b.best(Side::Ask).unwrap());
            assert!(bid.px < ask.px, "{v:?}: crossed at u={} ({} >= {})", m.hdr.u, bid.px, ask.px);
        }
        for side in [Side::Bid, Side::Ask] {
            let (mut x, mut y, mut z) = ([Level::default(); 50], [Level::default(); 50], [Level::default(); 50]);
            assert_eq!(a.top_n(side, &mut x), b.top_n(side, &mut y));
            assert_eq!(a.top_n(side, &mut x), c.top_n(side, &mut z));
            assert_eq!((x, a.len(side)), (y, b.len(side)));
            assert_eq!((x, a.len(side)), (z, c.len(side)), "{v:?}: batch merge diverged");
        }
    }
}
