use orderbooks::book::{BTreeBook, Level, OrderBook, Side, VecBook};
use orderbooks::config::Venue;
use orderbooks::fixture::{self, RecordedMessage};
use std::path::Path;

fn apply<B: OrderBook>(book: &mut B, message: &RecordedMessage) {
    message.bids.iter().for_each(|level| book.apply(Side::Bid, level.price, level.quantity));
    message.asks.iter().for_each(|level| book.apply(Side::Ask, level.price, level.quantity));
}

fn apply_batch<B: OrderBook>(book: &mut B, message: &RecordedMessage) {
    book.apply_batch(Side::Bid, &mut message.bids.clone());
    book.apply_batch(Side::Ask, &mut message.asks.clone());
}

#[test]
fn recorded_streams_replay_cleanly() {
    for venue in Venue::ALL {
        let recording = fixture::load(Path::new("fixtures"), venue).unwrap();
        assert!(recording.diffs.len() > 100, "{venue:?}: only {} diffs", recording.diffs.len());
        let (mut btree, mut vec, mut vec_merged) = (BTreeBook::default(), VecBook::default(), VecBook::default());
        apply(&mut btree, &recording.snapshot);
        apply(&mut vec, &recording.snapshot);
        apply_batch(&mut vec_merged, &recording.snapshot);
        for (index, diff) in recording.diffs.iter().enumerate() {
            if index > 0 {
                assert_eq!(diff.header.prev_update_id, recording.diffs[index - 1].header.last_update_id, "{venue:?}: chain");
            }
            apply(&mut btree, diff);
            apply(&mut vec, diff);
            apply_batch(&mut vec_merged, diff);
            let (bid, ask) = (vec.best(Side::Bid).unwrap(), vec.best(Side::Ask).unwrap());
            assert!(
                bid.price < ask.price,
                "{venue:?}: crossed at update id {} ({} >= {})",
                diff.header.last_update_id,
                bid.price,
                ask.price
            );
        }
        for side in [Side::Bid, Side::Ask] {
            let mut expected = [Level::default(); 50];
            let (mut vec_top, mut merged_top) = ([Level::default(); 50], [Level::default(); 50]);
            assert_eq!(btree.top_n(side, &mut expected), vec.top_n(side, &mut vec_top));
            assert_eq!(btree.top_n(side, &mut expected), vec_merged.top_n(side, &mut merged_top));
            assert_eq!((expected, btree.len(side)), (vec_top, vec.len(side)));
            assert_eq!((expected, btree.len(side)), (merged_top, vec_merged.len(side)), "{venue:?}: batch merge diverged");
        }
    }
}
