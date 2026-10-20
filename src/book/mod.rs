mod btree;
mod vec;

pub use btree::BTreeBook;
pub use vec::VecBook;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Level {
    pub price: i64,
    pub quantity: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

pub trait OrderBook: Default + Send + 'static {
    fn clear(&mut self);
    /// `quantity == 0` removes the level.
    fn apply(&mut self, side: Side, price: i64, quantity: i64);
    /// All updates for one side of one message; same result as applying them in order.
    /// May reorder `levels`.
    fn apply_batch(&mut self, side: Side, levels: &mut [Level]) {
        levels.iter().for_each(|level| self.apply(side, level.price, level.quantity));
    }
    fn best(&self, side: Side) -> Option<Level>;
    fn top_n(&self, side: Side, out: &mut [Level]) -> usize;
    fn len(&self, side: Side) -> usize;
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn level(price: i64, quantity: i64) -> Level {
        Level { price, quantity }
    }

    fn check<B: OrderBook>() {
        let mut book = B::default();
        book.apply(Side::Bid, 100, 5);
        book.apply(Side::Bid, 102, 1);
        book.apply(Side::Bid, 101, 2);
        book.apply(Side::Ask, 105, 3);
        book.apply(Side::Ask, 103, 4);
        book.apply(Side::Bid, 102, 0);
        book.apply(Side::Ask, 104, 0);
        assert_eq!(book.best(Side::Bid), Some(level(101, 2)));
        assert_eq!(book.best(Side::Ask), Some(level(103, 4)));
        let mut top = [Level::default(); 5];
        assert_eq!(book.top_n(Side::Bid, &mut top), 2);
        assert_eq!(top[..2], [level(101, 2), level(100, 5)]);
        book.apply(Side::Ask, 103, 7);
        assert_eq!(book.best(Side::Ask), Some(level(103, 7)));
        book.clear();
        assert_eq!((book.len(Side::Bid), book.len(Side::Ask)), (0, 0));
    }

    #[test]
    fn btree_basic() {
        check::<BTreeBook>();
    }

    #[test]
    fn vec_basic() {
        check::<VecBook>();
    }

    proptest! {
        #[test]
        fn implementations_agree(updates in prop::collection::vec((any::<bool>(), 90i64..110, 0i64..4), 0..500)) {
            let (mut btree, mut vec) = (BTreeBook::default(), VecBook::default());
            for (is_bid, price, quantity) in updates {
                let side = if is_bid { Side::Bid } else { Side::Ask };
                btree.apply(side, price, quantity);
                vec.apply(side, price, quantity);
            }
            for side in [Side::Bid, Side::Ask] {
                let (mut btree_top, mut vec_top) = ([Level::default(); 32], [Level::default(); 32]);
                let (btree_count, vec_count) = (btree.top_n(side, &mut btree_top), vec.top_n(side, &mut vec_top));
                prop_assert_eq!(btree_count, vec_count);
                prop_assert_eq!(&btree_top[..btree_count], &vec_top[..vec_count]);
                prop_assert_eq!(btree.len(side), vec.len(side));
            }
        }

        #[test]
        fn batch_merge_matches_sequential(
            batches in prop::collection::vec((any::<bool>(), 0u8..3, prop::collection::vec((80i64..120, 0i64..4), 0..40)), 0..40)
        ) {
            let (mut sequential, mut merged) = (BTreeBook::default(), VecBook::default());
            for (is_bid, order, updates) in batches {
                let side = if is_bid { Side::Bid } else { Side::Ask };
                let mut levels: Vec<Level> = updates.into_iter().map(|(price, quantity)| level(price, quantity)).collect();
                match order {
                    0 => levels.sort_by_key(|level| level.price),
                    1 => levels.sort_by_key(|level| -level.price),
                    _ => {}
                }
                levels.iter().for_each(|level| sequential.apply(side, level.price, level.quantity));
                merged.apply_batch(side, &mut levels);
            }
            for side in [Side::Bid, Side::Ask] {
                let (mut expected, mut actual) = ([Level::default(); 64], [Level::default(); 64]);
                let (expected_count, actual_count) = (sequential.top_n(side, &mut expected), merged.top_n(side, &mut actual));
                prop_assert_eq!(&expected[..expected_count], &actual[..actual_count]);
                prop_assert_eq!(sequential.len(side), merged.len(side));
            }
        }
    }
}
