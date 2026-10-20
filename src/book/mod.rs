mod btree;
mod vec;

pub use btree::BTreeBook;
pub use vec::VecBook;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Level {
    pub px: i64,
    pub qty: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

pub trait OrderBook: Default + Send + 'static {
    fn clear(&mut self);
    /// `qty == 0` removes the level.
    fn apply(&mut self, side: Side, px: i64, qty: i64);
    /// All updates for one side of one message; same result as applying them in order.
    /// May reorder `levels`.
    fn apply_batch(&mut self, side: Side, levels: &mut [Level]) {
        levels.iter().for_each(|l| self.apply(side, l.px, l.qty));
    }
    fn best(&self, side: Side) -> Option<Level>;
    fn top_n(&self, side: Side, out: &mut [Level]) -> usize;
    fn len(&self, side: Side) -> usize;
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn check<B: OrderBook>() {
        let mut b = B::default();
        b.apply(Side::Bid, 100, 5);
        b.apply(Side::Bid, 102, 1);
        b.apply(Side::Bid, 101, 2);
        b.apply(Side::Ask, 105, 3);
        b.apply(Side::Ask, 103, 4);
        b.apply(Side::Bid, 102, 0);
        b.apply(Side::Ask, 104, 0);
        assert_eq!(b.best(Side::Bid), Some(Level { px: 101, qty: 2 }));
        assert_eq!(b.best(Side::Ask), Some(Level { px: 103, qty: 4 }));
        let mut out = [Level::default(); 5];
        assert_eq!(b.top_n(Side::Bid, &mut out), 2);
        assert_eq!(out[..2], [Level { px: 101, qty: 2 }, Level { px: 100, qty: 5 }]);
        b.apply(Side::Ask, 103, 7);
        assert_eq!(b.best(Side::Ask), Some(Level { px: 103, qty: 7 }));
        b.clear();
        assert_eq!((b.len(Side::Bid), b.len(Side::Ask)), (0, 0));
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
        fn implementations_agree(ops in prop::collection::vec((any::<bool>(), 90i64..110, 0i64..4), 0..500)) {
            let (mut a, mut b) = (BTreeBook::default(), VecBook::default());
            for (bid, px, qty) in ops {
                let side = if bid { Side::Bid } else { Side::Ask };
                a.apply(side, px, qty);
                b.apply(side, px, qty);
            }
            for side in [Side::Bid, Side::Ask] {
                let (mut x, mut y) = ([Level::default(); 32], [Level::default(); 32]);
                let (n, m) = (a.top_n(side, &mut x), b.top_n(side, &mut y));
                prop_assert_eq!(n, m);
                prop_assert_eq!(&x[..n], &y[..m]);
                prop_assert_eq!(a.len(side), b.len(side));
            }
        }

        #[test]
        fn batch_merge_matches_sequential(
            batches in prop::collection::vec((any::<bool>(), 0u8..3, prop::collection::vec((80i64..120, 0i64..4), 0..40)), 0..40)
        ) {
            let (mut a, mut b) = (BTreeBook::default(), VecBook::default());
            for (bid, order, upd) in batches {
                let side = if bid { Side::Bid } else { Side::Ask };
                let mut lv: Vec<Level> = upd.into_iter().map(|(px, qty)| Level { px, qty }).collect();
                match order {
                    0 => lv.sort_by_key(|l| l.px),
                    1 => lv.sort_by_key(|l| -l.px),
                    _ => {}
                }
                lv.iter().for_each(|l| a.apply(side, l.px, l.qty));
                b.apply_batch(side, &mut lv);
            }
            for side in [Side::Bid, Side::Ask] {
                let (mut x, mut y) = ([Level::default(); 64], [Level::default(); 64]);
                let (n, m) = (a.top_n(side, &mut x), b.top_n(side, &mut y));
                prop_assert_eq!(&x[..n], &y[..m]);
                prop_assert_eq!(a.len(side), b.len(side));
            }
        }
    }
}
