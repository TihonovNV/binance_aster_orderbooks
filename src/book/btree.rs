use super::{Level, OrderBook, Side};
use std::collections::BTreeMap;

/// Price → quantity per side, kept sorted by the map.
#[derive(Default)]
pub struct BTreeBook {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl BTreeBook {
    fn side_mut(&mut self, side: Side) -> &mut BTreeMap<i64, i64> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }
}

impl OrderBook for BTreeBook {
    fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    #[inline]
    fn apply(&mut self, side: Side, price: i64, quantity: i64) {
        let levels = self.side_mut(side);
        if quantity == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, quantity);
        }
    }

    fn best(&self, side: Side) -> Option<Level> {
        let best = match side {
            Side::Bid => self.bids.last_key_value(),
            Side::Ask => self.asks.first_key_value(),
        };
        best.map(|(&price, &quantity)| Level { price, quantity })
    }

    fn top_n(&self, side: Side, out: &mut [Level]) -> usize {
        fn fill<'a>(out: &mut [Level], best_first: impl Iterator<Item = (&'a i64, &'a i64)>) -> usize {
            out.iter_mut().zip(best_first).map(|(slot, (&price, &quantity))| *slot = Level { price, quantity }).count()
        }
        match side {
            Side::Bid => fill(out, self.bids.iter().rev()),
            Side::Ask => fill(out, self.asks.iter()),
        }
    }

    fn len(&self, side: Side) -> usize {
        match side {
            Side::Bid => self.bids.len(),
            Side::Ask => self.asks.len(),
        }
    }
}
