use super::{Level, OrderBook, Side};
use std::collections::BTreeMap;

#[derive(Default)]
pub struct BTreeBook {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl BTreeBook {
    fn side(&mut self, side: Side) -> &mut BTreeMap<i64, i64> {
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
    fn apply(&mut self, side: Side, px: i64, qty: i64) {
        let m = self.side(side);
        if qty == 0 {
            m.remove(&px);
        } else {
            m.insert(px, qty);
        }
    }

    fn best(&self, side: Side) -> Option<Level> {
        let kv = match side {
            Side::Bid => self.bids.last_key_value(),
            Side::Ask => self.asks.first_key_value(),
        };
        kv.map(|(&px, &qty)| Level { px, qty })
    }

    fn top_n(&self, side: Side, out: &mut [Level]) -> usize {
        fn fill<'a>(out: &mut [Level], it: impl Iterator<Item = (&'a i64, &'a i64)>) -> usize {
            out.iter_mut().zip(it).map(|(o, (&px, &qty))| *o = Level { px, qty }).count()
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
