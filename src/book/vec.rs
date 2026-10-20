use super::{Level, OrderBook, Side};

/// Levels sorted worst → best, so the best level is at the end and the
/// scan for a price starts from the top of the book.
struct Ladder {
    lv: Vec<Level>,
    tmp: Vec<Level>,
    /// +1 for bids (ascending px), -1 for asks (descending px).
    sign: i64,
}

impl Ladder {
    fn new(sign: i64) -> Self {
        Self { lv: Vec::with_capacity(4096), tmp: Vec::with_capacity(4096), sign }
    }

    #[inline]
    fn apply(&mut self, px: i64, qty: i64) {
        let (s, k) = (self.sign, self.sign * px);
        let mut i = self.lv.len();
        while i > 0 && s * self.lv[i - 1].px > k {
            i -= 1;
        }
        if i > 0 && self.lv[i - 1].px == px {
            if qty == 0 {
                self.lv.remove(i - 1);
            } else {
                self.lv[i - 1].qty = qty;
            }
        } else if qty != 0 {
            self.lv.insert(i, Level { px, qty });
        }
    }

    /// One merge pass over the touched suffix: O(region + m) instead of O(m · depth).
    /// Venues send each side sorted (verified on recordings); other orders are sorted here.
    fn merge(&mut self, upd: &mut [Level]) {
        let s = self.sign;
        let key = |l: &Level| s * l.px;
        if !upd.is_sorted_by_key(key) {
            if upd.windows(2).all(|w| key(&w[0]) > key(&w[1])) {
                upd.reverse();
            } else {
                upd.sort_by_key(key);
            }
        }
        let Some(first) = upd.first() else { return };
        let start = self.lv.partition_point(|l| key(l) < key(first));
        self.tmp.clear();
        self.tmp.extend_from_slice(&self.lv[start..]);
        self.lv.truncate(start);
        let mut i = 0;
        for (j, u) in upd.iter().enumerate() {
            if upd.get(j + 1).is_some_and(|n| n.px == u.px) {
                continue;
            }
            let run = i + self.tmp[i..].partition_point(|l| key(l) < key(u));
            self.lv.extend_from_slice(&self.tmp[i..run]);
            i = run + (self.tmp.get(run).is_some_and(|l| l.px == u.px) as usize);
            if u.qty != 0 {
                self.lv.push(*u);
            }
        }
        self.lv.extend_from_slice(&self.tmp[i..]);
    }
}

pub struct VecBook {
    bids: Ladder,
    asks: Ladder,
}

impl Default for VecBook {
    fn default() -> Self {
        Self { bids: Ladder::new(1), asks: Ladder::new(-1) }
    }
}

impl VecBook {
    fn ladder(&self, side: Side) -> &Ladder {
        match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        }
    }

    fn ladder_mut(&mut self, side: Side) -> &mut Ladder {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }
}

impl OrderBook for VecBook {
    fn clear(&mut self) {
        self.bids.lv.clear();
        self.asks.lv.clear();
    }

    #[inline]
    fn apply(&mut self, side: Side, px: i64, qty: i64) {
        self.ladder_mut(side).apply(px, qty)
    }

    fn apply_batch(&mut self, side: Side, levels: &mut [Level]) {
        self.ladder_mut(side).merge(levels)
    }

    fn best(&self, side: Side) -> Option<Level> {
        self.ladder(side).lv.last().copied()
    }

    fn top_n(&self, side: Side, out: &mut [Level]) -> usize {
        out.iter_mut().zip(self.ladder(side).lv.iter().rev()).map(|(o, l)| *o = *l).count()
    }

    fn len(&self, side: Side) -> usize {
        self.ladder(side).lv.len()
    }
}
