use super::{Level, OrderBook, Side};

/// One side of the book as a sorted array, worst → best: the best level is at the end,
/// so lookups scan from the top of the book.
struct Ladder {
    levels: Vec<Level>,
    scratch: Vec<Level>,
    /// +1 for bids (ascending price), -1 for asks (descending price).
    direction: i64,
}

impl Ladder {
    fn new(direction: i64) -> Self {
        Self { levels: Vec::with_capacity(4096), scratch: Vec::with_capacity(4096), direction }
    }

    #[inline]
    fn apply(&mut self, price: i64, quantity: i64) {
        let direction = self.direction;
        let rank = direction * price;
        let mut index = self.levels.len();
        while index > 0 && direction * self.levels[index - 1].price > rank {
            index -= 1;
        }
        if index > 0 && self.levels[index - 1].price == price {
            if quantity == 0 {
                self.levels.remove(index - 1);
            } else {
                self.levels[index - 1].quantity = quantity;
            }
        } else if quantity != 0 {
            self.levels.insert(index, Level { price, quantity });
        }
    }

    /// One merge pass over the touched suffix: O(region + m) instead of O(m · depth).
    /// Venues send each side sorted (verified on recordings); other orders are sorted here.
    fn merge(&mut self, updates: &mut [Level]) {
        let direction = self.direction;
        let rank = |level: &Level| direction * level.price;
        if !updates.is_sorted_by_key(rank) {
            if updates.windows(2).all(|pair| rank(&pair[0]) > rank(&pair[1])) {
                updates.reverse();
            } else {
                updates.sort_by_key(rank);
            }
        }
        let Some(first_update) = updates.first() else { return };
        let region_start = self.levels.partition_point(|level| rank(level) < rank(first_update));
        self.scratch.clear();
        self.scratch.extend_from_slice(&self.levels[region_start..]);
        self.levels.truncate(region_start);
        let mut next_old = 0;
        for (index, update) in updates.iter().enumerate() {
            if updates.get(index + 1).is_some_and(|following| following.price == update.price) {
                continue;
            }
            let run_end = next_old + self.scratch[next_old..].partition_point(|level| rank(level) < rank(update));
            self.levels.extend_from_slice(&self.scratch[next_old..run_end]);
            let replaces_existing = self.scratch.get(run_end).is_some_and(|level| level.price == update.price);
            next_old = run_end + replaces_existing as usize;
            if update.quantity != 0 {
                self.levels.push(*update);
            }
        }
        self.levels.extend_from_slice(&self.scratch[next_old..]);
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
        self.bids.levels.clear();
        self.asks.levels.clear();
    }

    #[inline]
    fn apply(&mut self, side: Side, price: i64, quantity: i64) {
        self.ladder_mut(side).apply(price, quantity)
    }

    fn apply_batch(&mut self, side: Side, levels: &mut [Level]) {
        self.ladder_mut(side).merge(levels)
    }

    fn best(&self, side: Side) -> Option<Level> {
        self.ladder(side).levels.last().copied()
    }

    fn top_n(&self, side: Side, out: &mut [Level]) -> usize {
        out.iter_mut().zip(self.ladder(side).levels.iter().rev()).map(|(slot, level)| *slot = *level).count()
    }

    fn len(&self, side: Side) -> usize {
        self.ladder(side).levels.len()
    }
}
