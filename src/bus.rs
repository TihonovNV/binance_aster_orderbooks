use crate::book::{Level, Side};
use crate::feed::wire::{Depth, Header};
use crate::timing;
use rtrb::Producer;
use serde::Serialize;

pub const CHUNK: usize = 32;
pub const DATA_RING: usize = 4096;
pub const CONTROL_RING: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FeedState {
    Connecting,
    Syncing,
    Live,
    Resyncing,
    Disconnected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    Start,
    Connected,
    Bridged,
    Gap,
    StaleSnapshot,
    Overflow,
    EngineRequest,
    Watchdog,
    Closed,
    Io,
    Rotation,
    Rest,
    Panic,
    Shutdown,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct IoStamps {
    pub t0: u64,
    pub t1: u64,
    pub t2: u64,
    pub t3: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Snapshot,
    Diff,
}

/// One ring entry. A depth message is a `Begin, Levels*, Commit` transaction.
/// Entries are fixed-size by design (no heap, no pointers across threads).
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Copy, Debug)]
pub enum FeedMsg {
    Status { state: FeedState, reason: Reason },
    Begin { kind: Kind, hdr: Header, recv: u64 },
    Levels { side: Side, n: u8, lv: [Level; CHUNK] },
    Commit { stamps: IoStamps },
}

#[derive(Clone, Copy, Debug)]
pub enum Control {
    Resync,
}

pub fn slots_for(d: &Depth) -> usize {
    2 + d.bids.len().div_ceil(CHUNK) + d.asks.len().div_ceil(CHUNK)
}

/// All-or-nothing: pushes the whole transaction or nothing if the ring lacks room.
pub fn push_depth(tx: &mut Producer<FeedMsg>, kind: Kind, d: &Depth, recv: u64, mut stamps: IoStamps) -> bool {
    if tx.slots() < slots_for(d) {
        return false;
    }
    let _ = tx.push(FeedMsg::Begin { kind, hdr: d.hdr, recv });
    for (side, levels) in [(Side::Bid, &d.bids), (Side::Ask, &d.asks)] {
        for c in levels.chunks(CHUNK) {
            let mut lv = [Level::default(); CHUNK];
            lv[..c.len()].copy_from_slice(c);
            let _ = tx.push(FeedMsg::Levels { side, n: c.len() as u8, lv });
        }
    }
    if stamps.t0 != 0 {
        stamps.t3 = timing::now();
    }
    let _ = tx.push(FeedMsg::Commit { stamps });
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunked_roundtrip_and_all_or_nothing() {
        let mut d = Depth::default();
        d.hdr.u = 7;
        d.bids.extend((0..70).map(|i| Level { px: 1000 - i, qty: i + 1 }));
        d.asks.extend((0..5).map(|i| Level { px: 2000 + i, qty: 1 }));
        assert_eq!(slots_for(&d), 2 + 3 + 1);

        let (mut tx, mut rx) = rtrb::RingBuffer::new(6);
        assert!(push_depth(&mut tx, Kind::Diff, &d, 1, IoStamps::default()));
        assert!(!push_depth(&mut tx, Kind::Diff, &d, 1, IoStamps::default()));

        let (mut bids, mut asks) = (vec![], vec![]);
        assert!(matches!(rx.pop(), Ok(FeedMsg::Begin { hdr, .. }) if hdr.u == 7));
        while let Ok(m) = rx.pop() {
            match m {
                FeedMsg::Levels { side: Side::Bid, n, lv } => bids.extend_from_slice(&lv[..n as usize]),
                FeedMsg::Levels { side: Side::Ask, n, lv } => asks.extend_from_slice(&lv[..n as usize]),
                FeedMsg::Commit { .. } => break,
                m => panic!("{m:?}"),
            }
        }
        assert_eq!((bids, asks), (d.bids, d.asks));
    }
}
