use crate::book::{Level, Side};
use crate::feed::wire::{Depth, Header};
use crate::timing;
use rtrb::Producer;
use serde::Serialize;

pub const LEVELS_PER_ENTRY: usize = 32;
pub const DATA_RING_CAPACITY: usize = 4096;
pub const CONTROL_RING_CAPACITY: usize = 16;

const _: () = assert!(LEVELS_PER_ENTRY <= u8::MAX as usize);

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

/// Per-stage timestamps taken on the I/O thread; all zero when timing is off.
#[derive(Clone, Copy, Debug, Default)]
pub struct IoStamps {
    pub frame_read: u64,
    pub parsed: u64,
    pub sequenced: u64,
    pub pushed: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageKind {
    Snapshot,
    Diff,
}

/// One ring entry. A depth message is a `Begin, Levels*, Commit` transaction.
/// Entries are fixed-size by design (no heap, no pointers across threads).
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Copy, Debug)]
pub enum FeedMessage {
    Status { state: FeedState, reason: Reason },
    Begin { kind: MessageKind, header: Header, received_at: u64 },
    Levels { side: Side, count: u8, levels: [Level; LEVELS_PER_ENTRY] },
    Commit { stamps: IoStamps },
}

#[derive(Clone, Copy, Debug)]
pub enum Control {
    Resync,
}

/// Ring entries one depth message takes: `Begin` + `Commit` + level chunks per side.
pub fn entries_needed(depth: &Depth) -> usize {
    2 + depth.bids.len().div_ceil(LEVELS_PER_ENTRY) + depth.asks.len().div_ceil(LEVELS_PER_ENTRY)
}

/// All-or-nothing: pushes the whole transaction or nothing if the ring lacks room.
/// With a single producer, free slots can only grow after the check, so every push succeeds.
pub fn push_depth(producer: &mut Producer<FeedMessage>, kind: MessageKind, depth: &Depth, received_at: u64, mut stamps: IoStamps) -> bool {
    if producer.slots() < entries_needed(depth) {
        return false;
    }
    let _ = producer.push(FeedMessage::Begin { kind, header: depth.header, received_at });
    for (side, side_levels) in [(Side::Bid, &depth.bids), (Side::Ask, &depth.asks)] {
        for chunk in side_levels.chunks(LEVELS_PER_ENTRY) {
            let mut levels = [Level::default(); LEVELS_PER_ENTRY];
            levels[..chunk.len()].copy_from_slice(chunk);
            let _ = producer.push(FeedMessage::Levels { side, count: chunk.len() as u8, levels });
        }
    }
    if stamps.frame_read != 0 {
        stamps.pushed = timing::now();
    }
    let _ = producer.push(FeedMessage::Commit { stamps });
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunked_roundtrip_and_all_or_nothing() {
        let mut depth = Depth::default();
        depth.header.last_update_id = 7;
        depth.bids.extend((0..70).map(|i| Level { price: 1000 - i, quantity: i + 1 }));
        depth.asks.extend((0..5).map(|i| Level { price: 2000 + i, quantity: 1 }));
        assert_eq!(entries_needed(&depth), 2 + 3 + 1);

        let (mut producer, mut consumer) = rtrb::RingBuffer::new(6);
        assert!(push_depth(&mut producer, MessageKind::Diff, &depth, 1, IoStamps::default()));
        assert!(!push_depth(&mut producer, MessageKind::Diff, &depth, 1, IoStamps::default()));

        let (mut bids, mut asks) = (vec![], vec![]);
        assert!(matches!(consumer.pop(), Ok(FeedMessage::Begin { header, .. }) if header.last_update_id == 7));
        while let Ok(message) = consumer.pop() {
            match message {
                FeedMessage::Levels { side: Side::Bid, count, levels } => bids.extend_from_slice(&levels[..count as usize]),
                FeedMessage::Levels { side: Side::Ask, count, levels } => asks.extend_from_slice(&levels[..count as usize]),
                FeedMessage::Commit { .. } => break,
                other => panic!("{other:?}"),
            }
        }
        assert_eq!((bids, asks), (depth.bids, depth.asks));
    }
}
