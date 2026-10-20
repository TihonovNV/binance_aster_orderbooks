use orderbooks::book::{Level, VecBook};
use orderbooks::bus::{self, Control, FeedMessage, FeedState, IoStamps, MessageKind, Reason};
use orderbooks::config::{Idle, SignalParams, Venue, VenueConfig};
use orderbooks::engine::{Engine, EngineChannels, TELEMETRY_RING_CAPACITY, Telemetry};
use orderbooks::feed::wire::Depth;
use orderbooks::signal::{Bias, DecisionReason};
use orderbooks::{logging, timing};
use rtrb::{Consumer, Producer, RingBuffer};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const BINANCE: usize = 0;
const ASTER: usize = 1;

/// A running engine driven through its real rings.
struct Rig {
    feeds: [Producer<FeedMessage>; 2],
    control: [Consumer<Control>; 2],
    telemetry: Consumer<Telemetry>,
    stop: Arc<AtomicBool>,
    engine: Option<JoinHandle<()>>,
}

fn level(price: i64, quantity: i64) -> Level {
    Level { price, quantity }
}

impl Rig {
    fn new() -> Self {
        timing::init(false);
        let configs = Venue::ALL.map(|venue| VenueConfig::new(venue, "ETHUSDT", Duration::from_secs(5), Idle::Block));
        let (binance_feed, binance_feed_rx) = RingBuffer::new(bus::DATA_RING_CAPACITY);
        let (aster_feed, aster_feed_rx) = RingBuffer::new(bus::DATA_RING_CAPACITY);
        let (binance_control_tx, binance_control) = RingBuffer::new(bus::CONTROL_RING_CAPACITY);
        let (aster_control_tx, aster_control) = RingBuffer::new(bus::CONTROL_RING_CAPACITY);
        let (telemetry_tx, telemetry) = RingBuffer::new(TELEMETRY_RING_CAPACITY);
        let stop = Arc::new(AtomicBool::new(false));
        let channels = EngineChannels {
            feeds: [binance_feed_rx, aster_feed_rx],
            control: [binance_control_tx, aster_control_tx],
            telemetry: telemetry_tx,
            log: logging::channel().0,
        };
        let engine = Engine::<VecBook>::new(&configs, channels, SignalParams::default(), Idle::Block, stop.clone()).spawn();
        Rig { feeds: [binance_feed, aster_feed], control: [binance_control, aster_control], telemetry, stop, engine: Some(engine) }
    }

    fn status(&mut self, venue: usize, state: FeedState) {
        self.feeds[venue].push(FeedMessage::Status { state, reason: Reason::Bridged }).unwrap();
    }

    fn depth(&mut self, venue: usize, kind: MessageKind, update_ids: (u64, u64, u64), bids: &[(i64, i64)], asks: &[(i64, i64)]) {
        let mut depth = Depth::default();
        let (first, last, previous) = update_ids;
        (depth.header.first_update_id, depth.header.last_update_id, depth.header.prev_update_id) = (first, last, previous);
        depth.bids.extend(bids.iter().map(|&(price, quantity)| level(price, quantity)));
        depth.asks.extend(asks.iter().map(|&(price, quantity)| level(price, quantity)));
        assert!(bus::push_depth(&mut self.feeds[venue], kind, &depth, timing::now(), IoStamps::default()));
    }

    /// Waits for telemetry that satisfies `condition`.
    fn telemetry_where(&mut self, condition: impl Fn(&Telemetry) -> bool) -> Telemetry {
        let until = Instant::now() + Duration::from_secs(3);
        while Instant::now() < until {
            while let Ok(telemetry) = self.telemetry.pop() {
                if condition(&telemetry) {
                    return telemetry;
                }
            }
            std::thread::yield_now();
        }
        panic!("telemetry condition not met");
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.stop.store(true, Relaxed);
        self.engine.take().unwrap().join().unwrap();
    }
}

#[test]
fn snapshot_then_diffs_build_tradable_book() {
    let mut rig = Rig::new();
    rig.depth(BINANCE, MessageKind::Snapshot, (0, 100, 0), &[(10000, 5), (9999, 1)], &[(10001, 3), (10002, 4)]);
    rig.depth(BINANCE, MessageKind::Diff, (99, 101, 98), &[(10000, 0), (9998, 2)], &[(10001, 7)]);
    rig.depth(BINANCE, MessageKind::Diff, (102, 105, 101), &[], &[(10003, 1)]);
    rig.status(BINANCE, FeedState::Live);
    let telemetry = rig.telemetry_where(|t| t.venues[BINANCE].last_update_id == 105 && t.venues[BINANCE].state == FeedState::Live);
    let binance = &telemetry.venues[BINANCE];
    assert!(binance.tradable && binance.valid && !binance.crossed);
    assert_eq!(binance.bids[..2], [level(9999, 1), level(9998, 2)]);
    assert_eq!(binance.asks[..3], [level(10001, 7), level(10002, 4), level(10003, 1)]);
    assert_eq!((binance.commits, binance.engine_resyncs), (3, 0));
    assert!(!telemetry.venues[ASTER].tradable, "other venue untouched");
}

#[test]
fn broken_chain_invalidates_book_and_requests_resync() {
    let mut rig = Rig::new();
    rig.depth(ASTER, MessageKind::Snapshot, (0, 100, 0), &[(10000, 5)], &[(10001, 3)]);
    rig.depth(ASTER, MessageKind::Diff, (99, 101, 98), &[(10000, 6)], &[]);
    rig.depth(ASTER, MessageKind::Diff, (120, 125, 119), &[(10000, 7)], &[]);
    rig.status(ASTER, FeedState::Live);
    let telemetry = rig.telemetry_where(|t| t.venues[ASTER].last_update_id == 125);
    assert!(!telemetry.venues[ASTER].valid && !telemetry.venues[ASTER].tradable);
    assert_eq!(telemetry.venues[ASTER].engine_resyncs, 1);
    assert!(matches!(rig.control[ASTER].pop(), Ok(Control::Resync)));

    rig.depth(ASTER, MessageKind::Snapshot, (0, 200, 0), &[(10000, 1)], &[(10001, 1)]);
    let telemetry = rig.telemetry_where(|t| t.venues[ASTER].last_update_id == 200);
    assert!(telemetry.venues[ASTER].valid && telemetry.venues[ASTER].tradable, "fresh snapshot restores the book");
}

#[test]
fn crossed_book_is_flagged_not_resynced() {
    let mut rig = Rig::new();
    rig.depth(BINANCE, MessageKind::Snapshot, (0, 100, 0), &[(10000, 5)], &[(10001, 3)]);
    rig.depth(BINANCE, MessageKind::Diff, (99, 101, 98), &[(10002, 1)], &[]);
    rig.status(BINANCE, FeedState::Live);
    let telemetry = rig.telemetry_where(|t| t.venues[BINANCE].last_update_id == 101 && t.venues[BINANCE].state == FeedState::Live);
    assert!(telemetry.venues[BINANCE].crossed && !telemetry.venues[BINANCE].tradable);
    assert_eq!((telemetry.venues[BINANCE].crossed_books, telemetry.venues[BINANCE].engine_resyncs), (1, 0));
    assert!(rig.control[BINANCE].pop().is_err());
}

#[test]
fn signal_holds_while_one_venue_resyncs() {
    let mut rig = Rig::new();
    for venue in [BINANCE, ASTER] {
        rig.depth(venue, MessageKind::Snapshot, (0, 100, 0), &[(10000, 5)], &[(10001, 5)]);
        rig.depth(venue, MessageKind::Diff, (99, 101, 98), &[], &[]);
        rig.status(venue, FeedState::Live);
    }
    rig.telemetry_where(|t| t.venues.iter().all(|venue| venue.tradable));
    rig.status(BINANCE, FeedState::Resyncing);
    let telemetry = rig.telemetry_where(|t| t.venues[BINANCE].state == FeedState::Resyncing);
    assert!(telemetry.venues[ASTER].tradable);
    assert_eq!(telemetry.decision.bias, Bias::Hold);
    assert_eq!(telemetry.decision.reason, DecisionReason::NotLive(Venue::Binance));
}
