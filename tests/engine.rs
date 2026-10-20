use orderbooks::book::{Level, VecBook};
use orderbooks::bus::{self, Control, FeedMsg, FeedState, IoStamps, Kind, Reason};
use orderbooks::config::{Idle, SignalParams, Venue, VenueConfig};
use orderbooks::engine::{Engine, EngineIo, TELEMETRY_RING, Telemetry};
use orderbooks::feed::wire::Depth;
use orderbooks::{logging, timing};
use rtrb::{Consumer, Producer, RingBuffer};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

struct Rig {
    tx: [Producer<FeedMsg>; 2],
    ctl: [Consumer<Control>; 2],
    tele: Consumer<Telemetry>,
    stop: Arc<AtomicBool>,
    engine: Option<JoinHandle<()>>,
}

impl Rig {
    fn new() -> Self {
        timing::init(false);
        let cfgs = Venue::ALL.map(|v| VenueConfig::new(v, "ETHUSDT", Duration::from_secs(5), Idle::Block));
        let ((t0, r0), (t1, r1)) = (RingBuffer::new(bus::DATA_RING), RingBuffer::new(bus::DATA_RING));
        let ((c0, k0), (c1, k1)) = (RingBuffer::new(bus::CONTROL_RING), RingBuffer::new(bus::CONTROL_RING));
        let (tele_tx, tele) = RingBuffer::new(TELEMETRY_RING);
        let stop = Arc::new(AtomicBool::new(false));
        let io = EngineIo { rx: [r0, r1], ctl: [c0, c1], tele: tele_tx, log: logging::channel().0 };
        let engine = Engine::<VecBook>::new(&cfgs, io, SignalParams::default(), Idle::Block, stop.clone()).spawn();
        Rig { tx: [t0, t1], ctl: [k0, k1], tele, stop, engine: Some(engine) }
    }

    fn status(&mut self, i: usize, state: FeedState) {
        self.tx[i].push(FeedMsg::Status { state, reason: Reason::Bridged }).unwrap();
    }

    fn depth(&mut self, i: usize, kind: Kind, (first_u, u, pu): (u64, u64, u64), bids: &[(i64, i64)], asks: &[(i64, i64)]) {
        let mut d = Depth::default();
        (d.hdr.first_u, d.hdr.u, d.hdr.pu) = (first_u, u, pu);
        d.bids.extend(bids.iter().map(|&(px, qty)| Level { px, qty }));
        d.asks.extend(asks.iter().map(|&(px, qty)| Level { px, qty }));
        assert!(bus::push_depth(&mut self.tx[i], kind, &d, timing::now(), IoStamps::default()));
    }

    /// Waits for telemetry that reflects everything pushed so far.
    fn tele(&mut self, ok: impl Fn(&Telemetry) -> bool) -> Telemetry {
        let until = Instant::now() + Duration::from_secs(3);
        while Instant::now() < until {
            while let Ok(t) = self.tele.pop() {
                if ok(&t) {
                    return t;
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
    let mut r = Rig::new();
    r.depth(0, Kind::Snapshot, (0, 100, 0), &[(10000, 5), (9999, 1)], &[(10001, 3), (10002, 4)]);
    r.depth(0, Kind::Diff, (99, 101, 98), &[(10000, 0), (9998, 2)], &[(10001, 7)]);
    r.depth(0, Kind::Diff, (102, 105, 101), &[], &[(10003, 1)]);
    r.status(0, FeedState::Live);
    let t = r.tele(|t| t.venues[0].last_u == 105 && t.venues[0].state == FeedState::Live);
    let b = &t.venues[0];
    assert!(b.tradable && b.valid && !b.crossed);
    assert_eq!(b.bids[..2], [Level { px: 9999, qty: 1 }, Level { px: 9998, qty: 2 }]);
    assert_eq!(b.asks[..3], [Level { px: 10001, qty: 7 }, Level { px: 10002, qty: 4 }, Level { px: 10003, qty: 1 }]);
    assert_eq!((b.commits, b.engine_resyncs), (3, 0));
    assert!(!t.venues[1].tradable, "other venue untouched");
}

#[test]
fn broken_chain_invalidates_book_and_requests_resync() {
    let mut r = Rig::new();
    r.depth(1, Kind::Snapshot, (0, 100, 0), &[(10000, 5)], &[(10001, 3)]);
    r.depth(1, Kind::Diff, (99, 101, 98), &[(10000, 6)], &[]);
    r.depth(1, Kind::Diff, (120, 125, 119), &[(10000, 7)], &[]);
    r.status(1, FeedState::Live);
    let t = r.tele(|t| t.venues[1].last_u == 125);
    assert!(!t.venues[1].valid && !t.venues[1].tradable);
    assert_eq!(t.venues[1].engine_resyncs, 1);
    assert!(matches!(r.ctl[1].pop(), Ok(Control::Resync)));

    r.depth(1, Kind::Snapshot, (0, 200, 0), &[(10000, 1)], &[(10001, 1)]);
    let t = r.tele(|t| t.venues[1].last_u == 200);
    assert!(t.venues[1].valid && t.venues[1].tradable, "fresh snapshot restores the book");
}

#[test]
fn crossed_book_is_flagged_not_resynced() {
    let mut r = Rig::new();
    r.depth(0, Kind::Snapshot, (0, 100, 0), &[(10000, 5)], &[(10001, 3)]);
    r.depth(0, Kind::Diff, (99, 101, 98), &[(10002, 1)], &[]);
    r.status(0, FeedState::Live);
    let t = r.tele(|t| t.venues[0].last_u == 101 && t.venues[0].state == FeedState::Live);
    assert!(t.venues[0].crossed && !t.venues[0].tradable);
    assert_eq!((t.venues[0].crossed_books, t.venues[0].engine_resyncs), (1, 0));
    assert!(r.ctl[0].pop().is_err());
}

#[test]
fn signal_holds_while_one_venue_resyncs() {
    let mut r = Rig::new();
    for i in 0..2 {
        r.depth(i, Kind::Snapshot, (0, 100, 0), &[(10000, 5)], &[(10001, 5)]);
        r.depth(i, Kind::Diff, (99, 101, 98), &[], &[]);
        r.status(i, FeedState::Live);
    }
    r.tele(|t| t.venues.iter().all(|v| v.tradable));
    r.status(0, FeedState::Resyncing);
    let t = r.tele(|t| t.venues[0].state == FeedState::Resyncing);
    assert!(t.venues[1].tradable);
    assert_eq!(t.decision.bias, orderbooks::signal::Bias::Hold);
    assert_eq!(t.decision.why, orderbooks::signal::Why::NotLive(Venue::Binance));
}
