use crate::book::{Level, OrderBook, Side};
use crate::bus::{Control, FeedMsg, FeedState, Kind, Reason};
use crate::config::{Idle, SignalParams, Venue, VenueConfig};
use crate::feed::wire::Header;
use crate::fixed::to_f64;
use crate::logging::{Log, LogEvent};
use crate::signal::{self, Bbo, Decision, SignalState, Snap};
use crate::{rt, timing};
use hdrhistogram::Histogram;
use rtrb::{Consumer, Producer};
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub const TELEMETRY_RING: usize = 64;
pub const TOP: usize = 10;
const DRAIN_PER_PASS: usize = 64;
const EVAL_EVERY_NS: u64 = 250_000_000;
const TELEMETRY_EVERY_NS: u64 = 100_000_000;
const DECISION_LOG_EVERY_NS: u64 = 1_000_000_000;
const TIMING_WINDOW_NS: u64 = 10_000_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Parse,
    Sync,
    Push,
    RingHop,
    Apply,
    Signal,
    EndToEnd,
}

impl Stage {
    pub const ALL: [Stage; 7] = [Stage::Parse, Stage::Sync, Stage::Push, Stage::RingHop, Stage::Apply, Stage::Signal, Stage::EndToEnd];
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct StageStats {
    pub count: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct VenueTele {
    pub venue: Venue,
    pub state: FeedState,
    pub reason: Reason,
    pub in_state_ms: u64,
    pub tradable: bool,
    pub crossed: bool,
    pub valid: bool,
    pub last_u: u64,
    pub event_ms: u64,
    pub age_ms: f64,
    pub depth: (usize, usize),
    pub bids: [Level; TOP],
    pub asks: [Level; TOP],
    pub crossed_books: u64,
    pub engine_resyncs: u64,
    pub commits: u64,
    pub timing: [StageStats; 7],
}

#[derive(Clone, Copy, Debug)]
pub struct Telemetry {
    pub venues: [VenueTele; 2],
    pub decision: Decision,
}

struct BookState<B> {
    venue: Venue,
    px_scale: u32,
    qty_scale: u32,
    book: B,
    state: FeedState,
    reason: Reason,
    since: u64,
    processing_transaction: bool,
    valid: bool,
    crossed: bool,
    chained: bool,
    hdr: Header,
    pending: (Header, u64, Kind),
    recv: u64,
    commits: u64,
    crossed_books: u64,
    engine_resyncs: u64,
    batch: [Vec<Level>; 2],
    hist: [Histogram<u64>; 7],
    window: [StageStats; 7],
}

impl<B: OrderBook> BookState<B> {
    fn new(cfg: &VenueConfig) -> Self {
        let h = || Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("histogram");
        Self {
            venue: cfg.venue,
            px_scale: cfg.px_scale,
            qty_scale: cfg.qty_scale,
            book: B::default(),
            state: FeedState::Disconnected,
            reason: Reason::Start,
            since: timing::now(),
            processing_transaction: false,
            valid: false,
            crossed: false,
            chained: false,
            hdr: Header::default(),
            pending: (Header::default(), 0, Kind::Snapshot),
            recv: 0,
            commits: 0,
            crossed_books: 0,
            engine_resyncs: 0,
            batch: [Vec::with_capacity(4096), Vec::with_capacity(4096)],
            hist: std::array::from_fn(|_| h()),
            window: [StageStats::default(); 7],
        }
    }

    fn tradable(&self) -> bool {
        self.state == FeedState::Live && !self.processing_transaction && self.valid && !self.crossed
    }

    #[inline]
    fn record(&mut self, s: Stage, ns: u64) {
        let _ = self.hist[s as usize].record(ns.max(1));
    }

    fn snap(&self, now: u64) -> Snap {
        let (px, q) = (|v: i64| to_f64(v, self.px_scale), |v: i64| to_f64(v, self.qty_scale));
        let bbo = match (self.book.best(Side::Bid), self.book.best(Side::Ask)) {
            (Some(b), Some(a)) => Some(Bbo { bid: px(b.px), bid_qty: q(b.qty), ask: px(a.px), ask_qty: q(a.qty) }),
            _ => None,
        };
        let sum5 = |side| {
            let mut top = [Level::default(); 5];
            let n = self.book.top_n(side, &mut top);
            q(top[..n].iter().map(|l| l.qty).sum())
        };
        Snap {
            venue: self.venue,
            state: self.state,
            processing_transaction: self.processing_transaction,
            valid: self.valid,
            crossed: self.crossed,
            bbo,
            bid5: sum5(Side::Bid),
            ask5: sum5(Side::Ask),
            age_ms: if self.recv == 0 { f64::INFINITY } else { timing::nanos(self.recv, now) as f64 / 1e6 },
            seq: self.hdr.u,
        }
    }

    fn roll_window(&mut self) {
        for (w, h) in self.window.iter_mut().zip(&mut self.hist) {
            *w = StageStats {
                count: h.len(),
                p50_ns: h.value_at_quantile(0.5),
                p99_ns: h.value_at_quantile(0.99),
                p999_ns: h.value_at_quantile(0.999),
                max_ns: h.max(),
            };
            h.reset();
        }
    }

    fn tele(&self, now: u64) -> VenueTele {
        let (mut bids, mut asks) = ([Level::default(); TOP], [Level::default(); TOP]);
        self.book.top_n(Side::Bid, &mut bids);
        self.book.top_n(Side::Ask, &mut asks);
        VenueTele {
            venue: self.venue,
            state: self.state,
            reason: self.reason,
            in_state_ms: timing::nanos(self.since, now) / 1_000_000,
            tradable: self.tradable(),
            crossed: self.crossed,
            valid: self.valid,
            last_u: self.hdr.u,
            event_ms: self.hdr.event_ms,
            age_ms: if self.recv == 0 { f64::NAN } else { timing::nanos(self.recv, now) as f64 / 1e6 },
            depth: (self.book.len(Side::Bid), self.book.len(Side::Ask)),
            bids,
            asks,
            crossed_books: self.crossed_books,
            engine_resyncs: self.engine_resyncs,
            commits: self.commits,
            timing: self.window,
        }
    }
}

pub struct EngineIo {
    pub rx: [Consumer<FeedMsg>; 2],
    pub ctl: [Producer<Control>; 2],
    pub tele: Producer<Telemetry>,
    pub log: Log,
}

/// Single owner of both books; the signal runs inline between transactions.
pub struct Engine<B> {
    books: [BookState<B>; 2],
    io: EngineIo,
    sig: SignalState,
    params: SignalParams,
    last: Decision,
    state_changed: bool,
    idle: Idle,
    stop: Arc<AtomicBool>,
}

impl<B: OrderBook> Engine<B> {
    pub fn new(cfgs: &[VenueConfig; 2], io: EngineIo, params: SignalParams, idle: Idle, stop: Arc<AtomicBool>) -> Self {
        assert_eq!([cfgs[0].venue, cfgs[1].venue], Venue::ALL);
        Self {
            books: [BookState::new(&cfgs[0]), BookState::new(&cfgs[1])],
            io,
            sig: SignalState::default(),
            params,
            last: Decision::default(),
            state_changed: false,
            idle,
            stop,
        }
    }

    pub fn spawn(self) -> JoinHandle<()> {
        thread::Builder::new()
            .name("engine".into())
            .spawn(move || {
                rt::hot_thread();
                self.run()
            })
            .expect("spawn engine")
    }

    fn run(mut self) {
        let mut stamps: [Option<(u64, u64)>; 2] = [None; 2];
        let (mut last_eval, mut last_tele, mut last_log, mut last_roll) = (0, 0, 0, timing::now());
        while !self.stop.load(Relaxed) {
            let mut committed = false;
            for (i, stamp) in stamps.iter_mut().enumerate() {
                for _ in 0..DRAIN_PER_PASS {
                    let Ok(m) = self.io.rx[i].pop() else { break };
                    if let Some(s) = self.handle(i, m) {
                        *stamp = Some(s);
                        committed = true;
                    }
                }
            }
            let now = timing::now();
            if std::mem::take(&mut self.state_changed) || committed || timing::nanos(last_eval, now) > EVAL_EVERY_NS {
                let prev = self.last.bias;
                let (b, a) = (self.books[0].snap(now), self.books[1].snap(now));
                self.last = signal::decide(&b, &a, &mut self.sig, &self.params, now, timing::wall_ms());
                last_eval = now;
                let t6 = timing::now();
                for (i, s) in stamps.iter_mut().enumerate() {
                    if let Some((t0, t5)) = s.take()
                        && t0 != 0
                    {
                        self.books[i].record(Stage::Signal, timing::nanos(t5, t6));
                        self.books[i].record(Stage::EndToEnd, timing::nanos(t0, t6));
                    }
                }
                if self.last.bias != prev || timing::nanos(last_log, now) > DECISION_LOG_EVERY_NS {
                    self.io.log.emit(LogEvent::Decision(self.last));
                    last_log = now;
                }
                if self.last.bias != prev {
                    last_tele = 0;
                }
            }
            if timing::nanos(last_roll, now) > TIMING_WINDOW_NS {
                self.books.iter_mut().for_each(BookState::roll_window);
                last_roll = now;
            }
            if timing::nanos(last_tele, now) > TELEMETRY_EVERY_NS {
                let t = Telemetry { venues: [self.books[0].tele(now), self.books[1].tele(now)], decision: self.last };
                let _ = self.io.tele.push(t);
                last_tele = now;
            }
            if !committed {
                match self.idle {
                    Idle::Spin => std::hint::spin_loop(),
                    Idle::Block => thread::sleep(Duration::from_micros(200)),
                }
            }
        }
    }

    /// Returns `(t0, commit time)` when a transaction committed (t0 = 0 if timing was off).
    #[inline]
    fn handle(&mut self, i: usize, m: FeedMsg) -> Option<(u64, u64)> {
        let b = &mut self.books[i];
        match m {
            FeedMsg::Status { state, reason } => {
                (b.state, b.reason, b.since) = (state, reason, timing::now());
                self.state_changed = true;
                None
            }
            FeedMsg::Begin { kind, hdr, recv } => {
                if kind == Kind::Snapshot {
                    b.book.clear();
                    (b.valid, b.chained) = (true, false);
                } else if b.chained && b.valid && hdr.pu != b.hdr.u {
                    b.valid = false;
                    b.engine_resyncs += 1;
                    self.io.log.emit(LogEvent::EngineResync { venue: b.venue, last_u: b.hdr.u, pu: hdr.pu });
                    let _ = self.io.ctl[i].push(Control::Resync);
                }
                b.batch.iter_mut().for_each(Vec::clear);
                b.processing_transaction = true;
                b.pending = (hdr, recv, kind);
                None
            }
            FeedMsg::Levels { side, n, lv } => {
                b.batch[side as usize].extend_from_slice(&lv[..n as usize]);
                None
            }
            FeedMsg::Commit { stamps: s } => {
                let popped = timing::now();
                for (side, lv) in [Side::Bid, Side::Ask].into_iter().zip(&mut b.batch) {
                    b.book.apply_batch(side, lv);
                    lv.clear();
                }
                let t5 = timing::now();
                let kind;
                (b.hdr, b.recv, kind) = b.pending;
                b.processing_transaction = false;
                b.chained = kind == Kind::Diff;
                b.commits += 1;
                let crossed = matches!((b.book.best(Side::Bid), b.book.best(Side::Ask)), (Some(x), Some(y)) if x.px >= y.px);
                if crossed && !b.crossed {
                    b.crossed_books += 1;
                    let (x, y) = (b.book.best(Side::Bid).unwrap(), b.book.best(Side::Ask).unwrap());
                    self.io.log.emit(LogEvent::Crossed { venue: b.venue, bid: x.px, ask: y.px });
                }
                b.crossed = crossed;
                if s.t0 == 0 {
                    return Some((0, t5));
                }
                b.record(Stage::Parse, timing::nanos(s.t0, s.t1));
                b.record(Stage::Sync, timing::nanos(s.t1, s.t2));
                b.record(Stage::Push, timing::nanos(s.t2, s.t3));
                b.record(Stage::RingHop, timing::nanos(s.t3, popped));
                b.record(Stage::Apply, timing::nanos(popped, t5));
                Some((s.t0, t5))
            }
        }
    }
}
