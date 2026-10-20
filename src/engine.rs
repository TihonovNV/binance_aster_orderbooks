use crate::book::{Level, OrderBook, Side};
use crate::bus::{Control, FeedMessage, FeedState, MessageKind, Reason};
use crate::config::{Idle, SignalParams, Venue, VenueConfig};
use crate::feed::wire::Header;
use crate::fixed::to_f64;
use crate::logging::{Log, LogEvent};
use crate::signal::{self, BookView, Decision, SignalState, TopOfBook};
use crate::{rt, timing};
use hdrhistogram::Histogram;
use rtrb::{Consumer, Producer};
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub const TELEMETRY_RING_CAPACITY: usize = 64;
pub const TELEMETRY_DEPTH: usize = 10;
const MAX_ENTRIES_PER_PASS: usize = 64;
const EVALUATE_EVERY_NS: u64 = 250_000_000;
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

/// Copy of one venue's state for the HTTP thread.
#[derive(Clone, Copy, Debug)]
pub struct VenueTelemetry {
    pub venue: Venue,
    pub state: FeedState,
    pub reason: Reason,
    pub in_state_ms: u64,
    pub tradable: bool,
    pub crossed: bool,
    pub valid: bool,
    pub last_update_id: u64,
    pub event_time_ms: u64,
    pub book_age_ms: f64,
    pub level_counts: (usize, usize),
    pub bids: [Level; TELEMETRY_DEPTH],
    pub asks: [Level; TELEMETRY_DEPTH],
    pub crossed_books: u64,
    pub engine_resyncs: u64,
    pub commits: u64,
    pub timing: [StageStats; 7],
}

#[derive(Clone, Copy, Debug)]
pub struct Telemetry {
    pub venues: [VenueTelemetry; 2],
    pub decision: Decision,
}

/// The `Begin` of the transaction being received, applied at its `Commit`.
#[derive(Clone, Copy)]
struct OpenTransaction {
    header: Header,
    received_at: u64,
    kind: MessageKind,
}

/// One venue's book plus everything the engine tracks about it.
struct VenueBook<B> {
    venue: Venue,
    price_scale: u32,
    quantity_scale: u32,
    book: B,
    state: FeedState,
    reason: Reason,
    state_since: u64,
    processing_transaction: bool,
    valid: bool,
    crossed: bool,
    last_commit_was_diff: bool,
    header: Header,
    open_transaction: OpenTransaction,
    received_at: u64,
    commits: u64,
    crossed_books: u64,
    engine_resyncs: u64,
    pending_levels: [Vec<Level>; 2],
    histograms: [Histogram<u64>; 7],
    last_window: [StageStats; 7],
}

impl<B: OrderBook> VenueBook<B> {
    fn new(config: &VenueConfig) -> Self {
        let histogram = || Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("histogram");
        Self {
            venue: config.venue,
            price_scale: config.price_scale,
            quantity_scale: config.quantity_scale,
            book: B::default(),
            state: FeedState::Disconnected,
            reason: Reason::Start,
            state_since: timing::now(),
            processing_transaction: false,
            valid: false,
            crossed: false,
            last_commit_was_diff: false,
            header: Header::default(),
            open_transaction: OpenTransaction { header: Header::default(), received_at: 0, kind: MessageKind::Snapshot },
            received_at: 0,
            commits: 0,
            crossed_books: 0,
            engine_resyncs: 0,
            pending_levels: [Vec::with_capacity(4096), Vec::with_capacity(4096)],
            histograms: std::array::from_fn(|_| histogram()),
            last_window: [StageStats::default(); 7],
        }
    }

    fn tradable(&self) -> bool {
        self.state == FeedState::Live && !self.processing_transaction && self.valid && !self.crossed
    }

    #[inline]
    fn record(&mut self, stage: Stage, ns: u64) {
        let _ = self.histograms[stage as usize].record(ns.max(1));
    }

    fn view(&self, now: u64) -> BookView {
        let price = |value: i64| to_f64(value, self.price_scale);
        let quantity = |value: i64| to_f64(value, self.quantity_scale);
        let top = match (self.book.best(Side::Bid), self.book.best(Side::Ask)) {
            (Some(bid), Some(ask)) => Some(TopOfBook {
                bid: price(bid.price),
                bid_quantity: quantity(bid.quantity),
                ask: price(ask.price),
                ask_quantity: quantity(ask.quantity),
            }),
            _ => None,
        };
        let depth_5 = |side| {
            let mut top_levels = [Level::default(); 5];
            let count = self.book.top_n(side, &mut top_levels);
            quantity(top_levels[..count].iter().map(|level| level.quantity).sum())
        };
        BookView {
            venue: self.venue,
            state: self.state,
            processing_transaction: self.processing_transaction,
            valid: self.valid,
            crossed: self.crossed,
            top,
            bid_depth_5: depth_5(Side::Bid),
            ask_depth_5: depth_5(Side::Ask),
            age_ms: if self.received_at == 0 { f64::INFINITY } else { timing::nanos(self.received_at, now) as f64 / 1e6 },
            last_update_id: self.header.last_update_id,
        }
    }

    fn roll_window(&mut self) {
        for (stats, histogram) in self.last_window.iter_mut().zip(&mut self.histograms) {
            *stats = StageStats {
                count: histogram.len(),
                p50_ns: histogram.value_at_quantile(0.5),
                p99_ns: histogram.value_at_quantile(0.99),
                p999_ns: histogram.value_at_quantile(0.999),
                max_ns: histogram.max(),
            };
            histogram.reset();
        }
    }

    fn telemetry(&self, now: u64) -> VenueTelemetry {
        let (mut bids, mut asks) = ([Level::default(); TELEMETRY_DEPTH], [Level::default(); TELEMETRY_DEPTH]);
        self.book.top_n(Side::Bid, &mut bids);
        self.book.top_n(Side::Ask, &mut asks);
        VenueTelemetry {
            venue: self.venue,
            state: self.state,
            reason: self.reason,
            in_state_ms: timing::nanos(self.state_since, now) / 1_000_000,
            tradable: self.tradable(),
            crossed: self.crossed,
            valid: self.valid,
            last_update_id: self.header.last_update_id,
            event_time_ms: self.header.event_time_ms,
            book_age_ms: if self.received_at == 0 { f64::NAN } else { timing::nanos(self.received_at, now) as f64 / 1e6 },
            level_counts: (self.book.len(Side::Bid), self.book.len(Side::Ask)),
            bids,
            asks,
            crossed_books: self.crossed_books,
            engine_resyncs: self.engine_resyncs,
            commits: self.commits,
            timing: self.last_window,
        }
    }
}

pub struct EngineChannels {
    pub feeds: [Consumer<FeedMessage>; 2],
    pub control: [Producer<Control>; 2],
    pub telemetry: Producer<Telemetry>,
    pub log: Log,
}

/// Single owner of both books; the signal runs inline between transactions.
pub struct Engine<B> {
    venues: [VenueBook<B>; 2],
    channels: EngineChannels,
    signal_state: SignalState,
    params: SignalParams,
    last_decision: Decision,
    state_changed: bool,
    idle: Idle,
    stop: Arc<AtomicBool>,
}

impl<B: OrderBook> Engine<B> {
    pub fn new(configs: &[VenueConfig; 2], channels: EngineChannels, params: SignalParams, idle: Idle, stop: Arc<AtomicBool>) -> Self {
        assert_eq!([configs[0].venue, configs[1].venue], Venue::ALL);
        Self {
            venues: [VenueBook::new(&configs[0]), VenueBook::new(&configs[1])],
            channels,
            signal_state: SignalState::default(),
            params,
            last_decision: Decision::default(),
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
        let mut committed_this_pass: [Option<CommitTimes>; 2] = [None; 2];
        let (mut last_evaluation, mut last_telemetry, mut last_decision_log) = (0, 0, 0);
        let mut last_window_roll = timing::now();
        while !self.stop.load(Relaxed) {
            let mut committed = false;
            for (venue_index, commit_times) in committed_this_pass.iter_mut().enumerate() {
                for _ in 0..MAX_ENTRIES_PER_PASS {
                    let Ok(message) = self.channels.feeds[venue_index].pop() else { break };
                    if let Some(times) = self.handle(venue_index, message) {
                        *commit_times = Some(times);
                        committed = true;
                    }
                }
            }
            let now = timing::now();
            let state_changed = std::mem::take(&mut self.state_changed);
            if committed || state_changed || timing::nanos(last_evaluation, now) > EVALUATE_EVERY_NS {
                let previous_bias = self.last_decision.bias;
                let (binance, aster) = (self.venues[0].view(now), self.venues[1].view(now));
                self.last_decision = signal::decide(&binance, &aster, &mut self.signal_state, &self.params, now, timing::wall_ms());
                last_evaluation = now;
                let decided = timing::now();
                for (venue_index, commit_times) in committed_this_pass.iter_mut().enumerate() {
                    if let Some(times) = commit_times.take()
                        && times.frame_read != 0
                    {
                        self.venues[venue_index].record(Stage::Signal, timing::nanos(times.applied, decided));
                        self.venues[venue_index].record(Stage::EndToEnd, timing::nanos(times.frame_read, decided));
                    }
                }
                let bias_changed = self.last_decision.bias != previous_bias;
                if bias_changed || timing::nanos(last_decision_log, now) > DECISION_LOG_EVERY_NS {
                    self.channels.log.emit(LogEvent::Decision(self.last_decision));
                    last_decision_log = now;
                }
                if bias_changed {
                    last_telemetry = 0;
                }
            }
            if timing::nanos(last_window_roll, now) > TIMING_WINDOW_NS {
                self.venues.iter_mut().for_each(VenueBook::roll_window);
                last_window_roll = now;
            }
            if timing::nanos(last_telemetry, now) > TELEMETRY_EVERY_NS {
                let telemetry =
                    Telemetry { venues: [self.venues[0].telemetry(now), self.venues[1].telemetry(now)], decision: self.last_decision };
                let _ = self.channels.telemetry.push(telemetry);
                last_telemetry = now;
            }
            if !committed {
                match self.idle {
                    Idle::Spin => std::hint::spin_loop(),
                    Idle::Block => thread::sleep(Duration::from_micros(200)),
                }
            }
        }
    }

    /// Returns the commit's timestamps when a transaction committed (`frame_read` = 0 if timing was off).
    #[inline]
    fn handle(&mut self, venue_index: usize, message: FeedMessage) -> Option<CommitTimes> {
        let venue = &mut self.venues[venue_index];
        match message {
            FeedMessage::Status { state, reason } => {
                (venue.state, venue.reason, venue.state_since) = (state, reason, timing::now());
                self.state_changed = true;
                None
            }
            FeedMessage::Begin { kind, header, received_at } => {
                if kind == MessageKind::Snapshot {
                    venue.book.clear();
                    (venue.valid, venue.last_commit_was_diff) = (true, false);
                } else if venue.last_commit_was_diff && venue.valid && header.prev_update_id != venue.header.last_update_id {
                    venue.valid = false;
                    venue.engine_resyncs += 1;
                    self.channels.log.emit(LogEvent::EngineResync {
                        venue: venue.venue,
                        book_update_id: venue.header.last_update_id,
                        event_prev_update_id: header.prev_update_id,
                    });
                    let _ = self.channels.control[venue_index].push(Control::Resync);
                }
                venue.pending_levels.iter_mut().for_each(Vec::clear);
                venue.processing_transaction = true;
                venue.open_transaction = OpenTransaction { header, received_at, kind };
                None
            }
            FeedMessage::Levels { side, count, levels } => {
                venue.pending_levels[side as usize].extend_from_slice(&levels[..count as usize]);
                None
            }
            FeedMessage::Commit { stamps } => {
                let popped = timing::now();
                for (side, levels) in [Side::Bid, Side::Ask].into_iter().zip(&mut venue.pending_levels) {
                    venue.book.apply_batch(side, levels);
                    levels.clear();
                }
                let applied = timing::now();
                let transaction = venue.open_transaction;
                (venue.header, venue.received_at) = (transaction.header, transaction.received_at);
                venue.processing_transaction = false;
                venue.last_commit_was_diff = transaction.kind == MessageKind::Diff;
                venue.commits += 1;
                let best = (venue.book.best(Side::Bid), venue.book.best(Side::Ask));
                let crossed = matches!(best, (Some(bid), Some(ask)) if bid.price >= ask.price);
                if crossed
                    && !venue.crossed
                    && let (Some(bid), Some(ask)) = best
                {
                    venue.crossed_books += 1;
                    self.channels.log.emit(LogEvent::Crossed { venue: venue.venue, bid: bid.price, ask: ask.price });
                }
                venue.crossed = crossed;
                if stamps.frame_read == 0 {
                    return Some(CommitTimes { frame_read: 0, applied });
                }
                venue.record(Stage::Parse, timing::nanos(stamps.frame_read, stamps.parsed));
                venue.record(Stage::Sync, timing::nanos(stamps.parsed, stamps.sequenced));
                venue.record(Stage::Push, timing::nanos(stamps.sequenced, stamps.pushed));
                venue.record(Stage::RingHop, timing::nanos(stamps.pushed, popped));
                venue.record(Stage::Apply, timing::nanos(popped, applied));
                Some(CommitTimes { frame_read: stamps.frame_read, applied })
            }
        }
    }
}

#[derive(Clone, Copy)]
struct CommitTimes {
    frame_read: u64,
    applied: u64,
}
