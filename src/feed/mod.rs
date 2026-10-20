pub mod rest;
pub mod stats;
pub mod sync;
pub mod wire;
pub mod ws;

use crate::bus::{self, Control, FeedMessage, FeedState, IoStamps, MessageKind, Reason};
use crate::config::{Idle, VenueConfig};
use crate::logging::{Log, LogEvent};
use crate::{rt, timing};
use rest::{Rest, RestError};
use rtrb::{Consumer, Producer};
use stats::{FeedStats, increment, set};
use std::io::ErrorKind;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use sync::{Action, Sequencer};
use tracing::{error, info, warn};
use tungstenite::Message;
use wire::{Depth, Scales};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REST_TIMEOUT: Duration = Duration::from_secs(10);
const ROTATE_AFTER: Duration = Duration::from_secs(23 * 3600);
const STABLE_AFTER: Duration = Duration::from_secs(60);
const RESYNC_FREE_ENTRIES: usize = 128;
const MAX_STALE_SNAPSHOTS: u32 = 3;
const IDLE_SPINS_PER_CHECK: u32 = 4096;

/// One venue's I/O thread: socket, parsing, sequencing and recovery. Never touches a book.
pub struct IoFeed {
    config: VenueConfig,
    scales: Scales,
    engine_tx: Producer<FeedMessage>,
    control_rx: Consumer<Control>,
    log: Log,
    stats: Arc<FeedStats>,
    stop: Arc<AtomicBool>,
    rest: Rest,
    sequencer: Sequencer,
    parsed: Depth,
    state: FeedState,
    unsent_status: Option<Reason>,
    stale_snapshots: u32,
    snapshot_update_id: u64,
    retry_after: Option<Duration>,
}

impl IoFeed {
    pub fn new(
        config: VenueConfig,
        engine_tx: Producer<FeedMessage>,
        control_rx: Consumer<Control>,
        log: Log,
        stats: Arc<FeedStats>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        let scales = Scales { price: config.price_scale, quantity: config.quantity_scale };
        Self {
            config,
            scales,
            engine_tx,
            control_rx,
            log,
            stats,
            stop,
            rest: Rest::new(REST_TIMEOUT),
            sequencer: Sequencer::default(),
            parsed: Depth::default(),
            state: FeedState::Disconnected,
            unsent_status: None,
            stale_snapshots: 0,
            snapshot_update_id: 0,
            retry_after: None,
        }
    }

    pub fn spawn(self) -> JoinHandle<()> {
        thread::Builder::new()
            .name(format!("io-{}", self.config.venue.name()))
            .spawn(move || {
                rt::hot_thread();
                self.run()
            })
            .expect("spawn io thread")
    }

    fn run(mut self) {
        let venue = self.config.venue.name();
        let mut backoff = rt::Backoff::default();
        let mut reason = Reason::Start;
        while !self.stop.load(Relaxed) {
            self.set_state(FeedState::Connecting, reason);
            let session_started = Instant::now();
            reason = catch_unwind(AssertUnwindSafe(|| self.session())).unwrap_or_else(|_| {
                error!(venue, "io thread panicked, restarting session");
                Reason::Panic
            });
            self.sequencer.reset();
            self.set_state(FeedState::Disconnected, reason);
            if reason == Reason::Shutdown {
                break;
            }
            increment(&self.stats.reconnects);
            if session_started.elapsed() > STABLE_AFTER {
                backoff.reset();
            }
            let delay = backoff.delay().max(self.retry_after.take().unwrap_or_default());
            warn!(venue, ?reason, delay_ms = delay.as_millis() as u64, "disconnected, backing off");
            if !rt::wait(delay, self.config.idle, &self.stop) {
                break;
            }
        }
        self.set_state(FeedState::Disconnected, Reason::Shutdown);
    }

    fn session(&mut self) -> Reason {
        let venue = self.config.venue.name();
        let spin = self.config.idle == Idle::Spin;
        let connect_started = Instant::now();
        let mut ws = match ws::connect(&self.config.ws_url, CONNECT_TIMEOUT, spin) {
            Ok(ws) => ws,
            Err(e) => {
                warn!(venue, error = %e, "connect failed");
                return Reason::Io;
            }
        };
        info!(venue, ms = connect_started.elapsed().as_millis() as u64, url = %self.config.ws_url, "connected");
        let connected_at = Instant::now();
        let watchdog_ns = self.config.watchdog.as_nanos() as u64;
        let mut last_frame_at = timing::now();
        let mut pending_resync = Some(Reason::Connected);
        let mut idle_spins = 0u32;
        loop {
            if self.stop.load(Relaxed) {
                let _ = ws.close(None);
                return Reason::Shutdown;
            }
            if let Ok(Control::Resync) = self.control_rx.pop() {
                self.sequencer.reset();
                pending_resync = Some(Reason::EngineRequest);
            }
            if let Some(reason) = pending_resync
                && self.engine_tx.slots() >= RESYNC_FREE_ENTRIES
            {
                if let Err(reason) = self.resync(reason) {
                    return reason;
                }
                pending_resync = None;
            }
            match ws.read() {
                Ok(Message::Text(text)) => {
                    last_frame_at = timing::now();
                    if let Some(reason) = self.on_frame(text.as_str(), last_frame_at) {
                        self.sequencer.reset();
                        pending_resync = Some(reason);
                    }
                }
                Ok(Message::Close(_)) => return Reason::Closed,
                Ok(_) => last_frame_at = timing::now(),
                Err(tungstenite::Error::Io(e)) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                    idle_spins = idle_spins.wrapping_add(1);
                    if !spin || idle_spins.is_multiple_of(IDLE_SPINS_PER_CHECK) {
                        if timing::nanos(last_frame_at, timing::now()) > watchdog_ns {
                            return Reason::Watchdog;
                        }
                        if connected_at.elapsed() > ROTATE_AFTER {
                            let _ = ws.close(None);
                            return Reason::Rotation;
                        }
                        let _ = ws.flush();
                    }
                    if spin {
                        std::hint::spin_loop();
                    }
                }
                Err(e) => {
                    warn!(venue, error = %e, "ws error");
                    return Reason::Io;
                }
            }
        }
    }

    /// Hot path. Returns a resync reason if the stream can no longer be applied.
    fn on_frame(&mut self, text: &str, received_at: u64) -> Option<Reason> {
        let timing_on = timing::enabled();
        let frame_read = if timing_on { received_at } else { 0 };
        let venue = self.config.venue;
        increment(&self.stats.messages);
        set(&self.stats.bytes, self.stats.bytes.load(Relaxed) + text.len() as u64);
        if let Err(error) = self.parsed.parse(text, self.scales) {
            increment(&self.stats.parse_errors);
            self.log.emit(LogEvent::ParseError { venue, error, frame_len: text.len() as u32 });
            return None;
        }
        let parsed = timing::stamp_if(timing_on);
        self.stats.exchange_lag_ms.store(timing::wall_ms() as i64 - self.parsed.header.event_time_ms as i64, Relaxed);
        let book_update_id = self.sequencer.last_update_id();
        let action = self.sequencer.on_event(&self.parsed.header);
        let sequenced = timing::stamp_if(timing_on);
        let event = self.parsed.header;
        match action {
            Action::Apply => {
                if !self.send(MessageKind::Diff, received_at, IoStamps { frame_read, parsed, sequenced, pushed: 0 }) {
                    increment(&self.stats.ring_overflows);
                    self.log.emit(LogEvent::Overflow { venue });
                    return Some(Reason::Overflow);
                }
                if book_update_id.is_none() {
                    self.stale_snapshots = 0;
                    self.set_state(FeedState::Live, Reason::Bridged);
                }
                None
            }
            Action::Skip => {
                if let Some(book_update_id) = book_update_id {
                    increment(&self.stats.stale_events);
                    self.log.emit(LogEvent::Stale { venue, event_last_update_id: event.last_update_id, book_update_id });
                }
                None
            }
            Action::Resync => {
                increment(&self.stats.gaps);
                self.log.emit(LogEvent::Gap {
                    venue,
                    book_update_id: book_update_id.unwrap_or(0),
                    event_first_update_id: event.first_update_id,
                    event_last_update_id: event.last_update_id,
                    event_prev_update_id: event.prev_update_id,
                });
                Some(Reason::Gap)
            }
            Action::StaleSnapshot => {
                self.stale_snapshots += 1;
                self.log.emit(LogEvent::StaleSnapshot {
                    venue,
                    snapshot_update_id: self.snapshot_update_id,
                    event_first_update_id: event.first_update_id,
                });
                Some(Reason::StaleSnapshot)
            }
        }
    }

    fn resync(&mut self, reason: Reason) -> Result<(), Reason> {
        let venue = self.config.venue;
        if self.stale_snapshots > MAX_STALE_SNAPSHOTS {
            self.stale_snapshots = 0;
            return Err(Reason::StaleSnapshot);
        }
        let state = if reason == Reason::Connected { FeedState::Syncing } else { FeedState::Resyncing };
        self.set_state(state, reason);
        if reason != Reason::Connected {
            increment(&self.stats.resyncs);
        }
        self.sequencer.reset();
        let request_started = Instant::now();
        let body = self.rest.get(&self.config.rest_url).map_err(|e| {
            match e {
                RestError::RateLimited(retry_after) => {
                    warn!(venue = venue.name(), retry_after_s = retry_after.as_secs(), "snapshot rate limited");
                    self.retry_after = Some(retry_after);
                }
                RestError::Other(e) => warn!(venue = venue.name(), error = %e, "snapshot fetch failed"),
            }
            Reason::Rest
        })?;
        let rest_ms = request_started.elapsed().as_millis() as u64;
        if let Err(error) = self.parsed.parse(&body, self.scales) {
            warn!(venue = venue.name(), ?error, "snapshot parse failed");
            return Err(Reason::Rest);
        }
        if !self.send(MessageKind::Snapshot, timing::now(), IoStamps::default()) {
            increment(&self.stats.ring_overflows);
            return Err(Reason::Overflow);
        }
        self.snapshot_update_id = self.parsed.header.last_update_id;
        self.sequencer.on_snapshot(self.snapshot_update_id);
        increment(&self.stats.snapshots);
        set(&self.stats.last_snapshot_ms, rest_ms);
        self.log.emit(LogEvent::Snapshot {
            venue,
            update_id: self.snapshot_update_id,
            bids: self.parsed.bids.len() as u32,
            asks: self.parsed.asks.len() as u32,
            rest_ms: rest_ms as u32,
        });
        Ok(())
    }

    /// Keeps `Status` ordered before data: no data goes out while a status is still unsent.
    fn send(&mut self, kind: MessageKind, received_at: u64, stamps: IoStamps) -> bool {
        self.flush_status();
        self.unsent_status.is_none() && bus::push_depth(&mut self.engine_tx, kind, &self.parsed, received_at, stamps)
    }

    fn set_state(&mut self, to: FeedState, reason: Reason) {
        if to == self.state {
            return;
        }
        self.log.emit(LogEvent::State { venue: self.config.venue, from: self.state, to, reason });
        self.state = to;
        self.unsent_status = Some(reason);
        self.flush_status();
    }

    fn flush_status(&mut self) {
        if let Some(reason) = self.unsent_status
            && self.engine_tx.push(FeedMessage::Status { state: self.state, reason }).is_ok()
        {
            self.unsent_status = None;
        }
    }
}
