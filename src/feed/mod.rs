pub mod rest;
pub mod stats;
pub mod sync;
pub mod wire;
pub mod ws;

use crate::bus::{self, Control, FeedMsg, FeedState, IoStamps, Kind, Reason};
use crate::config::{Idle, VenueConfig};
use crate::logging::{Log, LogEvent};
use crate::{rt, timing};
use rest::{Rest, RestError};
use rtrb::{Consumer, Producer};
use stats::{FeedStats, inc, set};
use std::io::ErrorKind;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use sync::{Action, Sync};
use tracing::{error, info, warn};
use tungstenite::Message;
use wire::{Depth, Scales};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REST_TIMEOUT: Duration = Duration::from_secs(10);
const ROTATE_AFTER: Duration = Duration::from_secs(23 * 3600);
const STABLE_AFTER: Duration = Duration::from_secs(60);
const RESYNC_SLOTS: usize = 128;
const MAX_STALE_SNAPSHOTS: u32 = 3;

/// One venue's I/O thread: socket, parsing, sequencing and recovery. Never touches a book.
pub struct IoFeed {
    cfg: VenueConfig,
    sc: Scales,
    tx: Producer<FeedMsg>,
    ctl: Consumer<Control>,
    log: Log,
    stats: Arc<FeedStats>,
    stop: Arc<AtomicBool>,
    rest: Rest,
    sync: Sync,
    depth: Depth,
    state: FeedState,
    unsent_status: Option<Reason>,
    stale_snapshots: u32,
    snap_u: u64,
    retry_after: Option<Duration>,
}

impl IoFeed {
    pub fn new(
        cfg: VenueConfig,
        tx: Producer<FeedMsg>,
        ctl: Consumer<Control>,
        log: Log,
        stats: Arc<FeedStats>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        let sc = Scales { px: cfg.px_scale, qty: cfg.qty_scale };
        Self {
            cfg,
            sc,
            tx,
            ctl,
            log,
            stats,
            stop,
            rest: Rest::new(REST_TIMEOUT),
            sync: Sync::default(),
            depth: Depth::default(),
            state: FeedState::Disconnected,
            unsent_status: None,
            stale_snapshots: 0,
            snap_u: 0,
            retry_after: None,
        }
    }

    pub fn spawn(self) -> JoinHandle<()> {
        thread::Builder::new()
            .name(format!("io-{}", self.cfg.venue.name()))
            .spawn(move || {
                rt::hot_thread();
                self.run()
            })
            .expect("spawn io thread")
    }

    fn run(mut self) {
        let venue = self.cfg.venue.name();
        let mut backoff = rt::Backoff::default();
        let mut reason = Reason::Start;
        while !self.stop.load(Relaxed) {
            self.set_state(FeedState::Connecting, reason);
            let started = Instant::now();
            reason = catch_unwind(AssertUnwindSafe(|| self.session())).unwrap_or_else(|_| {
                error!(venue, "io thread panicked, restarting session");
                Reason::Panic
            });
            self.sync.reset();
            self.set_state(FeedState::Disconnected, reason);
            if reason == Reason::Shutdown {
                break;
            }
            inc(&self.stats.reconnects);
            if started.elapsed() > STABLE_AFTER {
                backoff.reset();
            }
            let delay = backoff.delay().max(self.retry_after.take().unwrap_or_default());
            warn!(venue, ?reason, delay_ms = delay.as_millis() as u64, "disconnected, backing off");
            if !rt::wait(delay, self.cfg.idle, &self.stop) {
                break;
            }
        }
        self.set_state(FeedState::Disconnected, Reason::Shutdown);
    }

    fn session(&mut self) -> Reason {
        let venue = self.cfg.venue.name();
        let spin = self.cfg.idle == Idle::Spin;
        let t = Instant::now();
        let mut ws = match ws::connect(&self.cfg.ws_url, CONNECT_TIMEOUT, spin) {
            Ok(ws) => ws,
            Err(e) => {
                warn!(venue, error = %e, "connect failed");
                return Reason::Io;
            }
        };
        info!(venue, ms = t.elapsed().as_millis() as u64, url = %self.cfg.ws_url, "connected");
        let born = Instant::now();
        let watchdog_ns = self.cfg.watchdog.as_nanos() as u64;
        let mut last_frame = timing::now();
        let mut pending = Some(Reason::Connected);
        let mut idle_spins = 0u32;
        loop {
            if self.stop.load(Relaxed) {
                let _ = ws.close(None);
                return Reason::Shutdown;
            }
            if let Ok(Control::Resync) = self.ctl.pop() {
                self.sync.reset();
                pending = Some(Reason::EngineRequest);
            }
            if let Some(r) = pending
                && self.tx.slots() >= RESYNC_SLOTS
            {
                if let Err(r) = self.resync(r) {
                    return r;
                }
                pending = None;
            }
            match ws.read() {
                Ok(Message::Text(txt)) => {
                    last_frame = timing::now();
                    if let Some(r) = self.on_frame(txt.as_str(), last_frame) {
                        self.sync.reset();
                        pending = Some(r);
                    }
                }
                Ok(Message::Close(_)) => return Reason::Closed,
                Ok(_) => last_frame = timing::now(),
                Err(tungstenite::Error::Io(e)) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                    idle_spins = idle_spins.wrapping_add(1);
                    if !spin || idle_spins.is_multiple_of(4096) {
                        if timing::nanos(last_frame, timing::now()) > watchdog_ns {
                            return Reason::Watchdog;
                        }
                        if born.elapsed() > ROTATE_AFTER {
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
    fn on_frame(&mut self, txt: &str, recv: u64) -> Option<Reason> {
        let on = timing::enabled();
        let t0 = if on { recv } else { 0 };
        let venue = self.cfg.venue;
        inc(&self.stats.msgs);
        set(&self.stats.bytes, self.stats.bytes.load(Relaxed) + txt.len() as u64);
        if let Err(err) = self.depth.parse(txt, self.sc) {
            inc(&self.stats.parse_errors);
            self.log.emit(LogEvent::ParseError { venue, err, len: txt.len() as u32 });
            return None;
        }
        let t1 = timing::stamp(on);
        self.stats.lag_ms.store(timing::wall_ms() as i64 - self.depth.hdr.event_ms as i64, Relaxed);
        let was_live = self.sync.last_u();
        let action = self.sync.on_event(&self.depth.hdr);
        let t2 = timing::stamp(on);
        let h = self.depth.hdr;
        match action {
            Action::Apply => {
                if !self.send(Kind::Diff, recv, IoStamps { t0, t1, t2, t3: 0 }) {
                    inc(&self.stats.ring_overflows);
                    self.log.emit(LogEvent::Overflow { venue });
                    return Some(Reason::Overflow);
                }
                if was_live.is_none() {
                    self.stale_snapshots = 0;
                    self.set_state(FeedState::Live, Reason::Bridged);
                }
                None
            }
            Action::Skip => {
                if let Some(last_u) = was_live {
                    inc(&self.stats.stale_events);
                    self.log.emit(LogEvent::Stale { venue, u: h.u, last_u });
                }
                None
            }
            Action::Resync => {
                inc(&self.stats.gaps);
                self.log.emit(LogEvent::Gap { venue, last_u: was_live.unwrap_or(0), first_u: h.first_u, u: h.u, pu: h.pu });
                Some(Reason::Gap)
            }
            Action::StaleSnapshot => {
                self.stale_snapshots += 1;
                self.log.emit(LogEvent::StaleSnapshot { venue, snap_u: self.snap_u, first_u: h.first_u });
                Some(Reason::StaleSnapshot)
            }
        }
    }

    fn resync(&mut self, reason: Reason) -> Result<(), Reason> {
        let venue = self.cfg.venue;
        if self.stale_snapshots > MAX_STALE_SNAPSHOTS {
            self.stale_snapshots = 0;
            return Err(Reason::StaleSnapshot);
        }
        let state = if reason == Reason::Connected { FeedState::Syncing } else { FeedState::Resyncing };
        self.set_state(state, reason);
        if reason != Reason::Connected {
            inc(&self.stats.resyncs);
        }
        self.sync.reset();
        let t = Instant::now();
        let body = self.rest.get(&self.cfg.rest_url).map_err(|e| {
            match e {
                RestError::RateLimited(d) => {
                    warn!(venue = venue.name(), retry_after_s = d.as_secs(), "snapshot rate limited");
                    self.retry_after = Some(d);
                }
                RestError::Other(e) => warn!(venue = venue.name(), error = %e, "snapshot fetch failed"),
            }
            Reason::Rest
        })?;
        let rest_ms = t.elapsed().as_millis() as u64;
        if let Err(err) = self.depth.parse(&body, self.sc) {
            warn!(venue = venue.name(), ?err, "snapshot parse failed");
            return Err(Reason::Rest);
        }
        if !self.send(Kind::Snapshot, timing::now(), IoStamps::default()) {
            inc(&self.stats.ring_overflows);
            return Err(Reason::Overflow);
        }
        self.snap_u = self.depth.hdr.u;
        self.sync.on_snapshot(self.snap_u);
        inc(&self.stats.snapshots);
        set(&self.stats.rest_ms, rest_ms);
        let d = &self.depth;
        self.log.emit(LogEvent::Snapshot {
            venue,
            u: d.hdr.u,
            bids: d.bids.len() as u32,
            asks: d.asks.len() as u32,
            rest_ms: rest_ms as u32,
        });
        Ok(())
    }

    /// Keeps `Status` ordered before data: no data goes out while a status is still unsent.
    fn send(&mut self, kind: Kind, recv: u64, stamps: IoStamps) -> bool {
        self.flush_status();
        self.unsent_status.is_none() && bus::push_depth(&mut self.tx, kind, &self.depth, recv, stamps)
    }

    fn set_state(&mut self, to: FeedState, reason: Reason) {
        if to == self.state {
            return;
        }
        self.log.emit(LogEvent::State { venue: self.cfg.venue, from: self.state, to, reason });
        self.state = to;
        self.unsent_status = Some(reason);
        self.flush_status();
    }

    fn flush_status(&mut self) {
        if let Some(reason) = self.unsent_status
            && self.tx.push(FeedMsg::Status { state: self.state, reason }).is_ok()
        {
            self.unsent_status = None;
        }
    }
}
