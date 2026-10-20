use crate::bus::{FeedState, Reason};
use crate::config::Venue;
use crate::feed::wire::WireError;
use crate::signal::{Bias, Decision};
use rtrb::{Consumer, Producer, RingBuffer};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tracing::{debug, info, warn};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

const LOG_RING: usize = 4096;

/// Hot-path log record: plain data, formatted later on the logger thread.
#[derive(Clone, Copy, Debug)]
pub enum LogEvent {
    State { venue: Venue, from: FeedState, to: FeedState, reason: Reason },
    Gap { venue: Venue, last_u: u64, first_u: u64, u: u64, pu: u64 },
    Stale { venue: Venue, u: u64, last_u: u64 },
    StaleSnapshot { venue: Venue, snap_u: u64, first_u: u64 },
    Snapshot { venue: Venue, u: u64, bids: u32, asks: u32, rest_ms: u32 },
    ParseError { venue: Venue, err: WireError, len: u32 },
    Overflow { venue: Venue },
    Crossed { venue: Venue, bid: i64, ask: i64 },
    EngineResync { venue: Venue, last_u: u64, pu: u64 },
    Decision(Decision),
    Text(&'static str),
}

pub struct Log {
    tx: Producer<LogEvent>,
    dropped: Arc<AtomicU64>,
}

impl Log {
    #[inline]
    pub fn emit(&mut self, e: LogEvent) {
        if self.tx.push(e).is_err() {
            self.dropped.store(self.dropped.load(Relaxed) + 1, Relaxed);
        }
    }

    pub fn dropped(&self) -> Arc<AtomicU64> {
        self.dropped.clone()
    }
}

pub fn channel() -> (Log, Consumer<LogEvent>) {
    let (tx, rx) = RingBuffer::new(LOG_RING);
    (Log { tx, dropped: Arc::default() }, rx)
}

pub fn init(dir: &Path) {
    let file = tracing_appender::rolling::daily(dir, "orderbooks.log");
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer().with_target(false).with_thread_names(true))
        .with(fmt::layer().with_ansi(false).with_target(false).with_thread_names(true).with_writer(file))
        .init();
}

/// Drains every hot thread's ring. Producers never wait on it, so it may sleep.
pub fn spawn(mut rxs: Vec<Consumer<LogEvent>>, stop: Arc<AtomicBool>) -> JoinHandle<()> {
    thread::Builder::new()
        .name("logger".into())
        .spawn(move || {
            loop {
                let stopping = stop.load(Relaxed);
                let mut any = false;
                for rx in &mut rxs {
                    while let Ok(e) = rx.pop() {
                        write(&e);
                        any = true;
                    }
                }
                if stopping {
                    break;
                }
                if !any {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        })
        .expect("spawn logger")
}

fn write(e: &LogEvent) {
    match *e {
        LogEvent::State { venue, from, to, reason } => match to {
            FeedState::Live | FeedState::Connecting | FeedState::Syncing => {
                info!(venue = venue.name(), ?from, ?to, ?reason, "state")
            }
            _ => warn!(venue = venue.name(), ?from, ?to, ?reason, "state"),
        },
        LogEvent::Gap { venue, last_u, first_u, u, pu } => {
            warn!(venue = venue.name(), last_u, first_u, u, pu, "sequence gap, resyncing")
        }
        LogEvent::Stale { venue, u, last_u } => debug!(venue = venue.name(), u, last_u, "stale event skipped"),
        LogEvent::StaleSnapshot { venue, snap_u, first_u } => {
            warn!(venue = venue.name(), snap_u, first_u, "snapshot older than stream, refetching")
        }
        LogEvent::Snapshot { venue, u, bids, asks, rest_ms } => {
            info!(venue = venue.name(), u, bids, asks, rest_ms, "snapshot loaded")
        }
        LogEvent::ParseError { venue, err, len } => warn!(venue = venue.name(), ?err, len, "bad frame skipped"),
        LogEvent::Overflow { venue } => warn!(venue = venue.name(), "engine ring full, dropping and resyncing"),
        LogEvent::Crossed { venue, bid, ask } => warn!(venue = venue.name(), bid, ask, "crossed book"),
        LogEvent::EngineResync { venue, last_u, pu } => {
            warn!(venue = venue.name(), last_u, pu, "engine saw broken chain, requesting resync")
        }
        LogEvent::Decision(d) => {
            let bias = match d.bias {
                Bias::Hold => "HOLD",
                Bias::LongBias => "LONG_BIAS",
                Bias::ShortBias => "SHORT_BIAS",
            };
            info!(
                bias,
                why = ?d.why,
                dev_bps = format_args!("{:+.2}", d.dev_bps),
                gap_bps = format_args!("{:+.2}", d.gap_bps),
                ema_bps = format_args!("{:+.2}", d.ema_bps),
                imb_a = format_args!("{:+.2}", d.imb_a),
                micro_b = format_args!("{:.3}", d.micro_b),
                mid_a = format_args!("{:.3}", d.mid_a),
                bbo_b = format_args!("{:.2}/{:.2}", d.bid_b, d.ask_b),
                bbo_a = format_args!("{:.2}/{:.2}", d.bid_a, d.ask_a),
                age_ms = format_args!("{:.0}/{:.0}", d.age_b_ms, d.age_a_ms),
                "signal"
            )
        }
        LogEvent::Text(s) => info!("{s}"),
    }
}
