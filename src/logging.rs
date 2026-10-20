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

const LOG_RING_CAPACITY: usize = 4096;

/// Hot-path log record: plain data, formatted later on the logger thread.
/// `book_update_id` is the last update id applied to our book; `event_*` fields come from the incoming event.
#[derive(Clone, Copy, Debug)]
pub enum LogEvent {
    State { venue: Venue, from: FeedState, to: FeedState, reason: Reason },
    Gap { venue: Venue, book_update_id: u64, event_first_update_id: u64, event_last_update_id: u64, event_prev_update_id: u64 },
    Stale { venue: Venue, event_last_update_id: u64, book_update_id: u64 },
    StaleSnapshot { venue: Venue, snapshot_update_id: u64, event_first_update_id: u64 },
    Snapshot { venue: Venue, update_id: u64, bids: u32, asks: u32, rest_ms: u32 },
    ParseError { venue: Venue, error: WireError, frame_len: u32 },
    Overflow { venue: Venue },
    Crossed { venue: Venue, bid: i64, ask: i64 },
    EngineResync { venue: Venue, book_update_id: u64, event_prev_update_id: u64 },
    Decision(Decision),
    Text(&'static str),
}

/// Producer side of one hot thread's log ring. Never blocks: a full ring drops the record.
pub struct Log {
    producer: Producer<LogEvent>,
    dropped: Arc<AtomicU64>,
}

impl Log {
    #[inline]
    pub fn emit(&mut self, event: LogEvent) {
        if self.producer.push(event).is_err() {
            self.dropped.store(self.dropped.load(Relaxed) + 1, Relaxed);
        }
    }

    pub fn dropped(&self) -> Arc<AtomicU64> {
        self.dropped.clone()
    }
}

pub fn channel() -> (Log, Consumer<LogEvent>) {
    let (producer, consumer) = RingBuffer::new(LOG_RING_CAPACITY);
    (Log { producer, dropped: Arc::default() }, consumer)
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
pub fn spawn(mut consumers: Vec<Consumer<LogEvent>>, stop: Arc<AtomicBool>) -> JoinHandle<()> {
    thread::Builder::new()
        .name("logger".into())
        .spawn(move || {
            loop {
                let stopping = stop.load(Relaxed);
                let mut wrote_any = false;
                for consumer in &mut consumers {
                    while let Ok(event) = consumer.pop() {
                        write(&event);
                        wrote_any = true;
                    }
                }
                if stopping {
                    break;
                }
                if !wrote_any {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        })
        .expect("spawn logger")
}

fn write(event: &LogEvent) {
    match *event {
        LogEvent::State { venue, from, to, reason } => match to {
            FeedState::Live | FeedState::Connecting | FeedState::Syncing => {
                info!(venue = venue.name(), ?from, ?to, ?reason, "state")
            }
            _ => warn!(venue = venue.name(), ?from, ?to, ?reason, "state"),
        },
        LogEvent::Gap { venue, book_update_id, event_first_update_id, event_last_update_id, event_prev_update_id } => warn!(
            venue = venue.name(),
            book_update_id, event_first_update_id, event_last_update_id, event_prev_update_id, "sequence gap, resyncing"
        ),
        LogEvent::Stale { venue, event_last_update_id, book_update_id } => {
            debug!(venue = venue.name(), event_last_update_id, book_update_id, "stale event skipped")
        }
        LogEvent::StaleSnapshot { venue, snapshot_update_id, event_first_update_id } => {
            warn!(venue = venue.name(), snapshot_update_id, event_first_update_id, "snapshot older than stream, refetching")
        }
        LogEvent::Snapshot { venue, update_id, bids, asks, rest_ms } => {
            info!(venue = venue.name(), update_id, bids, asks, rest_ms, "snapshot loaded")
        }
        LogEvent::ParseError { venue, error, frame_len } => warn!(venue = venue.name(), ?error, frame_len, "bad frame skipped"),
        LogEvent::Overflow { venue } => warn!(venue = venue.name(), "engine ring full, dropping and resyncing"),
        LogEvent::Crossed { venue, bid, ask } => warn!(venue = venue.name(), bid, ask, "crossed book"),
        LogEvent::EngineResync { venue, book_update_id, event_prev_update_id } => {
            warn!(venue = venue.name(), book_update_id, event_prev_update_id, "engine saw broken chain, requesting resync")
        }
        LogEvent::Decision(decision) => {
            let bias = match decision.bias {
                Bias::Hold => "HOLD",
                Bias::LongBias => "LONG_BIAS",
                Bias::ShortBias => "SHORT_BIAS",
            };
            info!(
                bias,
                reason = ?decision.reason,
                deviation_bps = format_args!("{:+.2}", decision.deviation_bps),
                gap_bps = format_args!("{:+.2}", decision.gap_bps),
                basis_ema_bps = format_args!("{:+.2}", decision.basis_ema_bps),
                aster_imbalance = format_args!("{:+.2}", decision.aster_imbalance),
                binance_microprice = format_args!("{:.3}", decision.binance_microprice),
                aster_mid = format_args!("{:.3}", decision.aster_mid),
                binance_bid_ask = format_args!("{:.2}/{:.2}", decision.binance_bid, decision.binance_ask),
                aster_bid_ask = format_args!("{:.2}/{:.2}", decision.aster_bid, decision.aster_ask),
                age_ms_binance_aster = format_args!("{:.0}/{:.0}", decision.binance_age_ms, decision.aster_age_ms),
                "signal"
            )
        }
        LogEvent::Text(text) => info!("{text}"),
    }
}
