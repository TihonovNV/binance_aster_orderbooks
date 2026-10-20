use serde::Serialize;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};

macro_rules! counters {
    ($($field:ident),* $(,)?) => {
        /// Written only by the venue's I/O thread; read by the HTTP thread. Own cache lines.
        #[derive(Default)]
        #[repr(align(128))]
        pub struct FeedStats { $(pub $field: AtomicU64,)* pub exchange_lag_ms: AtomicI64 }

        #[derive(Clone, Copy, Debug, Default, Serialize)]
        pub struct StatsView { $(pub $field: u64,)* pub exchange_lag_ms: i64 }

        impl FeedStats {
            pub fn view(&self) -> StatsView {
                StatsView { $($field: self.$field.load(Relaxed),)* exchange_lag_ms: self.exchange_lag_ms.load(Relaxed) }
            }
        }
    };
}

counters!(messages, bytes, reconnects, resyncs, gaps, stale_events, parse_errors, ring_overflows, snapshots, last_snapshot_ms);

#[inline]
pub fn increment(counter: &AtomicU64) {
    counter.fetch_add(1, Relaxed);
}

#[inline]
pub fn set(counter: &AtomicU64, value: u64) {
    counter.store(value, Relaxed);
}
