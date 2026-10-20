use serde::Serialize;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};

macro_rules! counters {
    ($($f:ident),* $(,)?) => {
        /// Written only by the venue's I/O thread; read by the HTTP thread. Own cache lines.
        #[derive(Default)]
        #[repr(align(128))]
        pub struct FeedStats { $(pub $f: AtomicU64,)* pub lag_ms: AtomicI64 }

        #[derive(Clone, Copy, Debug, Default, Serialize)]
        pub struct StatsView { $(pub $f: u64,)* pub lag_ms: i64 }

        impl FeedStats {
            pub fn view(&self) -> StatsView {
                StatsView { $($f: self.$f.load(Relaxed),)* lag_ms: self.lag_ms.load(Relaxed) }
            }
        }
    };
}

counters!(msgs, bytes, reconnects, resyncs, gaps, stale_events, parse_errors, ring_overflows, snapshots, rest_ms);

#[inline]
pub fn inc(c: &AtomicU64) {
    c.fetch_add(1, Relaxed);
}

#[inline]
pub fn set(c: &AtomicU64, v: u64) {
    c.store(v, Relaxed);
}
