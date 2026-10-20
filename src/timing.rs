use quanta::Clock;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

static CLOCK: OnceLock<Clock> = OnceLock::new();
static ENABLED: AtomicBool = AtomicBool::new(false);

pub fn init(enabled: bool) {
    clock();
    set_enabled(enabled);
}

fn clock() -> &'static Clock {
    CLOCK.get_or_init(Clock::new)
}

/// Raw monotonic counter ticks (CNTVCT / TSC), comparable across threads.
#[inline]
pub fn now() -> u64 {
    clock().raw()
}

#[inline]
pub fn nanos(start: u64, end: u64) -> u64 {
    if end <= start { 0 } else { clock().delta_as_nanos(start, end) }
}

#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Relaxed)
}

pub fn set_enabled(on: bool) {
    ENABLED.store(on, Relaxed);
}

#[inline]
pub fn stamp(on: bool) -> u64 {
    if on { now() } else { 0 }
}

pub fn wall_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}
