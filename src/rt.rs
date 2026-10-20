use crate::config::Idle;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};

/// Best-effort placement for spinning threads: P-cores on macOS (no hard affinity there).
pub fn hot_thread() {
    #[cfg(target_os = "macos")]
    {
        const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21;
        unsafe extern "C" {
            fn pthread_set_qos_class_self_np(qos: u32, relpri: i32) -> i32;
        }
        unsafe { pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0) };
    }
}

/// Waits without sleeping in spin mode; returns false if shutdown was requested.
pub fn wait(d: Duration, idle: Idle, stop: &AtomicBool) -> bool {
    let deadline = Instant::now() + d;
    while Instant::now() < deadline {
        if stop.load(Relaxed) {
            return false;
        }
        match idle {
            Idle::Spin => std::hint::spin_loop(),
            Idle::Block => std::thread::sleep(Duration::from_millis(10).min(deadline - Instant::now())),
        }
    }
    !stop.load(Relaxed)
}

#[derive(Default)]
pub struct Backoff(u32);

impl Backoff {
    /// Exponential 200 ms → 30 s with equal jitter.
    pub fn delay(&mut self) -> Duration {
        let cap = (200u64 << self.0).min(30_000);
        self.0 = (self.0 + 1).min(8);
        Duration::from_millis(fastrand::u64(cap / 2..=cap))
    }

    pub fn reset(&mut self) {
        self.0 = 0;
    }
}
