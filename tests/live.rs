//! Network tests against the real venues: `cargo test --test live -- --ignored --nocapture`.

use orderbooks::bus::{self, FeedMsg, FeedState, Kind};
use orderbooks::config::{Idle, Venue, VenueConfig};
use orderbooks::feed::{IoFeed, rest};
use orderbooks::{logging, timing};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};

#[test]
#[ignore]
fn scales_match_exchange_info() {
    for v in Venue::ALL {
        rest::validate_scales(&VenueConfig::new(v, "ETHUSDT", Duration::from_secs(5), Idle::Block)).unwrap();
    }
}

#[test]
#[ignore]
fn both_feeds_reach_live_with_unbroken_chain() {
    timing::init(true);
    let stop = Arc::new(AtomicBool::new(false));
    let mut rings = vec![];
    let mut threads = vec![];
    for v in Venue::ALL {
        let (tx, rx) = rtrb::RingBuffer::new(bus::DATA_RING);
        let (_ctl_tx, ctl_rx) = rtrb::RingBuffer::new(bus::CONTROL_RING);
        let (log, _log_rx) = logging::channel();
        let cfg = VenueConfig::new(v, "ETHUSDT", Duration::from_secs(5), Idle::Spin);
        threads.push(IoFeed::new(cfg, tx, ctl_rx, log, Default::default(), stop.clone()).spawn());
        rings.push((v, rx, None::<FeedState>, 0u64, 0usize, 0u64));
    }
    let until = Instant::now() + Duration::from_secs(8);
    while Instant::now() < until {
        for (v, rx, state, last_u, diffs, hop_ns) in &mut rings {
            while let Ok(m) = rx.pop() {
                match m {
                    FeedMsg::Status { state: s, .. } => *state = Some(s),
                    FeedMsg::Begin { kind: Kind::Snapshot, hdr, .. } => *last_u = hdr.u,
                    FeedMsg::Begin { kind: Kind::Diff, hdr, .. } => {
                        if *diffs > 0 {
                            assert_eq!(hdr.pu, *last_u, "{v:?} chain broken");
                        }
                        *last_u = hdr.u;
                        *diffs += 1;
                    }
                    FeedMsg::Commit { stamps } if stamps.t3 != 0 => *hop_ns = timing::nanos(stamps.t3, timing::now()),
                    _ => {}
                }
            }
        }
        std::hint::spin_loop();
    }
    stop.store(true, Relaxed);
    threads.into_iter().for_each(|t| t.join().unwrap());
    for (v, _, state, _, diffs, hop_ns) in rings {
        println!("{v:?}: state={state:?} diffs={diffs} last ring hop={hop_ns}ns");
        assert!(diffs > 10, "{v:?} got only {diffs} diffs");
    }
}
