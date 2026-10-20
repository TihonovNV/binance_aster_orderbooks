//! Network tests against the real venues: `cargo test --test live -- --ignored --nocapture`.

use orderbooks::bus::{self, FeedMessage, FeedState, MessageKind};
use orderbooks::config::{Idle, Venue, VenueConfig};
use orderbooks::feed::{IoFeed, rest};
use orderbooks::{logging, timing};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};

#[test]
#[ignore]
fn scales_match_exchange_info() {
    for venue in Venue::ALL {
        rest::validate_scales(&VenueConfig::new(venue, "ETHUSDT", Duration::from_secs(5), Idle::Block)).unwrap();
    }
}

struct Observed {
    venue: Venue,
    feed: rtrb::Consumer<FeedMessage>,
    state: Option<FeedState>,
    last_update_id: u64,
    diffs: usize,
    last_ring_hop_ns: u64,
}

#[test]
#[ignore]
fn both_feeds_reach_live_with_unbroken_chain() {
    timing::init(true);
    let stop = Arc::new(AtomicBool::new(false));
    let mut observed = vec![];
    let mut threads = vec![];
    for venue in Venue::ALL {
        let (feed_tx, feed) = rtrb::RingBuffer::new(bus::DATA_RING_CAPACITY);
        let (_control_tx, control_rx) = rtrb::RingBuffer::new(bus::CONTROL_RING_CAPACITY);
        let (log, _log_rx) = logging::channel();
        let config = VenueConfig::new(venue, "ETHUSDT", Duration::from_secs(5), Idle::Spin);
        threads.push(IoFeed::new(config, feed_tx, control_rx, log, Default::default(), stop.clone()).spawn());
        observed.push(Observed { venue, feed, state: None, last_update_id: 0, diffs: 0, last_ring_hop_ns: 0 });
    }
    let until = Instant::now() + Duration::from_secs(8);
    while Instant::now() < until {
        for o in &mut observed {
            while let Ok(message) = o.feed.pop() {
                match message {
                    FeedMessage::Status { state, .. } => o.state = Some(state),
                    FeedMessage::Begin { kind: MessageKind::Snapshot, header, .. } => o.last_update_id = header.last_update_id,
                    FeedMessage::Begin { kind: MessageKind::Diff, header, .. } => {
                        if o.diffs > 0 {
                            assert_eq!(header.prev_update_id, o.last_update_id, "{:?} chain broken", o.venue);
                        }
                        o.last_update_id = header.last_update_id;
                        o.diffs += 1;
                    }
                    FeedMessage::Commit { stamps } if stamps.pushed != 0 => {
                        o.last_ring_hop_ns = timing::nanos(stamps.pushed, timing::now())
                    }
                    _ => {}
                }
            }
        }
        std::hint::spin_loop();
    }
    stop.store(true, Relaxed);
    threads.into_iter().for_each(|thread| thread.join().unwrap());
    for o in observed {
        println!("{:?}: state={:?} diffs={} last ring hop={}ns", o.venue, o.state, o.diffs, o.last_ring_hop_ns);
        assert!(o.diffs > 10, "{:?} got only {} diffs", o.venue, o.diffs);
    }
}
