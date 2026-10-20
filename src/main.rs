use anyhow::Context;
use orderbooks::book::{BTreeBook, VecBook};
use orderbooks::bus::{CONTROL_RING, DATA_RING};
use orderbooks::config::{BookKind, Cli, Cmd, RunArgs, Venue, VenueConfig};
use orderbooks::engine::{Engine, EngineIo, TELEMETRY_RING};
use orderbooks::feed::stats::FeedStats;
use orderbooks::feed::{IoFeed, rest};
use orderbooks::http::{self, HttpCtx};
use orderbooks::{logging, timing};
use clap::Parser;
use rtrb::RingBuffer;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::Duration;
use tracing::info;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Some(Cmd::Run(args)) => run(args),
        Some(Cmd::Record(args)) => orderbooks::recorder::record(args),
        None => run(cli.run),
    }
}

fn two<T>(v: Vec<T>) -> [T; 2] {
    v.try_into().ok().expect("two venues")
}

fn run(a: RunArgs) -> anyhow::Result<()> {
    std::fs::create_dir_all(&a.log_dir)?;
    logging::init(&a.log_dir);
    timing::init(a.timing);
    let cfgs = Venue::ALL.map(|v| VenueConfig::new(v, &a.symbol, Duration::from_millis(a.watchdog_ms), a.idle));
    for c in &cfgs {
        rest::validate_scales(c).with_context(|| format!("{} exchangeInfo", c.venue.name()))?;
        info!(venue = c.venue.name(), ws = %c.ws_url, rest = %c.rest_url, "venue");
    }
    info!(book = ?a.book, idle = ?a.idle, timing = a.timing, signal = ?a.signal, "starting");

    let stop = Arc::new(AtomicBool::new(false));
    let s = stop.clone();
    ctrlc::set_handler(move || s.store(true, Relaxed))?;

    let (mut threads, mut rxs, mut ctls, mut stats, mut log_rxs, mut dropped) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for c in &cfgs {
        let (tx, rx) = RingBuffer::new(DATA_RING);
        let (ctl_tx, ctl_rx) = RingBuffer::new(CONTROL_RING);
        let (log, log_rx) = logging::channel();
        let st = Arc::new(FeedStats::default());
        dropped.push((c.venue.name(), log.dropped()));
        threads.push(IoFeed::new(c.clone(), tx, ctl_rx, log, st.clone(), stop.clone()).spawn());
        rxs.push(rx);
        ctls.push(ctl_tx);
        stats.push(st);
        log_rxs.push(log_rx);
    }
    let (tele_tx, tele_rx) = RingBuffer::new(TELEMETRY_RING);
    let (log, log_rx) = logging::channel();
    dropped.push(("engine", log.dropped()));
    log_rxs.push(log_rx);
    let io = EngineIo { rx: two(rxs), ctl: two(ctls), tele: tele_tx, log };
    threads.push(match a.book {
        BookKind::Vec => Engine::<VecBook>::new(&cfgs, io, a.signal, a.idle, stop.clone()).spawn(),
        BookKind::Btree => Engine::<BTreeBook>::new(&cfgs, io, a.signal, a.idle, stop.clone()).spawn(),
    });

    let log_stop = Arc::new(AtomicBool::new(false));
    let logger = logging::spawn(log_rxs, log_stop.clone());
    let http = http::spawn(HttpCtx { addr: a.http, tele: tele_rx, cfgs, stats: two(stats), log_dropped: dropped, stop: stop.clone() });

    for t in threads {
        let _ = t.join();
    }
    let _ = http.join();
    log_stop.store(true, Relaxed);
    let _ = logger.join();
    info!("stopped");
    Ok(())
}
