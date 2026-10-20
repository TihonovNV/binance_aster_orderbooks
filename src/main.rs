use anyhow::Context;
use clap::Parser;
use orderbooks::book::{BTreeBook, VecBook};
use orderbooks::bus::{CONTROL_RING_CAPACITY, DATA_RING_CAPACITY};
use orderbooks::config::{BookKind, Cli, Command, RunArgs, Venue, VenueConfig};
use orderbooks::engine::{Engine, EngineChannels, TELEMETRY_RING_CAPACITY};
use orderbooks::feed::stats::FeedStats;
use orderbooks::feed::{IoFeed, rest};
use orderbooks::http::{self, HttpContext};
use orderbooks::{logging, timing};
use rtrb::RingBuffer;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::Duration;
use tracing::info;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Run(args)) => run(args),
        Some(Command::Record(args)) => orderbooks::recorder::record(args),
        None => run(cli.run),
    }
}

fn into_pair<T>(items: Vec<T>) -> [T; 2] {
    items.try_into().ok().expect("two venues")
}

fn run(args: RunArgs) -> anyhow::Result<()> {
    std::fs::create_dir_all(&args.log_dir)?;
    logging::init(&args.log_dir);
    timing::init(args.timing);
    let configs = Venue::ALL.map(|venue| VenueConfig::new(venue, &args.symbol, Duration::from_millis(args.watchdog_ms), args.idle));
    for config in &configs {
        rest::validate_scales(config).with_context(|| format!("{} exchangeInfo", config.venue.name()))?;
        info!(venue = config.venue.name(), ws = %config.ws_url, rest = %config.rest_url, "venue");
    }
    info!(book = ?args.book, idle = ?args.idle, timing = args.timing, signal = ?args.signal, "starting");

    let stop = Arc::new(AtomicBool::new(false));
    let stop_on_ctrl_c = stop.clone();
    ctrlc::set_handler(move || stop_on_ctrl_c.store(true, Relaxed))?;

    let mut threads = vec![];
    let (mut feed_consumers, mut control_producers, mut feed_stats, mut log_consumers, mut log_dropped) =
        (vec![], vec![], vec![], vec![], vec![]);
    for config in &configs {
        let (feed_producer, feed_consumer) = RingBuffer::new(DATA_RING_CAPACITY);
        let (control_producer, control_consumer) = RingBuffer::new(CONTROL_RING_CAPACITY);
        let (log, log_consumer) = logging::channel();
        let stats = Arc::new(FeedStats::default());
        log_dropped.push((config.venue.name(), log.dropped()));
        threads.push(IoFeed::new(config.clone(), feed_producer, control_consumer, log, stats.clone(), stop.clone()).spawn());
        feed_consumers.push(feed_consumer);
        control_producers.push(control_producer);
        feed_stats.push(stats);
        log_consumers.push(log_consumer);
    }
    let (telemetry_producer, telemetry_consumer) = RingBuffer::new(TELEMETRY_RING_CAPACITY);
    let (engine_log, engine_log_consumer) = logging::channel();
    log_dropped.push(("engine", engine_log.dropped()));
    log_consumers.push(engine_log_consumer);
    let channels = EngineChannels {
        feeds: into_pair(feed_consumers),
        control: into_pair(control_producers),
        telemetry: telemetry_producer,
        log: engine_log,
    };
    threads.push(match args.book {
        BookKind::Vec => Engine::<VecBook>::new(&configs, channels, args.signal, args.idle, stop.clone()).spawn(),
        BookKind::Btree => Engine::<BTreeBook>::new(&configs, channels, args.signal, args.idle, stop.clone()).spawn(),
    });

    let logger_stop = Arc::new(AtomicBool::new(false));
    let logger = logging::spawn(log_consumers, logger_stop.clone());
    let http = http::spawn(HttpContext {
        address: args.http,
        telemetry_rx: telemetry_consumer,
        configs,
        stats: into_pair(feed_stats),
        log_dropped,
        stop: stop.clone(),
    });

    for thread in threads {
        let _ = thread.join();
    }
    let _ = http.join();
    logger_stop.store(true, Relaxed);
    let _ = logger.join();
    info!("stopped");
    Ok(())
}
