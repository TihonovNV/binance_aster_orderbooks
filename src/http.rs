use crate::book::Level;
use crate::config::{Venue, VenueConfig};
use crate::engine::{Stage, Telemetry};
use crate::feed::stats::FeedStats;
use crate::fixed::to_f64;
use crate::timing;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use rtrb::Consumer;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tracing::{error, info};

const FRESH: Duration = Duration::from_secs(1);
const SUMMARY_EVERY: Duration = Duration::from_secs(10);

pub struct HttpContext {
    pub address: SocketAddr,
    pub telemetry_rx: Consumer<Telemetry>,
    pub configs: [VenueConfig; 2],
    pub stats: [Arc<FeedStats>; 2],
    pub log_dropped: Vec<(&'static str, Arc<AtomicU64>)>,
    pub stop: Arc<AtomicBool>,
}

struct Shared {
    latest: Mutex<Option<(Telemetry, Instant)>>,
    configs: [VenueConfig; 2],
    stats: [Arc<FeedStats>; 2],
    log_dropped: Vec<(&'static str, Arc<AtomicU64>)>,
}

type AppState = State<Arc<Shared>>;

pub fn spawn(context: HttpContext) -> JoinHandle<()> {
    thread::Builder::new()
        .name("http".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("tokio");
            if let Err(e) = runtime.block_on(serve(context)) {
                error!(error = %e, "http server failed");
            }
        })
        .expect("spawn http")
}

async fn serve(context: HttpContext) -> anyhow::Result<()> {
    let HttpContext { address, telemetry_rx, configs, stats, log_dropped, stop } = context;
    let shared = Arc::new(Shared { latest: Mutex::new(None), configs, stats, log_dropped });
    tokio::spawn(drain(telemetry_rx, shared.clone(), stop.clone()));
    let app = Router::new()
        .route("/status", get(status))
        .route("/signal", get(signal))
        .route("/book/{venue}", get(book))
        .route("/timing", get(timing_get).post(timing_post))
        .route("/health", get(health))
        .with_state(shared);
    let listener = tokio::net::TcpListener::bind(address).await?;
    info!(%address, "status server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            while !stop.load(Relaxed) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await?;
    Ok(())
}

async fn drain(mut telemetry_rx: Consumer<Telemetry>, shared: Arc<Shared>, stop: Arc<AtomicBool>) {
    let mut last_summary = Instant::now();
    while !stop.load(Relaxed) {
        while let Ok(telemetry) = telemetry_rx.pop() {
            *shared.latest.lock().unwrap() = Some((telemetry, Instant::now()));
        }
        if last_summary.elapsed() > SUMMARY_EVERY {
            last_summary = Instant::now();
            log_summary(&shared);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn log_summary(shared: &Shared) {
    let Some((telemetry, _)) = *shared.latest.lock().unwrap() else { return };
    let describe = |index: usize| {
        let (venue, stats) = (&telemetry.venues[index], shared.stats[index].view());
        format!(
            "{:?} age={:.0}ms levels={}/{} resyncs={} reconnects={} gaps={}",
            venue.state, venue.book_age_ms, venue.level_counts.0, venue.level_counts.1, stats.resyncs, stats.reconnects, stats.gaps
        )
    };
    info!(binance = %describe(0), aster = %describe(1), signal = ?telemetry.decision.bias, "status");
}

fn latest(shared: &Shared) -> Option<(Telemetry, Duration)> {
    shared.latest.lock().unwrap().map(|(telemetry, received)| (telemetry, received.elapsed()))
}

async fn status(State(shared): AppState) -> Json<Value> {
    let Some((telemetry, age)) = latest(&shared) else { return Json(json!({ "engine": { "fresh": false } })) };
    let venues: Vec<Value> = telemetry
        .venues
        .iter()
        .zip(&shared.stats)
        .map(|(venue, stats)| {
            json!({
                "venue": venue.venue, "state": venue.state, "reason": venue.reason, "in_state_ms": venue.in_state_ms,
                "tradable": venue.tradable, "valid": venue.valid, "crossed": venue.crossed,
                "last_update_id": venue.last_update_id, "book_age_ms": venue.book_age_ms,
                "levels": [venue.level_counts.0, venue.level_counts.1],
                "commits": venue.commits, "crossed_books": venue.crossed_books, "engine_resyncs": venue.engine_resyncs,
                "io": stats.view(),
            })
        })
        .collect();
    let dropped: HashMap<_, _> = shared.log_dropped.iter().map(|(name, counter)| (*name, counter.load(Relaxed))).collect();
    Json(json!({
        "engine": { "fresh": age < FRESH, "telemetry_age_ms": age.as_millis() as u64 },
        "timing_enabled": timing::enabled(),
        "log_dropped": dropped,
        "venues": venues,
        "signal": telemetry.decision.bias,
    }))
}

async fn signal(State(shared): AppState) -> Result<Json<Value>, StatusCode> {
    let (telemetry, _) = latest(&shared).ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Json(json!(telemetry.decision)))
}

async fn book(State(shared): AppState, Path(venue_name): Path<String>) -> Result<Json<Value>, StatusCode> {
    let index = Venue::ALL.iter().position(|venue| venue.name() == venue_name).ok_or(StatusCode::NOT_FOUND)?;
    let (telemetry, _) = latest(&shared).ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let (venue, config) = (&telemetry.venues[index], &shared.configs[index]);
    let side = |levels: &[Level]| -> Vec<[f64; 2]> {
        levels
            .iter()
            .filter(|level| level.quantity > 0)
            .map(|level| [to_f64(level.price, config.price_scale), to_f64(level.quantity, config.quantity_scale)])
            .collect()
    };
    Ok(Json(json!({
        "venue": venue.venue, "state": venue.state, "last_update_id": venue.last_update_id,
        "bids": side(&venue.bids), "asks": side(&venue.asks),
    })))
}

async fn timing_get(State(shared): AppState) -> Json<Value> {
    let Some((telemetry, _)) = latest(&shared) else { return Json(json!({ "enabled": timing::enabled() })) };
    let venues: HashMap<_, _> = telemetry
        .venues
        .iter()
        .map(|venue| {
            let stages: HashMap<_, _> = Stage::ALL.iter().map(|stage| (stage, venue.timing[*stage as usize])).collect();
            (venue.venue.name(), stages)
        })
        .collect();
    Json(json!({ "enabled": timing::enabled(), "window_s": 10, "stages": venues }))
}

async fn timing_post(Query(query): Query<HashMap<String, String>>) -> Result<Json<Value>, StatusCode> {
    let enabled = query.get("enabled").and_then(|value| value.parse().ok()).ok_or(StatusCode::BAD_REQUEST)?;
    timing::set_enabled(enabled);
    info!(enabled, "timing toggled");
    Ok(Json(json!({ "enabled": enabled })))
}

async fn health(State(shared): AppState) -> StatusCode {
    match latest(&shared) {
        Some((telemetry, age)) if age < FRESH && telemetry.venues.iter().all(|venue| venue.tradable) => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    }
}
