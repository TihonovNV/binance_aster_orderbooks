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

pub struct HttpCtx {
    pub addr: SocketAddr,
    pub tele: Consumer<Telemetry>,
    pub cfgs: [VenueConfig; 2],
    pub stats: [Arc<FeedStats>; 2],
    pub log_dropped: Vec<(&'static str, Arc<AtomicU64>)>,
    pub stop: Arc<AtomicBool>,
}

struct Shared {
    latest: Mutex<Option<(Telemetry, Instant)>>,
    cfgs: [VenueConfig; 2],
    stats: [Arc<FeedStats>; 2],
    log_dropped: Vec<(&'static str, Arc<AtomicU64>)>,
}

type S = State<Arc<Shared>>;

pub fn spawn(ctx: HttpCtx) -> JoinHandle<()> {
    thread::Builder::new()
        .name("http".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("tokio");
            if let Err(e) = rt.block_on(serve(ctx)) {
                error!(error = %e, "http server failed");
            }
        })
        .expect("spawn http")
}

async fn serve(ctx: HttpCtx) -> anyhow::Result<()> {
    let HttpCtx { addr, tele, cfgs, stats, log_dropped, stop } = ctx;
    let shared = Arc::new(Shared { latest: Mutex::new(None), cfgs, stats, log_dropped });
    tokio::spawn(drain(tele, shared.clone(), stop.clone()));
    let app = Router::new()
        .route("/status", get(status))
        .route("/signal", get(signal))
        .route("/book/{venue}", get(book))
        .route("/timing", get(timing_get).post(timing_post))
        .route("/health", get(health))
        .with_state(shared);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "status server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            while !stop.load(Relaxed) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await?;
    Ok(())
}

async fn drain(mut rx: Consumer<Telemetry>, sh: Arc<Shared>, stop: Arc<AtomicBool>) {
    let mut last_summary = Instant::now();
    while !stop.load(Relaxed) {
        while let Ok(t) = rx.pop() {
            *sh.latest.lock().unwrap() = Some((t, Instant::now()));
        }
        if last_summary.elapsed() > SUMMARY_EVERY {
            last_summary = Instant::now();
            summary(&sh);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn summary(sh: &Shared) {
    let Some((t, _)) = *sh.latest.lock().unwrap() else { return };
    let v = |i: usize| {
        let (x, s) = (&t.venues[i], sh.stats[i].view());
        format!(
            "{:?} age={:.0}ms lvls={}/{} resyncs={} reconnects={} gaps={}",
            x.state, x.age_ms, x.depth.0, x.depth.1, s.resyncs, s.reconnects, s.gaps
        )
    };
    info!(binance = %v(0), aster = %v(1), signal = ?t.decision.bias, "status");
}

fn latest(sh: &Shared) -> Option<(Telemetry, Duration)> {
    sh.latest.lock().unwrap().map(|(t, at)| (t, at.elapsed()))
}

async fn status(State(sh): S) -> Json<Value> {
    let Some((t, age)) = latest(&sh) else { return Json(json!({ "engine": { "fresh": false } })) };
    let venues: Vec<Value> = t
        .venues
        .iter()
        .zip(&sh.stats)
        .map(|(v, s)| {
            json!({
                "venue": v.venue, "state": v.state, "reason": v.reason, "in_state_ms": v.in_state_ms,
                "tradable": v.tradable, "valid": v.valid, "crossed": v.crossed,
                "last_u": v.last_u, "book_age_ms": v.age_ms, "levels": [v.depth.0, v.depth.1],
                "commits": v.commits, "crossed_books": v.crossed_books, "engine_resyncs": v.engine_resyncs,
                "io": s.view(),
            })
        })
        .collect();
    let dropped: HashMap<_, _> = sh.log_dropped.iter().map(|(n, c)| (*n, c.load(Relaxed))).collect();
    Json(json!({
        "engine": { "fresh": age < FRESH, "telemetry_age_ms": age.as_millis() as u64 },
        "timing_enabled": timing::enabled(),
        "log_dropped": dropped,
        "venues": venues,
        "signal": t.decision.bias,
    }))
}

async fn signal(State(sh): S) -> Result<Json<Value>, StatusCode> {
    let (t, _) = latest(&sh).ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Json(json!(t.decision)))
}

async fn book(State(sh): S, Path(venue): Path<String>) -> Result<Json<Value>, StatusCode> {
    let i = Venue::ALL.iter().position(|v| v.name() == venue).ok_or(StatusCode::NOT_FOUND)?;
    let (t, _) = latest(&sh).ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let (v, c) = (&t.venues[i], &sh.cfgs[i]);
    let side = |lv: &[crate::book::Level]| -> Vec<[f64; 2]> {
        lv.iter().filter(|l| l.qty > 0).map(|l| [to_f64(l.px, c.px_scale), to_f64(l.qty, c.qty_scale)]).collect()
    };
    Ok(Json(json!({ "venue": v.venue, "state": v.state, "last_u": v.last_u, "bids": side(&v.bids), "asks": side(&v.asks) })))
}

async fn timing_get(State(sh): S) -> Json<Value> {
    let Some((t, _)) = latest(&sh) else { return Json(json!({ "enabled": timing::enabled() })) };
    let venues: HashMap<_, _> = t
        .venues
        .iter()
        .map(|v| (v.venue.name(), Stage::ALL.iter().map(|s| (s, v.timing[*s as usize])).collect::<HashMap<_, _>>()))
        .collect();
    Json(json!({ "enabled": timing::enabled(), "window_s": 10, "stages": venues }))
}

async fn timing_post(Query(q): Query<HashMap<String, String>>) -> Result<Json<Value>, StatusCode> {
    let on = q.get("enabled").and_then(|v| v.parse().ok()).ok_or(StatusCode::BAD_REQUEST)?;
    timing::set_enabled(on);
    info!(enabled = on, "timing toggled");
    Ok(Json(json!({ "enabled": on })))
}

async fn health(State(sh): S) -> StatusCode {
    match latest(&sh) {
        Some((t, age)) if age < FRESH && t.venues.iter().all(|v| v.tradable) => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    }
}
