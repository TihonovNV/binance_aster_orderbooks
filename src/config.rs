use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::{net::SocketAddr, path::PathBuf, time::Duration};

#[derive(Parser, Debug)]
#[command(about = "Binance + Aster ETHUSDT perp order books with a cross-venue signal", args_conflicts_with_subcommands = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
    #[command(flatten)]
    pub run: RunArgs,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run feeds, engine, signal and status server (default)
    Run(RunArgs),
    /// Record REST snapshot + raw WS frames per venue for tests and benches
    Record(RecordArgs),
}

#[derive(Args, Debug, Clone)]
pub struct RunArgs {
    #[arg(long, default_value = "ETHUSDT")]
    pub symbol: String,
    #[arg(long, value_enum, default_value_t = BookKind::Btree)]
    pub book: BookKind,
    #[arg(long, value_enum, default_value_t = Idle::Spin)]
    pub idle: Idle,
    /// Enable per-stage timing at startup (toggle later via POST /timing)
    #[arg(long)]
    pub timing: bool,
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub http: SocketAddr,
    #[arg(long, default_value = "logs")]
    pub log_dir: PathBuf,
    #[arg(long, default_value_t = 5000)]
    pub watchdog_ms: u64,
    #[command(flatten)]
    pub signal: SignalParams,
}

#[derive(Args, Debug, Clone, Copy, Serialize)]
pub struct SignalParams {
    /// Deviation from the basis needed to enter a bias; it is kept until it falls below half of this
    #[arg(long, default_value_t = 1.0)]
    pub entry_threshold_bps: f64,
    /// Aster top-5 imbalance against the bias that vetoes it
    #[arg(long, default_value_t = 0.3)]
    pub imbalance_veto: f64,
    /// Time constant of the EMA that tracks the normal Binance–Aster gap
    #[arg(long, default_value_t = 30.0)]
    pub basis_ema_secs: f64,
    #[arg(long, default_value_t = 5.0)]
    pub max_spread_bps: f64,
    #[arg(long, default_value_t = 1000)]
    pub max_age_ms: u64,
    #[arg(long, default_value_t = 500)]
    pub max_age_difference_ms: u64,
}

impl Default for SignalParams {
    fn default() -> Self {
        Self {
            entry_threshold_bps: 1.0,
            imbalance_veto: 0.3,
            basis_ema_secs: 30.0,
            max_spread_bps: 5.0,
            max_age_ms: 1000,
            max_age_difference_ms: 500,
        }
    }
}

#[derive(Args, Debug, Clone)]
pub struct RecordArgs {
    #[arg(long, default_value = "ETHUSDT")]
    pub symbol: String,
    #[arg(long, default_value_t = 60)]
    pub secs: u64,
    #[arg(long, default_value = "fixtures")]
    pub out: PathBuf,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookKind {
    Btree,
    Vec,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Idle {
    Spin,
    Block,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Venue {
    Binance,
    Aster,
}

impl Venue {
    pub const ALL: [Venue; 2] = [Venue::Binance, Venue::Aster];

    pub fn index(self) -> usize {
        self as usize
    }

    pub fn name(self) -> &'static str {
        match self {
            Venue::Binance => "binance",
            Venue::Aster => "aster",
        }
    }
}

#[derive(Debug, Clone)]
pub struct VenueConfig {
    pub venue: Venue,
    pub symbol: String,
    pub ws_url: String,
    pub rest_url: String,
    pub info_url: String,
    pub price_scale: u32,
    pub quantity_scale: u32,
    pub watchdog: Duration,
    pub idle: Idle,
}

impl VenueConfig {
    pub fn new(venue: Venue, symbol: &str, watchdog: Duration, idle: Idle) -> Self {
        let (ws, rest) = match venue {
            Venue::Binance => ("wss://fstream.binance.com/public", "https://fapi.binance.com"),
            Venue::Aster => ("wss://fstream.asterdex.com", "https://fapi.asterdex.com"),
        };
        let stream_symbol = symbol.to_lowercase();
        Self {
            venue,
            symbol: symbol.to_uppercase(),
            ws_url: format!("{ws}/stream?streams={stream_symbol}@depth@100ms"),
            rest_url: format!("{rest}/fapi/v1/depth?symbol={}&limit=1000", symbol.to_uppercase()),
            info_url: format!("{rest}/fapi/v1/exchangeInfo"),
            price_scale: 2,
            quantity_scale: 3,
            watchdog,
            idle,
        }
    }
}
