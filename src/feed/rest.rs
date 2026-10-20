use crate::config::VenueConfig;
use anyhow::{anyhow, bail};
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};
use std::time::Duration;
use ureq::Agent;

pub enum RestError {
    RateLimited(Duration),
    Other(anyhow::Error),
}

pub struct Rest(Agent);

impl Rest {
    pub fn new(timeout: Duration) -> Self {
        Self(Agent::config_builder().timeout_global(Some(timeout)).http_status_as_error(false).build().new_agent())
    }

    pub fn get(&self, url: &str) -> Result<String, RestError> {
        let mut resp = self.0.get(url).call().map_err(|e| RestError::Other(e.into()))?;
        match resp.status().as_u16() {
            200 => resp.body_mut().with_config().limit(32 << 20).read_to_string().map_err(|e| RestError::Other(e.into())),
            429 | 418 => {
                let secs = resp.headers().get("retry-after").and_then(|v| v.to_str().ok()?.parse().ok()).unwrap_or(30);
                Err(RestError::RateLimited(Duration::from_secs(secs)))
            }
            s => Err(RestError::Other(anyhow!("http {s}"))),
        }
    }
}

/// Fails fast if the venue's tick/step for the symbol disagree with our fixed-point scales.
pub fn validate_scales(cfg: &VenueConfig) -> anyhow::Result<()> {
    let body = Rest::new(Duration::from_secs(10)).get(&cfg.info_url).map_err(|e| match e {
        RestError::RateLimited(d) => anyhow!("rate limited for {d:?}"),
        RestError::Other(e) => e,
    })?;
    let info: Value = sonic_rs::from_str(&body)?;
    let sym = info["symbols"]
        .as_array()
        .and_then(|a| a.iter().find(|s| s["symbol"].as_str() == Some(&cfg.symbol)))
        .ok_or_else(|| anyhow!("{} not listed on {}", cfg.symbol, cfg.venue.name()))?;
    let filter = |kind: &str, field: &str| {
        sym["filters"].as_array().and_then(|f| f.iter().find(|x| x["filterType"].as_str() == Some(kind))).and_then(|x| x[field].as_str())
    };
    let decimals = |s: &str| s.trim_end_matches('0').split_once('.').map_or(0, |(_, f)| f.len() as u32);
    let (tick, step) = (filter("PRICE_FILTER", "tickSize"), filter("LOT_SIZE", "stepSize"));
    match (tick.map(decimals), step.map(decimals)) {
        (Some(p), Some(q)) if p <= cfg.px_scale && q <= cfg.qty_scale => Ok(()),
        _ => bail!("{}: tick {tick:?} / step {step:?} do not fit scales {}/{}", cfg.venue.name(), cfg.px_scale, cfg.qty_scale),
    }
}
