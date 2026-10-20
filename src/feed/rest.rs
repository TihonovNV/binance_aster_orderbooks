use crate::config::VenueConfig;
use anyhow::{anyhow, bail};
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};
use std::time::Duration;
use ureq::Agent;

pub enum RestError {
    RateLimited(Duration),
    Other(anyhow::Error),
}

/// Blocking HTTP client; the agent keeps the connection alive between snapshots.
pub struct Rest {
    agent: Agent,
}

impl Rest {
    pub fn new(timeout: Duration) -> Self {
        Self { agent: Agent::config_builder().timeout_global(Some(timeout)).http_status_as_error(false).build().new_agent() }
    }

    pub fn get(&self, url: &str) -> Result<String, RestError> {
        let mut response = self.agent.get(url).call().map_err(|e| RestError::Other(e.into()))?;
        match response.status().as_u16() {
            200 => response.body_mut().with_config().limit(32 << 20).read_to_string().map_err(|e| RestError::Other(e.into())),
            429 | 418 => {
                let retry_after_secs =
                    response.headers().get("retry-after").and_then(|value| value.to_str().ok()?.parse().ok()).unwrap_or(30);
                Err(RestError::RateLimited(Duration::from_secs(retry_after_secs)))
            }
            status => Err(RestError::Other(anyhow!("http {status}"))),
        }
    }
}

/// Fails fast if the venue's tick/step for the symbol disagree with our fixed-point scales.
pub fn validate_scales(config: &VenueConfig) -> anyhow::Result<()> {
    let body = Rest::new(Duration::from_secs(10)).get(&config.info_url).map_err(|e| match e {
        RestError::RateLimited(retry_after) => anyhow!("rate limited for {retry_after:?}"),
        RestError::Other(e) => e,
    })?;
    let info: Value = sonic_rs::from_str(&body)?;
    let symbol = info["symbols"]
        .as_array()
        .and_then(|symbols| symbols.iter().find(|s| s["symbol"].as_str() == Some(&config.symbol)))
        .ok_or_else(|| anyhow!("{} not listed on {}", config.symbol, config.venue.name()))?;
    let filter_value = |filter_type: &str, field: &str| {
        symbol["filters"]
            .as_array()
            .and_then(|filters| filters.iter().find(|filter| filter["filterType"].as_str() == Some(filter_type)))
            .and_then(|filter| filter[field].as_str())
    };
    let decimals = |text: &str| text.trim_end_matches('0').split_once('.').map_or(0, |(_, fraction)| fraction.len() as u32);
    let (tick_size, step_size) = (filter_value("PRICE_FILTER", "tickSize"), filter_value("LOT_SIZE", "stepSize"));
    match (tick_size.map(decimals), step_size.map(decimals)) {
        (Some(price_decimals), Some(quantity_decimals))
            if price_decimals <= config.price_scale && quantity_decimals <= config.quantity_scale =>
        {
            Ok(())
        }
        _ => bail!(
            "{}: tick {tick_size:?} / step {step_size:?} do not fit scales {}/{}",
            config.venue.name(),
            config.price_scale,
            config.quantity_scale
        ),
    }
}
