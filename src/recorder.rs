use crate::config::{Idle, RecordArgs, Venue, VenueConfig};
use crate::feed::rest::{Rest, RestError};
use crate::feed::ws;
use anyhow::anyhow;
use std::fs::{self, File};
use std::io::{BufWriter, ErrorKind, Write};
use std::thread;
use std::time::{Duration, Instant};
use tungstenite::Message;

/// Writes `{venue}.snapshot.json` (fetched right after the WS connects, as in live sync)
/// and `{venue}.jsonl` (one raw WS text frame per line).
pub fn record(a: RecordArgs) -> anyhow::Result<()> {
    fs::create_dir_all(&a.out)?;
    let handles: Vec<_> = Venue::ALL
        .into_iter()
        .map(|v| {
            let (a, cfg) = (a.clone(), VenueConfig::new(v, &a.symbol, Duration::from_secs(10), Idle::Block));
            thread::spawn(move || -> anyhow::Result<usize> {
                let mut ws = ws::connect(&cfg.ws_url, Duration::from_secs(10), false)?;
                let snap = Rest::new(Duration::from_secs(10)).get(&cfg.rest_url).map_err(|e| match e {
                    RestError::RateLimited(d) => anyhow!("rate limited {d:?}"),
                    RestError::Other(e) => e,
                })?;
                fs::write(a.out.join(format!("{}.snapshot.json", v.name())), snap)?;
                let mut out = BufWriter::new(File::create(a.out.join(format!("{}.jsonl", v.name())))?);
                let (until, mut n) = (Instant::now() + Duration::from_secs(a.secs), 0);
                while Instant::now() < until {
                    match ws.read() {
                        Ok(Message::Text(t)) => {
                            writeln!(out, "{}", t.as_str())?;
                            n += 1;
                        }
                        Ok(_) => {}
                        Err(tungstenite::Error::Io(e)) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                out.flush()?;
                Ok(n)
            })
        })
        .collect();
    for (v, h) in Venue::ALL.iter().zip(handles) {
        let n = h.join().map_err(|_| anyhow!("recorder panicked"))??;
        println!("{}: {n} frames -> {}", v.name(), a.out.display());
    }
    Ok(())
}
