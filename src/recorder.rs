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
pub fn record(args: RecordArgs) -> anyhow::Result<()> {
    fs::create_dir_all(&args.out)?;
    let recorders: Vec<_> = Venue::ALL
        .into_iter()
        .map(|venue| {
            let args = args.clone();
            let config = VenueConfig::new(venue, &args.symbol, Duration::from_secs(10), Idle::Block);
            thread::spawn(move || -> anyhow::Result<usize> {
                let mut ws = ws::connect(&config.ws_url, Duration::from_secs(10), false)?;
                let snapshot = Rest::new(Duration::from_secs(10)).get(&config.rest_url).map_err(|e| match e {
                    RestError::RateLimited(retry_after) => anyhow!("rate limited {retry_after:?}"),
                    RestError::Other(e) => e,
                })?;
                fs::write(args.out.join(format!("{}.snapshot.json", venue.name())), snapshot)?;
                let mut frames_file = BufWriter::new(File::create(args.out.join(format!("{}.jsonl", venue.name())))?);
                let (until, mut frames) = (Instant::now() + Duration::from_secs(args.secs), 0);
                while Instant::now() < until {
                    match ws.read() {
                        Ok(Message::Text(text)) => {
                            writeln!(frames_file, "{}", text.as_str())?;
                            frames += 1;
                        }
                        Ok(_) => {}
                        Err(tungstenite::Error::Io(e)) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                frames_file.flush()?;
                Ok(frames)
            })
        })
        .collect();
    for (venue, recorder) in Venue::ALL.iter().zip(recorders) {
        let frames = recorder.join().map_err(|_| anyhow!("recorder panicked"))??;
        println!("{}: {frames} frames -> {}", venue.name(), args.out.display());
    }
    Ok(())
}
