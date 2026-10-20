use crate::book::Level;
use crate::config::Venue;
use crate::feed::sync::{Action, Sync};
use crate::feed::wire::{Depth, Header, Scales};
use anyhow::{Context, bail};
use std::path::Path;

pub const SCALES: Scales = Scales { px: 2, qty: 3 };

pub struct Msg {
    pub hdr: Header,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// Raw recording plus the sequenced messages a live feed would forward.
pub struct Recording {
    pub frames: Vec<String>,
    pub snapshot: Msg,
    pub diffs: Vec<Msg>,
}

impl Msg {
    fn from(d: &Depth) -> Self {
        Msg { hdr: d.hdr, bids: d.bids.clone(), asks: d.asks.clone() }
    }
}

pub fn load(dir: &Path, venue: Venue) -> anyhow::Result<Recording> {
    let read = |f: String| std::fs::read_to_string(dir.join(&f)).with_context(|| f);
    let snap = read(format!("{}.snapshot.json", venue.name()))?;
    let frames: Vec<String> = read(format!("{}.jsonl", venue.name()))?.lines().map(str::to_owned).collect();
    let mut d = Depth::default();
    d.parse(&snap, SCALES).map_err(|e| anyhow::anyhow!("{e:?}"))?;
    let snapshot = Msg::from(&d);
    let mut sync = Sync::default();
    sync.on_snapshot(snapshot.hdr.u);
    let mut diffs = vec![];
    for f in &frames {
        d.parse(f, SCALES).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        match sync.on_event(&d.hdr) {
            Action::Apply => diffs.push(Msg::from(&d)),
            Action::Skip => {}
            a => bail!("{}: {a:?} at u={}", venue.name(), d.hdr.u),
        }
    }
    Ok(Recording { frames, snapshot, diffs })
}
