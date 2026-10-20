use crate::book::Level;
use crate::config::Venue;
use crate::feed::sync::{Action, Sequencer};
use crate::feed::wire::{Depth, Header, Scales};
use anyhow::{Context, bail};
use std::path::Path;

pub const SCALES: Scales = Scales { price: 2, quantity: 3 };

pub struct RecordedMessage {
    pub header: Header,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// Raw recording plus the sequenced messages a live feed would forward.
pub struct Recording {
    pub frames: Vec<String>,
    pub snapshot: RecordedMessage,
    pub diffs: Vec<RecordedMessage>,
}

impl RecordedMessage {
    fn from(depth: &Depth) -> Self {
        RecordedMessage { header: depth.header, bids: depth.bids.clone(), asks: depth.asks.clone() }
    }
}

pub fn load(dir: &Path, venue: Venue) -> anyhow::Result<Recording> {
    let read = |file_name: String| std::fs::read_to_string(dir.join(&file_name)).with_context(|| file_name);
    let snapshot_json = read(format!("{}.snapshot.json", venue.name()))?;
    let frames: Vec<String> = read(format!("{}.jsonl", venue.name()))?.lines().map(str::to_owned).collect();
    let mut depth = Depth::default();
    depth.parse(&snapshot_json, SCALES).map_err(|e| anyhow::anyhow!("{e:?}"))?;
    let snapshot = RecordedMessage::from(&depth);
    let mut sequencer = Sequencer::default();
    sequencer.on_snapshot(snapshot.header.last_update_id);
    let mut diffs = vec![];
    for frame in &frames {
        depth.parse(frame, SCALES).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        match sequencer.on_event(&depth.header) {
            Action::Apply => diffs.push(RecordedMessage::from(&depth)),
            Action::Skip => {}
            action => bail!("{}: {action:?} at update id {}", venue.name(), depth.header.last_update_id),
        }
    }
    Ok(Recording { frames, snapshot, diffs })
}
