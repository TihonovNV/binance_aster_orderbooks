use crate::feed::wire::Header;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Apply,
    /// Already covered by the book (older than the book or the snapshot).
    Skip,
    /// Sequence broken: gap, out-of-order or partial overlap.
    Resync,
    /// Snapshot is older than the first event after it (`first_update_id > snapshot id`).
    StaleSnapshot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Unsynced,
    Bridging { snapshot_update_id: u64 },
    Live { last_update_id: u64 },
}

/// Binance USD-M diff-depth sequencing rules (shared by Aster). Pure: no I/O, no book.
#[derive(Debug)]
pub struct Sequencer {
    state: State,
}

impl Default for Sequencer {
    fn default() -> Self {
        Self { state: State::Unsynced }
    }
}

impl Sequencer {
    pub fn reset(&mut self) {
        self.state = State::Unsynced;
    }

    pub fn on_snapshot(&mut self, snapshot_update_id: u64) {
        self.state = State::Bridging { snapshot_update_id };
    }

    pub fn is_live(&self) -> bool {
        matches!(self.state, State::Live { .. })
    }

    pub fn last_update_id(&self) -> Option<u64> {
        match self.state {
            State::Live { last_update_id } => Some(last_update_id),
            _ => None,
        }
    }

    /// Arm order matters: each guard relies on the arms above it.
    pub fn on_event(&mut self, event: &Header) -> Action {
        match self.state {
            State::Unsynced => Action::Skip,
            State::Bridging { snapshot_update_id } if event.last_update_id < snapshot_update_id => Action::Skip,
            State::Bridging { snapshot_update_id } if event.first_update_id <= snapshot_update_id => {
                self.state = State::Live { last_update_id: event.last_update_id };
                Action::Apply
            }
            State::Bridging { .. } => {
                self.state = State::Unsynced;
                Action::StaleSnapshot
            }
            State::Live { last_update_id } if event.prev_update_id == last_update_id => {
                self.state = State::Live { last_update_id: event.last_update_id };
                Action::Apply
            }
            State::Live { last_update_id } if event.last_update_id <= last_update_id => Action::Skip,
            State::Live { .. } => {
                self.state = State::Unsynced;
                Action::Resync
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(first_update_id: u64, last_update_id: u64, prev_update_id: u64) -> Header {
        Header { first_update_id, last_update_id, prev_update_id, ..Default::default() }
    }

    fn live_at(update_id: u64) -> Sequencer {
        let mut sequencer = Sequencer::default();
        sequencer.on_snapshot(update_id);
        assert_eq!(sequencer.on_event(&event(update_id - 1, update_id, update_id - 2)), Action::Apply);
        sequencer
    }

    #[test]
    fn unsynced_skips() {
        assert_eq!(Sequencer::default().on_event(&event(1, 2, 0)), Action::Skip);
    }

    #[test]
    fn bridge() {
        let mut sequencer = Sequencer::default();
        sequencer.on_snapshot(100);
        assert_eq!(sequencer.on_event(&event(80, 90, 79)), Action::Skip);
        assert_eq!(sequencer.on_event(&event(91, 99, 90)), Action::Skip);
        assert_eq!(sequencer.on_event(&event(100, 105, 99)), Action::Apply);
        assert_eq!(sequencer.last_update_id(), Some(105));
    }

    #[test]
    fn bridge_on_exact_boundary() {
        let mut sequencer = Sequencer::default();
        sequencer.on_snapshot(100);
        assert_eq!(sequencer.on_event(&event(95, 100, 94)), Action::Apply);
    }

    #[test]
    fn stale_snapshot() {
        let mut sequencer = Sequencer::default();
        sequencer.on_snapshot(100);
        assert_eq!(sequencer.on_event(&event(101, 110, 100)), Action::StaleSnapshot);
        assert!(!sequencer.is_live());
    }

    #[test]
    fn in_order() {
        let mut sequencer = live_at(100);
        assert_eq!(sequencer.on_event(&event(101, 120, 100)), Action::Apply);
        assert_eq!(sequencer.on_event(&event(121, 130, 120)), Action::Apply);
        assert_eq!(sequencer.last_update_id(), Some(130));
    }

    #[test]
    fn duplicate_and_old_are_skipped() {
        let mut sequencer = live_at(100);
        assert_eq!(sequencer.on_event(&event(101, 120, 100)), Action::Apply);
        assert_eq!(sequencer.on_event(&event(101, 120, 100)), Action::Skip);
        assert_eq!(sequencer.on_event(&event(50, 60, 49)), Action::Skip);
        assert_eq!(sequencer.last_update_id(), Some(120));
    }

    #[test]
    fn gap_resyncs() {
        let mut sequencer = live_at(100);
        assert_eq!(sequencer.on_event(&event(111, 120, 110)), Action::Resync);
        assert!(!sequencer.is_live());
    }

    #[test]
    fn out_of_order_resyncs() {
        let mut sequencer = live_at(100);
        assert_eq!(sequencer.on_event(&event(121, 130, 120)), Action::Resync);
    }

    #[test]
    fn partial_overlap_resyncs() {
        let mut sequencer = live_at(100);
        assert_eq!(sequencer.on_event(&event(95, 110, 94)), Action::Resync);
    }
}
