use crate::feed::wire::Header;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Apply,
    /// Already covered by the book (`u <= last_u` or older than the snapshot).
    Skip,
    /// Sequence broken: gap, out-of-order or partial overlap.
    Resync,
    /// Snapshot is older than the first event after it (`U > lastUpdateId`).
    StaleSnapshot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Unsynced,
    Bridging { snap_u: u64 },
    Live { last_u: u64 },
}

#[derive(Debug)]
pub struct Sync {
    state: State,
}

impl Default for Sync {
    fn default() -> Self {
        Self { state: State::Unsynced }
    }
}

impl Sync {
    pub fn reset(&mut self) {
        self.state = State::Unsynced;
    }

    pub fn on_snapshot(&mut self, last_update_id: u64) {
        self.state = State::Bridging { snap_u: last_update_id };
    }

    pub fn is_live(&self) -> bool {
        matches!(self.state, State::Live { .. })
    }

    pub fn last_u(&self) -> Option<u64> {
        match self.state {
            State::Live { last_u } => Some(last_u),
            _ => None,
        }
    }

    pub fn on_event(&mut self, h: &Header) -> Action {
        match self.state {
            State::Unsynced => Action::Skip,
            State::Bridging { snap_u } if h.u < snap_u => Action::Skip,
            State::Bridging { snap_u } if h.first_u <= snap_u => {
                self.state = State::Live { last_u: h.u };
                Action::Apply
            }
            State::Bridging { .. } => {
                self.state = State::Unsynced;
                Action::StaleSnapshot
            }
            State::Live { last_u } if h.pu == last_u => {
                self.state = State::Live { last_u: h.u };
                Action::Apply
            }
            State::Live { last_u } if h.u <= last_u => Action::Skip,
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

    fn ev(first_u: u64, u: u64, pu: u64) -> Header {
        Header { first_u, u, pu, ..Default::default() }
    }

    fn live_at(u: u64) -> Sync {
        let mut s = Sync::default();
        s.on_snapshot(u);
        assert_eq!(s.on_event(&ev(u - 1, u, u - 2)), Action::Apply);
        s
    }

    #[test]
    fn unsynced_skips() {
        assert_eq!(Sync::default().on_event(&ev(1, 2, 0)), Action::Skip);
    }

    #[test]
    fn bridge() {
        let mut s = Sync::default();
        s.on_snapshot(100);
        assert_eq!(s.on_event(&ev(80, 90, 79)), Action::Skip);
        assert_eq!(s.on_event(&ev(91, 99, 90)), Action::Skip);
        assert_eq!(s.on_event(&ev(100, 105, 99)), Action::Apply);
        assert_eq!(s.last_u(), Some(105));
    }

    #[test]
    fn bridge_on_exact_boundary() {
        let mut s = Sync::default();
        s.on_snapshot(100);
        assert_eq!(s.on_event(&ev(95, 100, 94)), Action::Apply);
    }

    #[test]
    fn stale_snapshot() {
        let mut s = Sync::default();
        s.on_snapshot(100);
        assert_eq!(s.on_event(&ev(101, 110, 100)), Action::StaleSnapshot);
        assert!(!s.is_live());
    }

    #[test]
    fn in_order() {
        let mut s = live_at(100);
        assert_eq!(s.on_event(&ev(101, 120, 100)), Action::Apply);
        assert_eq!(s.on_event(&ev(121, 130, 120)), Action::Apply);
        assert_eq!(s.last_u(), Some(130));
    }

    #[test]
    fn duplicate_and_old_are_skipped() {
        let mut s = live_at(100);
        assert_eq!(s.on_event(&ev(101, 120, 100)), Action::Apply);
        assert_eq!(s.on_event(&ev(101, 120, 100)), Action::Skip);
        assert_eq!(s.on_event(&ev(50, 60, 49)), Action::Skip);
        assert_eq!(s.last_u(), Some(120));
    }

    #[test]
    fn gap_resyncs() {
        let mut s = live_at(100);
        assert_eq!(s.on_event(&ev(111, 120, 110)), Action::Resync);
        assert!(!s.is_live());
    }

    #[test]
    fn out_of_order_resyncs() {
        let mut s = live_at(100);
        assert_eq!(s.on_event(&ev(121, 130, 120)), Action::Resync);
    }

    #[test]
    fn partial_overlap_resyncs() {
        let mut s = live_at(100);
        assert_eq!(s.on_event(&ev(95, 110, 94)), Action::Resync);
    }
}
