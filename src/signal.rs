use crate::bus::FeedState;
use crate::config::{SignalParams, Venue};
use serde::Serialize;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub enum Bias {
    #[default]
    Hold,
    LongBias,
    ShortBias,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionReason {
    #[default]
    Init,
    Signal,
    BelowThreshold,
    ImbalanceVeto,
    NotLive(Venue),
    ProcessingTransaction(Venue),
    Invalid(Venue),
    Empty(Venue),
    Crossed(Venue),
    Stale(Venue),
    WideSpread(Venue),
    AgeDifference,
}

/// What the signal needs from one book, taken by the engine between transactions.
#[derive(Clone, Copy, Debug)]
pub struct BookView {
    pub venue: Venue,
    pub state: FeedState,
    pub processing_transaction: bool,
    pub valid: bool,
    pub crossed: bool,
    pub top: Option<TopOfBook>,
    pub bid_depth_5: f64,
    pub ask_depth_5: f64,
    pub age_ms: f64,
    pub last_update_id: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TopOfBook {
    pub bid: f64,
    pub bid_quantity: f64,
    pub ask: f64,
    pub ask_quantity: f64,
}

impl TopOfBook {
    fn mid(&self) -> f64 {
        (self.bid + self.ask) / 2.0
    }

    /// Mid weighted toward the side with less size: that side is more likely to trade through.
    fn microprice(&self) -> f64 {
        (self.bid * self.ask_quantity + self.ask * self.bid_quantity) / (self.bid_quantity + self.ask_quantity)
    }

    fn spread_bps(&self) -> f64 {
        (self.ask - self.bid) / self.mid() * 1e4
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct Decision {
    pub bias: Bias,
    pub reason: DecisionReason,
    pub wall_time_ms: u64,
    pub binance_bid: f64,
    pub binance_ask: f64,
    pub binance_microprice: f64,
    pub aster_bid: f64,
    pub aster_ask: f64,
    pub aster_mid: f64,
    pub gap_bps: f64,
    pub basis_ema_bps: f64,
    pub deviation_bps: f64,
    pub aster_imbalance: f64,
    pub binance_age_ms: f64,
    pub aster_age_ms: f64,
    pub binance_update_id: u64,
    pub aster_update_id: u64,
}

#[derive(Default)]
pub struct SignalState {
    basis_ema_bps: Option<f64>,
    last_update_ns: u64,
    bias: Bias,
}

fn blocking_reason(view: &BookView, params: &SignalParams) -> Option<DecisionReason> {
    let venue = view.venue;
    let Some(top) = view.top else { return Some(DecisionReason::Empty(venue)) };
    if view.state != FeedState::Live {
        Some(DecisionReason::NotLive(venue))
    } else if view.processing_transaction {
        Some(DecisionReason::ProcessingTransaction(venue))
    } else if !view.valid {
        Some(DecisionReason::Invalid(venue))
    } else if view.crossed {
        Some(DecisionReason::Crossed(venue))
    } else if view.age_ms > params.max_age_ms as f64 {
        Some(DecisionReason::Stale(venue))
    } else if top.spread_bps() > params.max_spread_bps {
        Some(DecisionReason::WideSpread(venue))
    } else {
        None
    }
}

/// Lead-lag: Binance microprice vs Aster mid, de-biased by an EMA of the gap,
/// confirmed by Aster top-5 imbalance, with enter at threshold / exit at half-threshold hysteresis.
pub fn decide(
    binance: &BookView,
    aster: &BookView,
    state: &mut SignalState,
    params: &SignalParams,
    now_ns: u64,
    wall_time_ms: u64,
) -> Decision {
    let mut decision = Decision {
        wall_time_ms,
        binance_age_ms: binance.age_ms,
        aster_age_ms: aster.age_ms,
        binance_update_id: binance.last_update_id,
        aster_update_id: aster.last_update_id,
        ..Default::default()
    };
    if let Some(top) = binance.top {
        (decision.binance_bid, decision.binance_ask, decision.binance_microprice) = (top.bid, top.ask, top.microprice());
    }
    if let Some(top) = aster.top {
        (decision.aster_bid, decision.aster_ask, decision.aster_mid) = (top.bid, top.ask, top.mid());
    }
    let ages_too_far_apart = (binance.age_ms - aster.age_ms).abs() > params.max_age_difference_ms as f64;
    let blocked = blocking_reason(binance, params)
        .or_else(|| blocking_reason(aster, params))
        .or(ages_too_far_apart.then_some(DecisionReason::AgeDifference));
    if let Some(reason) = blocked {
        state.bias = Bias::Hold;
        decision.reason = reason;
        return decision;
    }

    decision.gap_bps = (decision.binance_microprice - decision.aster_mid) / decision.aster_mid * 1e4;
    let basis_ema_bps = match state.basis_ema_bps {
        None => decision.gap_bps,
        Some(previous) => {
            let elapsed_secs = (now_ns.saturating_sub(state.last_update_ns)) as f64 / 1e9;
            previous + (1.0 - (-elapsed_secs / params.basis_ema_secs).exp()) * (decision.gap_bps - previous)
        }
    };
    (state.basis_ema_bps, state.last_update_ns) = (Some(basis_ema_bps), now_ns);
    decision.basis_ema_bps = basis_ema_bps;
    decision.deviation_bps = decision.gap_bps - basis_ema_bps;
    let aster_depth = aster.bid_depth_5 + aster.ask_depth_5;
    decision.aster_imbalance = if aster_depth > 0.0 { (aster.bid_depth_5 - aster.ask_depth_5) / aster_depth } else { 0.0 };

    let threshold = params.entry_threshold_bps;
    let (long_threshold, short_threshold) = match state.bias {
        Bias::LongBias => (threshold / 2.0, threshold),
        Bias::ShortBias => (threshold, threshold / 2.0),
        Bias::Hold => (threshold, threshold),
    };
    let veto = params.imbalance_veto;
    (decision.bias, decision.reason) = if decision.deviation_bps > long_threshold {
        if decision.aster_imbalance > -veto {
            (Bias::LongBias, DecisionReason::Signal)
        } else {
            (Bias::Hold, DecisionReason::ImbalanceVeto)
        }
    } else if decision.deviation_bps < -short_threshold {
        if decision.aster_imbalance < veto {
            (Bias::ShortBias, DecisionReason::Signal)
        } else {
            (Bias::Hold, DecisionReason::ImbalanceVeto)
        }
    } else {
        (Bias::Hold, DecisionReason::BelowThreshold)
    };
    state.bias = decision.bias;
    decision
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND_NS: u64 = 1_000_000_000;

    fn top(bid: f64, ask: f64) -> Option<TopOfBook> {
        Some(TopOfBook { bid, bid_quantity: 5.0, ask, ask_quantity: 5.0 })
    }

    fn view(venue: Venue, bid: f64, ask: f64) -> BookView {
        BookView {
            venue,
            state: FeedState::Live,
            processing_transaction: false,
            valid: true,
            crossed: false,
            top: top(bid, ask),
            bid_depth_5: 10.0,
            ask_depth_5: 10.0,
            age_ms: 10.0,
            last_update_id: 1,
        }
    }

    fn base() -> (BookView, BookView, SignalState, SignalParams) {
        (view(Venue::Binance, 100.0, 100.01), view(Venue::Aster, 100.0, 100.01), SignalState::default(), SignalParams::default())
    }

    #[test]
    fn gates_block_with_reason() {
        let (binance, aster, mut state, params) = base();
        type Tweak = Box<dyn Fn(&mut BookView, &mut BookView)>;
        let cases: [(Tweak, DecisionReason); 8] = [
            (Box::new(|binance, _| binance.state = FeedState::Resyncing), DecisionReason::NotLive(Venue::Binance)),
            (Box::new(|_, aster| aster.processing_transaction = true), DecisionReason::ProcessingTransaction(Venue::Aster)),
            (Box::new(|_, aster| aster.valid = false), DecisionReason::Invalid(Venue::Aster)),
            (Box::new(|binance, _| binance.top = None), DecisionReason::Empty(Venue::Binance)),
            (Box::new(|_, aster| aster.crossed = true), DecisionReason::Crossed(Venue::Aster)),
            (Box::new(|binance, _| binance.age_ms = 5_000.0), DecisionReason::Stale(Venue::Binance)),
            (Box::new(|_, aster| aster.top = top(99.0, 101.0)), DecisionReason::WideSpread(Venue::Aster)),
            (Box::new(|binance, aster| (binance.age_ms, aster.age_ms) = (10.0, 900.0)), DecisionReason::AgeDifference),
        ];
        for (tweak, expected) in cases {
            let (mut binance, mut aster) = (binance, aster);
            tweak(&mut binance, &mut aster);
            let decision = decide(&binance, &aster, &mut state, &params, SECOND_NS, 0);
            assert_eq!((decision.bias, decision.reason), (Bias::Hold, expected));
        }
    }

    #[test]
    fn persistent_basis_is_not_a_signal() {
        let (mut binance, aster, mut state, params) = base();
        binance.top = top(100.05, 100.06);
        for second in 1..=5 {
            let decision = decide(&binance, &aster, &mut state, &params, second * SECOND_NS, 0);
            assert_eq!(decision.bias, Bias::Hold);
            assert!(decision.gap_bps > 4.0 && decision.deviation_bps.abs() < 1e-9);
        }
    }

    #[test]
    fn binance_jump_gives_bias_and_hysteresis_holds_it() {
        let (mut binance, aster, mut state, params) = base();
        decide(&binance, &aster, &mut state, &params, SECOND_NS, 0);
        binance.top = top(100.03, 100.04);
        let decision = decide(&binance, &aster, &mut state, &params, SECOND_NS + 1, 0);
        assert_eq!((decision.bias, decision.reason), (Bias::LongBias, DecisionReason::Signal), "{decision:?}");
        assert!(decision.deviation_bps > params.entry_threshold_bps);

        binance.top = top(100.0075, 100.0175);
        let decision = decide(&binance, &aster, &mut state, &params, SECOND_NS + 2, 0);
        let threshold = params.entry_threshold_bps;
        assert!(decision.deviation_bps > threshold / 2.0 && decision.deviation_bps < threshold, "{decision:?}");
        assert_eq!(decision.bias, Bias::LongBias, "hysteresis keeps the bias between half-threshold and threshold");

        binance.top = top(99.97, 99.98);
        assert_eq!(decide(&binance, &aster, &mut state, &params, SECOND_NS + 3, 0).bias, Bias::ShortBias);
    }

    #[test]
    fn opposite_aster_imbalance_vetoes() {
        let (mut binance, mut aster, mut state, params) = base();
        decide(&binance, &aster, &mut state, &params, SECOND_NS, 0);
        binance.top = top(100.03, 100.04);
        (aster.bid_depth_5, aster.ask_depth_5) = (1.0, 9.0);
        let decision = decide(&binance, &aster, &mut state, &params, SECOND_NS + 1, 0);
        assert_eq!((decision.bias, decision.reason), (Bias::Hold, DecisionReason::ImbalanceVeto));
        assert!((decision.aster_imbalance + 0.8).abs() < 1e-9);
    }

    #[test]
    fn microprice_leans_to_thin_side() {
        let top = TopOfBook { bid: 100.0, bid_quantity: 9.0, ask: 101.0, ask_quantity: 1.0 };
        assert!((top.microprice() - 100.9).abs() < 1e-9);
    }
}
