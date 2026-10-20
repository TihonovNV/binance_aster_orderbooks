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
pub enum Why {
    #[default]
    Init,
    Signal,
    BelowThreshold,
    ImbalanceVeto,
    NotLive(Venue),
    InTxn(Venue),
    Invalid(Venue),
    Empty(Venue),
    Crossed(Venue),
    Stale(Venue),
    WideSpread(Venue),
    Skew,
}

/// What the signal needs from one book, taken by the engine between transactions.
#[derive(Clone, Copy, Debug)]
pub struct Snap {
    pub venue: Venue,
    pub state: FeedState,
    pub in_txn: bool,
    pub valid: bool,
    pub crossed: bool,
    pub bbo: Option<Bbo>,
    pub bid5: f64,
    pub ask5: f64,
    pub age_ms: f64,
    pub seq: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Bbo {
    pub bid: f64,
    pub bid_qty: f64,
    pub ask: f64,
    pub ask_qty: f64,
}

impl Bbo {
    fn mid(&self) -> f64 {
        (self.bid + self.ask) / 2.0
    }
    fn micro(&self) -> f64 {
        (self.bid * self.ask_qty + self.ask * self.bid_qty) / (self.bid_qty + self.ask_qty)
    }
    fn spread_bps(&self) -> f64 {
        (self.ask - self.bid) / self.mid() * 1e4
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct Decision {
    pub bias: Bias,
    pub why: Why,
    pub ts_ms: u64,
    pub bid_b: f64,
    pub ask_b: f64,
    pub micro_b: f64,
    pub bid_a: f64,
    pub ask_a: f64,
    pub mid_a: f64,
    pub gap_bps: f64,
    pub ema_bps: f64,
    pub dev_bps: f64,
    pub imb_a: f64,
    pub age_b_ms: f64,
    pub age_a_ms: f64,
    pub seq_b: u64,
    pub seq_a: u64,
}

#[derive(Default)]
pub struct SignalState {
    ema: Option<f64>,
    last_ns: u64,
    bias: Bias,
}

fn gate(s: &Snap, p: &SignalParams) -> Option<Why> {
    let v = s.venue;
    let Some(bbo) = s.bbo else { return Some(Why::Empty(v)) };
    if s.state != FeedState::Live {
        Some(Why::NotLive(v))
    } else if s.in_txn {
        Some(Why::InTxn(v))
    } else if !s.valid {
        Some(Why::Invalid(v))
    } else if s.crossed {
        Some(Why::Crossed(v))
    } else if s.age_ms > p.max_age_ms as f64 {
        Some(Why::Stale(v))
    } else if bbo.spread_bps() > p.max_spread_bps {
        Some(Why::WideSpread(v))
    } else {
        None
    }
}

/// Lead-lag: Binance microprice vs Aster mid, de-biased by an EMA of the gap,
/// confirmed by Aster top-5 imbalance, with enter θ / exit θ/2 hysteresis.
pub fn decide(b: &Snap, a: &Snap, st: &mut SignalState, p: &SignalParams, now_ns: u64, ts_ms: u64) -> Decision {
    let mut d = Decision { ts_ms, age_b_ms: b.age_ms, age_a_ms: a.age_ms, seq_b: b.seq, seq_a: a.seq, ..Default::default() };
    if let Some(x) = b.bbo {
        (d.bid_b, d.ask_b, d.micro_b) = (x.bid, x.ask, x.micro());
    }
    if let Some(x) = a.bbo {
        (d.bid_a, d.ask_a, d.mid_a) = (x.bid, x.ask, x.mid());
    }
    let blocked = gate(b, p).or_else(|| gate(a, p)).or(((b.age_ms - a.age_ms).abs() > p.max_skew_ms as f64).then_some(Why::Skew));
    if let Some(why) = blocked {
        st.bias = Bias::Hold;
        d.why = why;
        return d;
    }

    d.gap_bps = (d.micro_b - d.mid_a) / d.mid_a * 1e4;
    let ema = match st.ema {
        None => d.gap_bps,
        Some(e) => {
            let dt = (now_ns.saturating_sub(st.last_ns)) as f64 / 1e9;
            e + (1.0 - (-dt / p.ema_secs).exp()) * (d.gap_bps - e)
        }
    };
    (st.ema, st.last_ns) = (Some(ema), now_ns);
    d.ema_bps = ema;
    d.dev_bps = d.gap_bps - ema;
    let depth = a.bid5 + a.ask5;
    d.imb_a = if depth > 0.0 { (a.bid5 - a.ask5) / depth } else { 0.0 };

    let (long_th, short_th) = match st.bias {
        Bias::LongBias => (p.theta_bps / 2.0, p.theta_bps),
        Bias::ShortBias => (p.theta_bps, p.theta_bps / 2.0),
        Bias::Hold => (p.theta_bps, p.theta_bps),
    };
    (d.bias, d.why) = if d.dev_bps > long_th {
        if d.imb_a > -p.kappa { (Bias::LongBias, Why::Signal) } else { (Bias::Hold, Why::ImbalanceVeto) }
    } else if d.dev_bps < -short_th {
        if d.imb_a < p.kappa { (Bias::ShortBias, Why::Signal) } else { (Bias::Hold, Why::ImbalanceVeto) }
    } else {
        (Bias::Hold, Why::BelowThreshold)
    };
    st.bias = d.bias;
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: u64 = 1_000_000_000;

    fn snap(venue: Venue, bid: f64, ask: f64, bid_qty: f64, ask_qty: f64) -> Snap {
        Snap {
            venue,
            state: FeedState::Live,
            in_txn: false,
            valid: true,
            crossed: false,
            bbo: Some(Bbo { bid, bid_qty, ask, ask_qty }),
            bid5: 10.0,
            ask5: 10.0,
            age_ms: 10.0,
            seq: 1,
        }
    }

    fn base() -> (Snap, Snap, SignalState, SignalParams) {
        (
            snap(Venue::Binance, 100.0, 100.01, 5.0, 5.0),
            snap(Venue::Aster, 100.0, 100.01, 5.0, 5.0),
            SignalState::default(),
            SignalParams::default(),
        )
    }

    #[test]
    fn gates_block_with_reason() {
        let (b, a, mut st, p) = base();
        type Tweak = Box<dyn Fn(&mut Snap, &mut Snap)>;
        let cases: [(Tweak, Why); 8] = [
            (Box::new(|b, _| b.state = FeedState::Resyncing), Why::NotLive(Venue::Binance)),
            (Box::new(|_, a| a.in_txn = true), Why::InTxn(Venue::Aster)),
            (Box::new(|_, a| a.valid = false), Why::Invalid(Venue::Aster)),
            (Box::new(|b, _| b.bbo = None), Why::Empty(Venue::Binance)),
            (Box::new(|_, a| a.crossed = true), Why::Crossed(Venue::Aster)),
            (Box::new(|b, _| b.age_ms = 5_000.0), Why::Stale(Venue::Binance)),
            (Box::new(|_, a| a.bbo = Some(Bbo { bid: 99.0, bid_qty: 1.0, ask: 101.0, ask_qty: 1.0 })), Why::WideSpread(Venue::Aster)),
            (Box::new(|b, a| (b.age_ms, a.age_ms) = (10.0, 900.0)), Why::Skew),
        ];
        for (f, why) in cases {
            let (mut b, mut a) = (b, a);
            f(&mut b, &mut a);
            let d = decide(&b, &a, &mut st, &p, S, 0);
            assert_eq!((d.bias, d.why), (Bias::Hold, why));
        }
    }

    #[test]
    fn persistent_basis_is_not_a_signal() {
        let (mut b, a, mut st, p) = base();
        b.bbo = Some(Bbo { bid: 100.05, bid_qty: 5.0, ask: 100.06, ask_qty: 5.0 });
        for i in 1..=5 {
            let d = decide(&b, &a, &mut st, &p, i * S, 0);
            assert_eq!(d.bias, Bias::Hold);
            assert!(d.gap_bps > 4.0 && d.dev_bps.abs() < 1e-9);
        }
    }

    #[test]
    fn binance_jump_gives_bias_and_hysteresis_holds_it() {
        let (mut b, a, mut st, p) = base();
        decide(&b, &a, &mut st, &p, S, 0);
        b.bbo = Some(Bbo { bid: 100.03, bid_qty: 5.0, ask: 100.04, ask_qty: 5.0 });
        let d = decide(&b, &a, &mut st, &p, S + 1, 0);
        assert_eq!((d.bias, d.why), (Bias::LongBias, Why::Signal), "{d:?}");
        assert!(d.dev_bps > p.theta_bps);

        b.bbo = Some(Bbo { bid: 100.0075, bid_qty: 5.0, ask: 100.0175, ask_qty: 5.0 });
        let d = decide(&b, &a, &mut st, &p, S + 2, 0);
        assert!(d.dev_bps > p.theta_bps / 2.0 && d.dev_bps < p.theta_bps, "{d:?}");
        assert_eq!(d.bias, Bias::LongBias, "hysteresis keeps the bias between θ/2 and θ");

        b.bbo = Some(Bbo { bid: 99.97, bid_qty: 5.0, ask: 99.98, ask_qty: 5.0 });
        assert_eq!(decide(&b, &a, &mut st, &p, S + 3, 0).bias, Bias::ShortBias);
    }

    #[test]
    fn opposite_aster_imbalance_vetoes() {
        let (mut b, mut a, mut st, p) = base();
        decide(&b, &a, &mut st, &p, S, 0);
        b.bbo = Some(Bbo { bid: 100.03, bid_qty: 5.0, ask: 100.04, ask_qty: 5.0 });
        (a.bid5, a.ask5) = (1.0, 9.0);
        let d = decide(&b, &a, &mut st, &p, S + 1, 0);
        assert_eq!((d.bias, d.why), (Bias::Hold, Why::ImbalanceVeto));
        assert!((d.imb_a + 0.8).abs() < 1e-9);
    }

    #[test]
    fn microprice_leans_to_thin_side() {
        let x = Bbo { bid: 100.0, bid_qty: 9.0, ask: 101.0, ask_qty: 1.0 };
        assert!((x.micro() - 100.9).abs() < 1e-9);
    }
}
