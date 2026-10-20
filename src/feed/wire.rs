use crate::book::Level;
use crate::fixed;
use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use std::fmt;

#[derive(Clone, Copy, Debug)]
pub struct Scales {
    pub px: u32,
    pub qty: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Header {
    pub first_u: u64,
    pub u: u64,
    pub pu: u64,
    pub event_ms: u64,
    pub tx_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireError {
    Invalid,
    MissingSeq,
}

/// Reusable parse target: depth diff (`U u pu E T b a`) or REST snapshot (`lastUpdateId E T bids asks`),
/// optionally wrapped in a combined-stream envelope (`{"stream":..,"data":{..}}`).
/// Parsing is a serde seed straight into the reused level buffers: borrowed `&str`s, no allocation.
pub struct Depth {
    pub hdr: Header,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

impl Default for Depth {
    fn default() -> Self {
        Self { hdr: Header::default(), bids: Vec::with_capacity(2048), asks: Vec::with_capacity(2048) }
    }
}

impl Depth {
    pub fn parse(&mut self, json: &str, sc: Scales) -> Result<(), WireError> {
        self.hdr = Header::default();
        self.bids.clear();
        self.asks.clear();
        let mut de = sonic_rs::Deserializer::from_str(json);
        let has_u = Seed { d: self, sc }.deserialize(&mut de).map_err(|_| WireError::Invalid)?;
        de.end().map_err(|_| WireError::Invalid)?;
        if has_u { Ok(()) } else { Err(WireError::MissingSeq) }
    }
}

struct Seed<'a> {
    d: &'a mut Depth,
    sc: Scales,
}

impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = bool;
    fn deserialize<D: Deserializer<'de>>(self, de: D) -> Result<bool, D::Error> {
        de.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for Seed<'_> {
    type Value = bool;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("depth object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut m: A) -> Result<bool, A::Error> {
        let (d, sc) = (self.d, self.sc);
        let mut has_u = false;
        while let Some(k) = m.next_key::<&str>()? {
            match k {
                "data" => has_u |= m.next_value_seed(Seed { d: &mut *d, sc })?,
                "U" => d.hdr.first_u = m.next_value()?,
                "u" | "lastUpdateId" => {
                    d.hdr.u = m.next_value()?;
                    has_u = true;
                }
                "pu" => d.hdr.pu = m.next_value()?,
                "E" => d.hdr.event_ms = m.next_value()?,
                "T" => d.hdr.tx_ms = m.next_value()?,
                "b" | "bids" => m.next_value_seed(Levels { out: &mut d.bids, sc })?,
                "a" | "asks" => m.next_value_seed(Levels { out: &mut d.asks, sc })?,
                _ => {
                    m.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(has_u)
    }
}

struct Levels<'a> {
    out: &'a mut Vec<Level>,
    sc: Scales,
}

impl<'de> DeserializeSeed<'de> for Levels<'_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, de: D) -> Result<(), D::Error> {
        de.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for Levels<'_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("[[price, qty], ...]")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut s: A) -> Result<(), A::Error> {
        while let Some((p, q)) = s.next_element::<(&str, &str)>()? {
            let (Some(px), Some(qty)) = (fixed::parse(p, self.sc.px), fixed::parse(q, self.sc.qty)) else {
                return Err(de::Error::custom("bad decimal"));
            };
            self.out.push(Level { px, qty });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SC: Scales = Scales { px: 2, qty: 3 };

    #[test]
    fn diff_in_envelope() {
        let j = r#"{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1792431094434,"T":1792431094400,"s":"ETHUSDT","U":10,"u":12,"pu":9,"ps":"x","b":[["2401.80","0.003"],["2401.78","2.180"]],"a":[["2401.81","0"]]}}"#;
        let mut d = Depth::default();
        d.parse(j, SC).unwrap();
        assert_eq!(d.hdr, Header { first_u: 10, u: 12, pu: 9, event_ms: 1792431094434, tx_ms: 1792431094400 });
        assert_eq!(d.bids, [Level { px: 240180, qty: 3 }, Level { px: 240178, qty: 2180 }]);
        assert_eq!(d.asks, [Level { px: 240181, qty: 0 }]);
    }

    #[test]
    fn snapshot() {
        let j = r#"{"lastUpdateId":610786745237,"E":1,"T":2,"bids":[["2401.80","0.003"]],"asks":[["2401.81","3.752"]]}"#;
        let mut d = Depth::default();
        d.parse(j, SC).unwrap();
        assert_eq!(d.hdr.u, 610786745237);
        assert_eq!(d.asks, [Level { px: 240181, qty: 3752 }]);
    }

    #[test]
    fn errors() {
        let mut d = Depth::default();
        assert_eq!(d.parse(r#"{"E":1}"#, SC), Err(WireError::MissingSeq));
        for bad in [r#"{"u":1,"b":[["1.234","1"]]}"#, r#"{"u":1,"b":[["1"]]}"#, r#"{"u":1,"b":[["1","2"]"#, r#"{"u":"x"}"#, "[]", ""] {
            assert_eq!(d.parse(bad, SC), Err(WireError::Invalid), "{bad}");
        }
    }
}
