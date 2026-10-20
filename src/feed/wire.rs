use crate::book::Level;
use crate::fixed;
use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use std::fmt;

/// Decimal places used to turn price and quantity strings into integers.
#[derive(Clone, Copy, Debug)]
pub struct Scales {
    pub price: u32,
    pub quantity: u32,
}

/// Sequencing and timing fields of a depth message.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Header {
    /// `U`: first update id covered by this event.
    pub first_update_id: u64,
    /// `u` (or `lastUpdateId` in a REST snapshot): last update id covered.
    pub last_update_id: u64,
    /// `pu`: last update id of the previous event in the stream.
    pub prev_update_id: u64,
    /// `E`: exchange event time.
    pub event_time_ms: u64,
    /// `T`: exchange transaction time.
    pub transaction_time_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireError {
    Invalid,
    MissingUpdateId,
}

/// Reusable parse target: depth diff (`U u pu E T b a`) or REST snapshot (`lastUpdateId E T bids asks`),
/// optionally wrapped in a combined-stream envelope (`{"stream":..,"data":{..}}`).
/// Parsing is a serde seed straight into the reused level buffers: borrowed `&str`s, no allocation.
pub struct Depth {
    pub header: Header,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

impl Default for Depth {
    fn default() -> Self {
        Self { header: Header::default(), bids: Vec::with_capacity(2048), asks: Vec::with_capacity(2048) }
    }
}

impl Depth {
    pub fn parse(&mut self, json: &str, scales: Scales) -> Result<(), WireError> {
        self.header = Header::default();
        self.bids.clear();
        self.asks.clear();
        let mut deserializer = sonic_rs::Deserializer::from_str(json);
        let has_update_id = DepthSeed { depth: self, scales }.deserialize(&mut deserializer).map_err(|_| WireError::Invalid)?;
        deserializer.end().map_err(|_| WireError::Invalid)?;
        if has_update_id { Ok(()) } else { Err(WireError::MissingUpdateId) }
    }
}

/// Fills a `Depth` from a JSON object. Returns whether an update id was present.
struct DepthSeed<'a> {
    depth: &'a mut Depth,
    scales: Scales,
}

impl<'de> DeserializeSeed<'de> for DepthSeed<'_> {
    type Value = bool;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<bool, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for DepthSeed<'_> {
    type Value = bool;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("depth object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<bool, A::Error> {
        let (depth, scales) = (self.depth, self.scales);
        let mut has_update_id = false;
        while let Some(key) = map.next_key::<&str>()? {
            match key {
                "data" => has_update_id |= map.next_value_seed(DepthSeed { depth: &mut *depth, scales })?,
                "U" => depth.header.first_update_id = map.next_value()?,
                "u" | "lastUpdateId" => {
                    depth.header.last_update_id = map.next_value()?;
                    has_update_id = true;
                }
                "pu" => depth.header.prev_update_id = map.next_value()?,
                "E" => depth.header.event_time_ms = map.next_value()?,
                "T" => depth.header.transaction_time_ms = map.next_value()?,
                "b" | "bids" => map.next_value_seed(LevelsSeed { levels: &mut depth.bids, scales })?,
                "a" | "asks" => map.next_value_seed(LevelsSeed { levels: &mut depth.asks, scales })?,
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(has_update_id)
    }
}

/// Appends `[["price", "quantity"], ...]` to a level buffer as fixed-point integers.
struct LevelsSeed<'a> {
    levels: &'a mut Vec<Level>,
    scales: Scales,
}

impl<'de> DeserializeSeed<'de> for LevelsSeed<'_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for LevelsSeed<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("[[price, quantity], ...]")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while let Some((price_text, quantity_text)) = seq.next_element::<(&str, &str)>()? {
            let (Some(price), Some(quantity)) =
                (fixed::parse(price_text, self.scales.price), fixed::parse(quantity_text, self.scales.quantity))
            else {
                return Err(de::Error::custom("bad decimal"));
            };
            self.levels.push(Level { price, quantity });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCALES: Scales = Scales { price: 2, quantity: 3 };

    fn level(price: i64, quantity: i64) -> Level {
        Level { price, quantity }
    }

    #[test]
    fn diff_in_envelope() {
        let json = r#"{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1792431094434,"T":1792431094400,"s":"ETHUSDT","U":10,"u":12,"pu":9,"ps":"x","b":[["2401.80","0.003"],["2401.78","2.180"]],"a":[["2401.81","0"]]}}"#;
        let mut depth = Depth::default();
        depth.parse(json, SCALES).unwrap();
        assert_eq!(
            depth.header,
            Header {
                first_update_id: 10,
                last_update_id: 12,
                prev_update_id: 9,
                event_time_ms: 1792431094434,
                transaction_time_ms: 1792431094400
            }
        );
        assert_eq!(depth.bids, [level(240180, 3), level(240178, 2180)]);
        assert_eq!(depth.asks, [level(240181, 0)]);
    }

    #[test]
    fn snapshot() {
        let json = r#"{"lastUpdateId":610786745237,"E":1,"T":2,"bids":[["2401.80","0.003"]],"asks":[["2401.81","3.752"]]}"#;
        let mut depth = Depth::default();
        depth.parse(json, SCALES).unwrap();
        assert_eq!(depth.header.last_update_id, 610786745237);
        assert_eq!(depth.asks, [level(240181, 3752)]);
    }

    #[test]
    fn errors() {
        let mut depth = Depth::default();
        assert_eq!(depth.parse(r#"{"E":1}"#, SCALES), Err(WireError::MissingUpdateId));
        for bad in [r#"{"u":1,"b":[["1.234","1"]]}"#, r#"{"u":1,"b":[["1"]]}"#, r#"{"u":1,"b":[["1","2"]"#, r#"{"u":"x"}"#, "[]", ""] {
            assert_eq!(depth.parse(bad, SCALES), Err(WireError::Invalid), "{bad}");
        }
    }
}
