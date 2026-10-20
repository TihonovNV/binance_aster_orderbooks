/// Parses an unsigned decimal string into an integer scaled by `10^scale`.
/// Extra fractional digits are accepted only if they are zeros.
#[inline]
pub fn parse(s: &str, scale: u32) -> Option<i64> {
    let mut v: i64 = 0;
    let mut dot = false;
    let mut frac = 0u32;
    let mut digits = false;
    for &c in s.as_bytes() {
        match c {
            b'0'..=b'9' => {
                digits = true;
                if dot {
                    if frac == scale {
                        if c != b'0' {
                            return None;
                        }
                        continue;
                    }
                    frac += 1;
                }
                v = v.checked_mul(10)?.checked_add((c - b'0') as i64)?;
            }
            b'.' if !dot => dot = true,
            _ => return None,
        }
    }
    if !digits {
        return None;
    }
    v.checked_mul(10i64.pow(scale - frac))
}

pub fn to_f64(v: i64, scale: u32) -> f64 {
    v as f64 / 10f64.powi(scale as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses() {
        assert_eq!(parse("2401.80", 2), Some(240180));
        assert_eq!(parse("2401.8", 2), Some(240180));
        assert_eq!(parse("2401", 2), Some(240100));
        assert_eq!(parse("0.003", 3), Some(3));
        assert_eq!(parse("98.515", 3), Some(98515));
        assert_eq!(parse("0.00000000", 3), Some(0));
        assert_eq!(parse("1.2300", 2), Some(123));
        assert_eq!(parse(".5", 1), Some(5));
    }

    #[test]
    fn rejects() {
        for s in ["", ".", "1.234", "-1", "1e3", "1..2", "1.2.3", " 1", "99999999999999999999"] {
            assert_eq!(parse(s, 2), None, "{s:?}");
        }
    }

    #[test]
    fn roundtrip_f64() {
        assert_eq!(to_f64(240180, 2), 2401.8);
    }
}
