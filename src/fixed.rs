/// Parses an unsigned decimal string into an integer scaled by `10^scale`.
/// Extra fractional digits are accepted only if they are zeros.
#[inline]
pub fn parse(text: &str, scale: u32) -> Option<i64> {
    let mut value: i64 = 0;
    let mut seen_dot = false;
    let mut fraction_digits = 0u32;
    let mut seen_digit = false;
    for &byte in text.as_bytes() {
        match byte {
            b'0'..=b'9' => {
                seen_digit = true;
                if seen_dot {
                    if fraction_digits == scale {
                        if byte != b'0' {
                            return None;
                        }
                        continue;
                    }
                    fraction_digits += 1;
                }
                value = value.checked_mul(10)?.checked_add((byte - b'0') as i64)?;
            }
            b'.' if !seen_dot => seen_dot = true,
            _ => return None,
        }
    }
    if !seen_digit {
        return None;
    }
    value.checked_mul(10i64.pow(scale - fraction_digits))
}

pub fn to_f64(value: i64, scale: u32) -> f64 {
    value as f64 / 10f64.powi(scale as i32)
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
        for text in ["", ".", "1.234", "-1", "1e3", "1..2", "1.2.3", " 1", "99999999999999999999"] {
            assert_eq!(parse(text, 2), None, "{text:?}");
        }
    }

    #[test]
    fn roundtrip_f64() {
        assert_eq!(to_f64(240180, 2), 2401.8);
    }
}
