use bigdecimal::{BigDecimal, Signed, ToPrimitive, Zero};
use num_bigint::BigInt;
use num_integer::Integer;

#[must_use]
pub fn round(src: BigDecimal, round_digits: i64) -> BigDecimal {
    let (_, decimal_part_digits) = src.as_bigint_and_exponent();
    if round_digits >= 0 && decimal_part_digits <= round_digits {
        return src;
    }

    let (bigint, decimal_part_digits) = src.as_bigint_and_exponent();
    let need_to_round_digits = decimal_part_digits - round_digits;
    let number = if need_to_round_digits > 1 {
        bigint.clone() / ten_to_the(need_to_round_digits as u64 - 1)
    } else {
        bigint.clone()
    };
    let sign = bigint.signum();
    let digit = (sign * number) % BigInt::from(10);
    let digit = digit.to_u8().unwrap(); // Unwrap is safe because x % 10 < 10

    if digit <= 4 {
        src.with_scale(round_digits)
    } else if bigint.is_negative() {
        src.with_scale(round_digits) - BigDecimal::new(BigInt::from(1), round_digits)
    } else {
        src.with_scale(round_digits) + BigDecimal::new(BigInt::from(1), round_digits)
    }
}

#[inline(always)]
fn ten_to_the(pow: u64) -> BigInt {
    if pow < 20 {
        BigInt::from(10_u64.pow(pow as u32))
    } else {
        let (half, rem) = pow.div_rem(&16);

        let mut x = ten_to_the(half);

        for _ in 0..4 {
            x = &x * &x;
        }

        if rem == 0 {
            x
        } else {
            x * ten_to_the(rem)
        }
    }
}

#[test]
fn test_round() {
    use std::str::FromStr;
    let test_cases = vec![
        ("1.45", 1, "1.5"),
        ("1.444445", 1, "1.4"),
        ("1.44", 1, "1.4"),
        ("0.444", 2, "0.44"),
        ("0.0045", 2, "0.00"),
        ("-1.555", 2, "-1.56"),
        ("-1.555", 99, "-1.555"),
        ("5.5", 0, "6"),
        ("-1", -1, "0"),
        ("5", -1, "10"),
        ("44", -1, "40"),
        ("44", -99, "0"),
        ("1.4499999999", 1, "1.4"),
        ("-1.4499999999", 1, "-1.4"),
        ("1.449999999", 1, "1.4"),
        ("-9999.444455556666", 10, "-9999.4444555567"),
        (
            "-12345678987654321.123456789",
            8,
            "-12345678987654321.12345679",
        ),
        (
            "1361.00189308912423129908350070049495058482123978686897440196109989199030002285434786855740011525483100",
            8,
            "1361.00189309",
        ),
    ];
    for &(x, digits, y) in test_cases.iter() {
        let a = BigDecimal::from_str(x).unwrap();
        let b = BigDecimal::from_str(y).unwrap();
        assert_eq!(
            round(a, digits),
            b,
            "Failed: round({}, {}) = {}",
            x,
            digits,
            y
        );
    }
}

#[must_use]
pub fn normalize(x: BigDecimal) -> BigDecimal {
    if x == BigDecimal::zero() {
        return BigDecimal::zero();
    }
    let (x_int_val, x_scale) = x.into_bigint_and_exponent();
    let (sign, mut digits) = x_int_val.to_radix_be(10);
    let trailing_count = digits.iter().rev().take_while(|i| **i == 0).count();
    let trunc_to = digits.len() - trailing_count as usize;
    digits.truncate(trunc_to);
    let int_val = BigInt::from_radix_be(sign, &digits, 10).unwrap();
    let scale = x_scale - trailing_count as i64;
    BigDecimal::new(int_val, scale)
}

#[test]
fn test_normalize() {
    let vals = vec![
        (
            BigDecimal::new(BigInt::from(10), 2),
            BigDecimal::new(BigInt::from(1), 1),
            "0.1",
        ),
        (
            BigDecimal::new(BigInt::from(132400), -4),
            BigDecimal::new(BigInt::from(1324), -6),
            "1324000000",
        ),
        (
            BigDecimal::new(BigInt::from(1_900_000), 3),
            BigDecimal::new(BigInt::from(19), -2),
            "1900",
        ),
        (
            BigDecimal::new(BigInt::from(0), -3),
            BigDecimal::zero(),
            "0",
        ),
        (BigDecimal::new(BigInt::from(0), 5), BigDecimal::zero(), "0"),
    ];

    for (not_normalized, normalized, string) in vals {
        assert_eq!(normalize(not_normalized.clone()), normalized);
        assert_eq!(normalize(not_normalized).to_string(), string);
        assert_eq!(normalized.to_string(), string);
    }
}
