//! Time range and date range utils

use chrono::{DateTime, Duration, NaiveDate, Utc};
use std::fmt;

/// Timestamp range, using `chrono::DateTime<Utc>`, each bound is optional.
#[derive(Clone, PartialEq, Eq)]
pub struct UtcTimeRangeOpt {
    /// Time interval start, inclusive, optional.
    pub timestamp_gte: Option<DateTime<Utc>>,
    /// Time interval end, exclusive, optional.
    pub timestamp_lt: Option<DateTime<Utc>>,
}

/// Date range, using `chrono::NaiveDate`, each bound is optional.
#[derive(Clone, PartialEq, Eq)]
pub struct DateRangeOpt {
    /// Start date, inclusive, optional.
    pub date_from: Option<NaiveDate>,
    /// End date, inclusive, optional.
    pub date_to: Option<NaiveDate>,
}

impl fmt::Debug for UtcTimeRangeOpt {
    #[rustfmt::skip]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[ {:?} ... {:?} )", self.timestamp_gte, self.timestamp_lt)
    }
}

impl fmt::Debug for DateRangeOpt {
    #[rustfmt::skip]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[ {:?} ... {:?} ]", self.date_from, self.date_to)
    }
}

impl UtcTimeRangeOpt {
    pub fn to_date_range(&self) -> DateRangeOpt {
        let milli = Duration::milliseconds(1);
        DateRangeOpt {
            date_from: self.timestamp_gte.map(|dt| dt.naive_utc().date()),
            date_to: self.timestamp_lt.map(|dt| (dt - milli).naive_utc().date()),
        }
    }
}

#[test]
fn test_date_range() {
    #[rustfmt::skip]
    let dt = |s| Some(DateTime::parse_from_rfc3339(s).unwrap().naive_utc().and_utc());
    let d = |s| Some(NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap());

    // End date not included
    let tr = UtcTimeRangeOpt {
        timestamp_gte: dt("2023-09-01T00:00:00Z"),
        timestamp_lt: dt("2023-09-05T00:00:00Z"),
    };
    let dr = tr.to_date_range();
    assert_eq!(dr.date_from, d("2023-09-01"));
    assert_eq!(dr.date_to, d("2023-09-04"));

    // End date included
    let tr = UtcTimeRangeOpt {
        timestamp_gte: dt("2023-09-01T00:00:00Z"),
        timestamp_lt: dt("2023-09-05T23:59:59Z"),
    };
    let dr = tr.to_date_range();
    assert_eq!(dr.date_from, d("2023-09-01"));
    assert_eq!(dr.date_to, d("2023-09-05"));
}
