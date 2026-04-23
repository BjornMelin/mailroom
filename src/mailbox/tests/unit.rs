use super::*;

#[test]
fn parses_yyyy_mm_dd_date_bounds() {
    assert_eq!(parse_start_of_day_epoch_ms("1970-01-01").unwrap(), 0);
    assert_eq!(
        parse_start_of_day_epoch_ms("1970-01-02").unwrap(),
        86_400_000
    );
    assert!(parse_start_of_day_epoch_ms("1970-1-01").is_err());
    assert!(parse_start_of_day_epoch_ms("12345-01-01").is_err());
    assert!(parse_start_of_day_epoch_ms("1970-13-01").is_err());
}

#[test]
fn newest_history_id_keeps_the_highest_seen_cursor() {
    let cursor = newest_history_id(Some(String::from("250")), "400");
    let cursor = newest_history_id(cursor, "300");

    assert_eq!(cursor.as_deref(), Some("400"));
}

#[test]
fn search_request_default_limit_is_nonzero() {
    let request = SearchRequest {
        terms: String::from("alpha"),
        label: None,
        from_address: None,
        after: None,
        before: None,
        limit: DEFAULT_SEARCH_LIMIT,
    };

    assert!(request.limit > 0);
}
