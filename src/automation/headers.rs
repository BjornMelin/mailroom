const PRECEDENCE_VALUES: [&str; 3] = ["bulk", "list", "junk"];

pub(crate) fn normalized_precedence(header: Option<&str>) -> Option<String> {
    let value = header?.trim().to_ascii_lowercase();
    PRECEDENCE_VALUES
        .iter()
        .find(|expected| has_ascii_token(&value, expected))
        .map(|matched| (*matched).to_owned())
}

pub(crate) fn match_precedence_values(
    candidate: Option<&str>,
    required_values: &[String],
) -> Option<Vec<String>> {
    if required_values.is_empty() {
        return Some(Vec::new());
    }
    let candidate = candidate?.trim().to_ascii_lowercase();
    let matches = required_values
        .iter()
        .filter(|required| {
            let required = required.trim().to_ascii_lowercase();
            !required.is_empty() && has_ascii_token(&candidate, &required)
        })
        .cloned()
        .collect::<Vec<_>>();
    (!matches.is_empty()).then_some(matches)
}

fn has_ascii_token(value: &str, expected: &str) -> bool {
    value
        .split(|character: char| !character.is_ascii_alphanumeric())
        .any(|token| token == expected)
}

#[cfg(test)]
mod tests {
    use super::{match_precedence_values, normalized_precedence};

    #[test]
    fn normalized_precedence_requires_exact_tokens() {
        assert_eq!(
            normalized_precedence(Some("bulk")),
            Some(String::from("bulk"))
        );
        assert_eq!(
            normalized_precedence(Some("x-priority; list")),
            Some(String::from("list"))
        );
        assert_eq!(normalized_precedence(Some("notbulk")), None);
        assert_eq!(normalized_precedence(Some("xlistx")), None);
    }

    #[test]
    fn match_precedence_values_requires_exact_tokens() {
        assert_eq!(
            match_precedence_values(Some("x-priority; bulk"), &[String::from("bulk")]),
            Some(vec![String::from("bulk")])
        );
        assert_eq!(
            match_precedence_values(Some("notbulk"), &[String::from("bulk")]),
            None
        );
    }
}
