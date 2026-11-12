//! The value encoding of the CloudEvents NATS binding: a `ce-` header value is percent-encoded,
//! so it holds only printable ASCII without space, `"` or `%`. A receiver also accepts the
//! double-quoted strings that older versions of the binding allowed.

use std::fmt::Write as _;

/// Percent-encodes an attribute value as the binding says: every byte of a space, `"`, `%` or a
/// character outside U+0021 to U+007E becomes `%XY`, in upper case.
pub(crate) fn encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for character in value.chars() {
        if matches!(character, '!'..='~') && !matches!(character, '"' | '%') {
            encoded.push(character);
        } else {
            let mut bytes = [0; 4];
            for byte in character.encode_utf8(&mut bytes).bytes() {
                write!(encoded, "%{byte:02X}").expect("writing to a String doesn't fail");
            }
        }
    }
    encoded
}

/// Removes the double quotes of the quoted strings in a header value and their backslash
/// escapes, as RFC 7230 section 3.2.6 describes them.
fn unquote(value: &str) -> Result<String, String> {
    let mut unquoted = String::with_capacity(value.len());
    let mut quoted = false;
    let mut characters = value.chars();
    while let Some(character) = characters.next() {
        match character {
            '"' => quoted = !quoted,
            '\\' if quoted => match characters.next() {
                Some(escaped) => unquoted.push(escaped),
                None => break,
            },
            character => unquoted.push(character),
        }
    }
    if quoted {
        return Err(format!(
            "the header value {value:?} has an unterminated quoted string"
        ));
    }
    Ok(unquoted)
}

/// Decodes a header value as the binding says: its quoted strings are unquoted, then it is
/// percent-decoded once. Characters encoded without need are accepted.
///
/// # Errors
///
/// When a quoted string isn't closed, a `%` isn't followed by two hex digits, or the decoded
/// bytes aren't UTF-8.
pub(crate) fn decode(value: &str) -> Result<String, String> {
    let unquoted = unquote(value)?;
    let mut bytes = Vec::with_capacity(unquoted.len());
    let mut rest = unquoted.as_bytes();
    while let Some((&byte, after)) = rest.split_first() {
        if byte == b'%' {
            let hex = after
                .get(..2)
                .filter(|hex| hex.iter().all(u8::is_ascii_hexdigit))
                .and_then(|hex| std::str::from_utf8(hex).ok())
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
                .ok_or_else(|| format!("the header value {value:?} has a bad percent-encoding"))?;
            bytes.push(hex);
            rest = &after[2..];
        } else {
            bytes.push(byte);
            rest = after;
        }
    }
    String::from_utf8(bytes)
        .map_err(|_| format!("the header value {value:?} doesn't decode to UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_the_binding_example() {
        // The example in the binding's "Metadata Headers" section.
        assert_eq!(encode("Euro € 😀"), "Euro%20%E2%82%AC%20%F0%9F%98%80");
    }

    #[test]
    fn encodes_space_quote_percent_and_controls() {
        assert_eq!(encode(r#"a "b" 100%"#), "a%20%22b%22%20100%25");
        assert_eq!(encode("42\r\nce-type: x"), "42%0D%0Ace-type:%20x");
        assert_eq!(encode("\u{7f}\u{85}"), "%7F%C2%85");
    }

    #[test]
    fn leaves_printable_ascii() {
        let printable = "/orders:0190f5a4-8c3b-7d2e-9f10-1a2b3c4d5e6f;charset=utf-8+json~!";
        assert_eq!(encode(printable), printable);
    }

    #[test]
    fn decodes_what_it_encodes() {
        for value in ["Euro € 😀", r#"a "b" 100%"#, "42\r\n", "", "plain"] {
            assert_eq!(decode(&encode(value)).as_deref(), Ok(value));
        }
    }

    #[test]
    fn decodes_needless_encoding_and_lower_case() {
        assert_eq!(decode("%2forders%e2%82%ac").as_deref(), Ok("/orders€"));
    }

    #[test]
    fn unquotes_before_percent_decoding() {
        assert_eq!(decode(r#""order%2042""#).as_deref(), Ok("order 42"));
        assert_eq!(
            decode(r#""say \"hi\" \\ 100%25""#).as_deref(),
            Ok(r#"say "hi" \ 100%"#)
        );
        assert_eq!(decode(r#"a"b c"d"#).as_deref(), Ok("ab cd"));
        // A backslash outside quotes is kept.
        assert_eq!(decode(r"a\b").as_deref(), Ok(r"a\b"));
    }

    #[test]
    fn rejects_an_unterminated_quoted_string() {
        assert!(decode(r#""order"#).is_err());
        assert!(decode(r#""order\""#).is_err());
        assert!(decode(r#""order\"#).is_err());
    }

    #[test]
    fn rejects_bad_percent_encoding_and_invalid_utf8() {
        assert!(decode("100%").is_err());
        assert!(decode("%4").is_err());
        assert!(decode("%zz").is_err());
        assert!(decode("%+1").is_err());
        // An overlong encoding of a space, the binding's own example.
        assert!(decode("%C0%A0").is_err());
    }
}
