pub fn non_empty_owned(value: Option<&String>) -> Option<String> {
    value
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::non_empty_owned;

    #[test]
    fn discards_blank_strings() {
        let empty = String::from("   ");
        let value = String::from(" hello ");

        assert_eq!(non_empty_owned(Some(&empty)), None);
        assert_eq!(non_empty_owned(Some(&value)).as_deref(), Some("hello"));
    }
}
