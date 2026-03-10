/// Extract the content of a single XML tag: `<tag>content</tag>`
pub fn extract_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(&open)?;
    let content_start = start + open.len();
    let end = xml[content_start..].find(&close)?;
    Some(xml[content_start..content_start + end].to_string())
}

/// Extract an S3 error from XML (Code + Message)
pub fn extract_error(xml: &str) -> Option<String> {
    let code = extract_tag(xml, "Code")?;
    let message = extract_tag(xml, "Message").unwrap_or_default();
    Some(format!("{}: {}", code, message))
}
