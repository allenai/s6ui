/// Extract the content of a single XML tag: `<tag>content</tag>`
pub fn extract_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(&open)?;
    let content_start = start + open.len();
    let end = xml[content_start..].find(&close)?;
    Some(xml[content_start..content_start + end].to_string())
}

/// Extract all occurrences of a tag from XML
pub fn extract_all_tags(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let mut results = Vec::new();
    let mut pos = 0;

    while let Some(start) = xml[pos..].find(&open) {
        let abs_start = pos + start + open.len();
        if let Some(end) = xml[abs_start..].find(&close) {
            results.push(xml[abs_start..abs_start + end].to_string());
            pos = abs_start + end + close.len();
        } else {
            break;
        }
    }

    results
}

/// Extract an S3 error from XML (Code + Message)
pub fn extract_error(xml: &str) -> Option<String> {
    let code = extract_tag(xml, "Code")?;
    let message = extract_tag(xml, "Message").unwrap_or_default();
    Some(format!("{}: {}", code, message))
}
