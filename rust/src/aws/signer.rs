use ring::digest;
use ring::hmac;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::time::SystemTime;

/// Result of signing an AWS request
pub struct SignedRequest {
    pub url: String,
    pub headers: BTreeMap<String, String>,
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(s, "{:02x}", b).unwrap();
    }
    s
}

fn sha256(data: &[u8]) -> String {
    let digest = digest::digest(&digest::SHA256, data);
    to_hex(digest.as_ref())
}

fn hmac_sha256_raw(key: &[u8], data: &[u8]) -> Vec<u8> {
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    let tag = hmac::sign(&k, data);
    tag.as_ref().to_vec()
}

fn hmac_sha256_hex(key: &[u8], data: &[u8]) -> String {
    to_hex(&hmac_sha256_raw(key, data))
}

/// Get current UTC timestamp in AWS format (YYYYMMDDTHHMMSSz)
fn get_timestamp() -> String {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Manual UTC time formatting (avoid chrono dependency)
    let secs = now;
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Calculate year/month/day from days since epoch (1970-01-01)
    let (year, month, day) = days_to_ymd(days as i64);

    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

fn days_to_ymd(days: i64) -> (i64, i64, i64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// URI-encode a value. If encode_slash is false, '/' characters are preserved.
pub fn uri_encode(value: &str, encode_slash: bool) -> String {
    let mut result = String::with_capacity(value.len() * 3);
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
            result.push(b as char);
        } else if b == b'/' && !encode_slash {
            result.push('/');
        } else {
            write!(result, "%{:02X}", b).unwrap();
        }
    }
    result
}

/// URL-encode a value (RFC 3986 - encodes everything including slashes)
pub fn url_encode(value: &str) -> String {
    uri_encode(value, true)
}

/// Sort query string parameters alphabetically
fn sort_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }

    let mut params: Vec<(&str, &str)> = query
        .split('&')
        .map(|p| {
            if let Some(eq) = p.find('=') {
                (&p[..eq], &p[eq + 1..])
            } else {
                (p, "")
            }
        })
        .collect();

    params.sort();

    params
        .iter()
        .enumerate()
        .fold(String::new(), |mut acc, (i, (k, v))| {
            if i > 0 {
                acc.push('&');
            }
            acc.push_str(k);
            if !v.is_empty() {
                acc.push('=');
                acc.push_str(v);
            }
            acc
        })
}

/// Sign an AWS request using Signature Version 4
pub fn sign_request(
    method: &str,
    host: &str,
    path: &str,
    query: &str,
    region: &str,
    service: &str,
    access_key: &str,
    secret_key: &str,
    payload: &str,
    session_token: &str,
) -> SignedRequest {
    let timestamp = get_timestamp();
    let date = &timestamp[..8];

    let payload_hash = sha256(payload.as_bytes());

    // Canonical URI - must be URI-encoded but keep slashes
    let canonical_uri = if path.is_empty() {
        "/".to_string()
    } else {
        uri_encode(path, false)
    };

    let canonical_query = sort_query_string(query);

    // Canonical headers (must be sorted, lowercase)
    let mut canonical_headers = format!(
        "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
        host, payload_hash, timestamp
    );
    let mut signed_headers = "host;x-amz-content-sha256;x-amz-date".to_string();

    if !session_token.is_empty() {
        canonical_headers.push_str(&format!("x-amz-security-token:{}\n", session_token));
        signed_headers.push_str(";x-amz-security-token");
    }

    // Canonical request
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method, canonical_uri, canonical_query, canonical_headers, signed_headers, payload_hash
    );

    // String to sign
    let algorithm = "AWS4-HMAC-SHA256";
    let credential_scope = format!("{}/{}/{}/aws4_request", date, region, service);
    let string_to_sign = format!(
        "{}\n{}\n{}\n{}",
        algorithm,
        timestamp,
        credential_scope,
        sha256(canonical_request.as_bytes())
    );

    // Signing key
    let k_secret = format!("AWS4{}", secret_key);
    let k_date = hmac_sha256_raw(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256_raw(&k_date, region.as_bytes());
    let k_service = hmac_sha256_raw(&k_region, service.as_bytes());
    let k_signing = hmac_sha256_raw(&k_service, b"aws4_request");

    let signature = hmac_sha256_hex(&k_signing, string_to_sign.as_bytes());

    // Authorization header
    let authorization = format!(
        "{} Credential={}/{}, SignedHeaders={}, Signature={}",
        algorithm, access_key, credential_scope, signed_headers, signature
    );

    // Build URL
    let mut url = format!("https://{}{}", host, canonical_uri);
    if !canonical_query.is_empty() {
        url.push('?');
        url.push_str(&canonical_query);
    }

    // Build headers
    let mut headers = BTreeMap::new();
    headers.insert("Host".to_string(), host.to_string());
    headers.insert("x-amz-date".to_string(), timestamp);
    headers.insert("x-amz-content-sha256".to_string(), payload_hash);
    headers.insert("Authorization".to_string(), authorization);
    if !session_token.is_empty() {
        headers.insert("x-amz-security-token".to_string(), session_token.to_string());
    }

    SignedRequest { url, headers }
}
