use ring::{digest, hmac};
use std::collections::BTreeMap;
use std::fmt::Write;
use std::time::SystemTime;

pub struct AWSSignedRequest {
    pub url: String,
    pub headers: BTreeMap<String, String>,
}

fn to_hex(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2);
    for b in data {
        write!(s, "{:02x}", b).unwrap();
    }
    s
}

fn sha256(data: &[u8]) -> String {
    let d = digest::digest(&digest::SHA256, data);
    to_hex(d.as_ref())
}

fn hmac_sha256_raw(key: &[u8], data: &[u8]) -> Vec<u8> {
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    let tag = hmac::sign(&k, data);
    tag.as_ref().to_vec()
}

fn hmac_sha256_hex(key: &[u8], data: &[u8]) -> String {
    let raw = hmac_sha256_raw(key, data);
    to_hex(&raw)
}

fn get_timestamp() -> String {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Format as YYYYMMDDTHHMMSSZ
    let secs_per_day: u64 = 86400;
    let mut days = now / secs_per_day;
    let day_secs = now % secs_per_day;
    let hour = day_secs / 3600;
    let minute = (day_secs % 3600) / 60;
    let second = day_secs % 60;

    // Calculate year/month/day from days since epoch
    let mut year: u64 = 1970;
    loop {
        let days_in_year = if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
            366
        } else {
            365
        };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }
    let is_leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days: [u64; 12] = [
        31,
        if is_leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month: u64 = 1;
    for &md in &month_days {
        if days < md {
            break;
        }
        days -= md;
        month += 1;
    }
    let day = days + 1;

    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        year, month, day, hour, minute, second
    )
}

fn get_date(timestamp: &str) -> &str {
    &timestamp[..8]
}

fn uri_encode(value: &str, encode_slash: bool) -> String {
    let mut encoded = String::with_capacity(value.len() * 3);
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
            encoded.push(b as char);
        } else if b == b'/' && !encode_slash {
            encoded.push('/');
        } else {
            write!(encoded, "%{:02X}", b).unwrap();
        }
    }
    encoded
}

fn url_encode(value: &str) -> String {
    uri_encode(value, true)
}

fn sort_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }

    let mut params: Vec<(&str, &str)> = query
        .split('&')
        .map(|param| {
            if let Some(eq) = param.find('=') {
                (&param[..eq], &param[eq + 1..])
            } else {
                (param, "")
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

/// Sign an AWS request using Signature Version 4.
pub fn aws_sign_request(
    method: &str,
    host: &str,
    path: &str,
    query: &str,
    region: &str,
    service: &str,
    access_key: &str,
    secret_key: &str,
    payload: &[u8],
    session_token: &str,
) -> AWSSignedRequest {
    let timestamp = get_timestamp();
    let date = get_date(&timestamp);

    let payload_hash = sha256(payload);

    let canonical_uri = if path.is_empty() {
        "/".to_string()
    } else {
        uri_encode(path, false)
    };

    let canonical_query = sort_query_string(query);

    // Canonical headers
    let mut canonical_headers = format!("host:{}\n", host);
    canonical_headers.push_str(&format!("x-amz-content-sha256:{}\n", payload_hash));
    canonical_headers.push_str(&format!("x-amz-date:{}\n", timestamp));
    if !session_token.is_empty() {
        canonical_headers.push_str(&format!("x-amz-security-token:{}\n", session_token));
    }

    let mut signed_headers = "host;x-amz-content-sha256;x-amz-date".to_string();
    if !session_token.is_empty() {
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

    // Signing key chain
    let k_secret = format!("AWS4{}", secret_key);
    let k_date = hmac_sha256_raw(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256_raw(&k_date, region.as_bytes());
    let k_service = hmac_sha256_raw(&k_region, service.as_bytes());
    let k_signing = hmac_sha256_raw(&k_service, b"aws4_request");

    let signature = hmac_sha256_hex(&k_signing, string_to_sign.as_bytes());

    let authorization = format!(
        "{} Credential={}/{}, SignedHeaders={}, Signature={}",
        algorithm, access_key, credential_scope, signed_headers, signature
    );

    let mut url = format!("https://{}{}", host, canonical_uri);
    if !canonical_query.is_empty() {
        url.push('?');
        url.push_str(&canonical_query);
    }

    let mut headers = BTreeMap::new();
    headers.insert("Host".to_string(), host.to_string());
    headers.insert("x-amz-date".to_string(), timestamp);
    headers.insert("x-amz-content-sha256".to_string(), payload_hash);
    headers.insert("Authorization".to_string(), authorization);
    if !session_token.is_empty() {
        headers.insert("x-amz-security-token".to_string(), session_token.to_string());
    }

    AWSSignedRequest { url, headers }
}

/// Generate a presigned URL for S3 object download.
pub fn aws_generate_presigned_url(
    bucket: &str,
    key: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
    session_token: &str,
    expires_seconds: i32,
) -> String {
    let timestamp = get_timestamp();
    let date = get_date(&timestamp);

    let host = format!("{}.s3.{}.amazonaws.com", bucket, region);
    let canonical_uri = format!("/{}", uri_encode(key, false));
    let payload_hash = "UNSIGNED-PAYLOAD";

    let credential_scope = format!("{}/{}/s3/aws4_request", date, region);
    let credential = format!("{}/{}", access_key, credential_scope);

    let mut query = format!("X-Amz-Algorithm=AWS4-HMAC-SHA256");
    write!(query, "&X-Amz-Credential={}", url_encode(&credential)).unwrap();
    write!(query, "&X-Amz-Date={}", timestamp).unwrap();
    write!(query, "&X-Amz-Expires={}", expires_seconds).unwrap();
    if !session_token.is_empty() {
        write!(query, "&X-Amz-Security-Token={}", url_encode(session_token)).unwrap();
    }
    write!(query, "&X-Amz-SignedHeaders=host").unwrap();

    let canonical_headers = format!("host:{}\n", host);
    let signed_headers = "host";

    let canonical_request = format!(
        "GET\n{}\n{}\n{}\n{}\n{}",
        canonical_uri, query, canonical_headers, signed_headers, payload_hash
    );

    let algorithm = "AWS4-HMAC-SHA256";
    let string_to_sign = format!(
        "{}\n{}\n{}\n{}",
        algorithm,
        timestamp,
        credential_scope,
        sha256(canonical_request.as_bytes())
    );

    let k_secret = format!("AWS4{}", secret_key);
    let k_date = hmac_sha256_raw(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256_raw(&k_date, region.as_bytes());
    let k_service = hmac_sha256_raw(&k_region, b"s3");
    let k_signing = hmac_sha256_raw(&k_service, b"aws4_request");

    let signature = hmac_sha256_hex(&k_signing, string_to_sign.as_bytes());

    format!(
        "https://{}{}?{}&X-Amz-Signature={}",
        host, canonical_uri, query, signature
    )
}
