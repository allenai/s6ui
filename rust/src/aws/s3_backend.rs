use crate::aws::credentials::{AWSProfile, refresh_profile_credentials};
use crate::aws::signer::{aws_sign_request, AWSSignedRequest};
use crate::backend::{Backend, CancelFlag};
use crate::events::{S3Bucket, S3Object, StateEvent};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Write as FmtWrite;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};

// ---------------------------------------------------------------------------
// WorkItem
// ---------------------------------------------------------------------------

enum WorkItem {
    ListBuckets,
    ListObjects {
        bucket: String,
        prefix: String,
        continuation_token: String,
        cancel_flag: Option<CancelFlag>,
    },
    GetObject {
        bucket: String,
        key: String,
        max_bytes: Option<usize>,
        cancel_flag: Option<CancelFlag>,
    },
    GetObjectRange {
        bucket: String,
        key: String,
        start_byte: usize,
        end_byte: usize,
        cancel_flag: Option<CancelFlag>,
    },
    GetObjectStreaming {
        bucket: String,
        key: String,
        start_byte: usize,
        total_size: usize,
        cancel_flag: Option<CancelFlag>,
    },
}

// ---------------------------------------------------------------------------
// S3Backend
// ---------------------------------------------------------------------------

pub struct S3Backend {
    profile: Arc<Mutex<AWSProfile>>,
    _event_tx: mpsc::Sender<StateEvent>,
    event_rx: Mutex<mpsc::Receiver<StateEvent>>,
    high_queue: Arc<Mutex<VecDeque<WorkItem>>>,
    low_queue: Arc<Mutex<VecDeque<WorkItem>>>,
    high_notify: Arc<tokio::sync::Notify>,
    low_notify: Arc<tokio::sync::Notify>,
    shutdown: Arc<AtomicBool>,
    hover_cancel: Mutex<Option<CancelFlag>>,
    region_cache: Arc<Mutex<HashMap<String, String>>>,
    _runtime_handle: std::thread::JoinHandle<()>,
    request_lag_seconds: Arc<Mutex<f32>>,
}

impl Drop for S3Backend {
    fn drop(&mut self) {
        eprintln!("S3Backend: shutting down");
        self.shutdown.store(true, Ordering::SeqCst);
        self.high_notify.notify_waiters();
        self.low_notify.notify_waiters();
        // The runtime thread will exit when the runtime is dropped after
        // all workers see shutdown==true.
    }
}

// ---------------------------------------------------------------------------
// Helpers (free functions)
// ---------------------------------------------------------------------------

/// Strip scheme and trailing slash / path from an endpoint URL, returning just
/// the host (and optional port).
fn parse_endpoint_host(endpoint_url: &str) -> String {
    let mut url = endpoint_url;
    if let Some(rest) = url.strip_prefix("https://") {
        url = rest;
    } else if let Some(rest) = url.strip_prefix("http://") {
        url = rest;
    }
    // Remove trailing slash
    let url = url.trim_end_matches('/');
    // Remove path component (keep only host:port)
    match url.find('/') {
        Some(pos) => url[..pos].to_string(),
        None => url.to_string(),
    }
}

/// Percent-encode a value for use in query parameters (encodes `/` too).
fn url_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() * 3);
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
            encoded.push(b as char);
        } else {
            write!(encoded, "%{:02X}", b).unwrap();
        }
    }
    encoded
}

/// Simple XML tag extraction (mirrors C++ `extractTag`).
fn extract_tag(xml: &str, tag: &str) -> String {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    if let Some(start) = xml.find(&open) {
        let content_start = start + open.len();
        if let Some(end) = xml[content_start..].find(&close) {
            return xml[content_start..content_start + end].to_string();
        }
    }
    String::new()
}

/// Extract an S3 error string from XML (`<Code>: <Message>`).
fn extract_error(xml: &str) -> String {
    let code = extract_tag(xml, "Code");
    let message = extract_tag(xml, "Message");
    if !code.is_empty() {
        return format!("{}: {}", code, message);
    }
    String::new()
}

/// Try to extract region from an S3-style endpoint host string.
fn extract_region_from_endpoint(endpoint: &str) -> String {
    // Patterns:
    //   bucket.s3.region.amazonaws.com
    //   bucket.s3-region.amazonaws.com
    //   s3.region.amazonaws.com
    //   s3.amazonaws.com  (global, no region)

    // Global endpoint – no region
    if endpoint == "s3.amazonaws.com" {
        return String::new();
    }

    // Look for "s3." or "s3-"
    let (s3_pos, region_start) = if let Some(pos) = endpoint.find("s3.") {
        (pos, pos + 3)
    } else if let Some(pos) = endpoint.find("s3-") {
        (pos, pos + 3)
    } else {
        return String::new();
    };
    let _ = s3_pos; // suppress unused

    let rest = &endpoint[region_start..];
    let region_end = match rest.find('.') {
        Some(pos) if pos > 0 => pos,
        _ => return String::new(),
    };

    let region = &rest[..region_end];

    // Must contain a dash to look like a real region (e.g. "us-east-1")
    if !region.contains('-') {
        return String::new();
    }

    region.to_string()
}

/// Parse `<ListAllMyBucketsResult>` XML into a vec of `S3Bucket`.
fn parse_list_buckets_xml(xml: &str) -> Vec<S3Bucket> {
    let mut buckets = Vec::new();
    let search = "<Bucket>";
    let end_search = "</Bucket>";
    let mut pos = 0;

    while let Some(start) = xml[pos..].find(search) {
        let abs_start = pos + start;
        let after_tag = abs_start + search.len();
        if let Some(end) = xml[after_tag..].find(end_search) {
            let abs_end = after_tag + end + end_search.len();
            let bucket_xml = &xml[abs_start..abs_end];

            let name = extract_tag(bucket_xml, "Name");
            let creation_date = extract_tag(bucket_xml, "CreationDate");
            if !name.is_empty() {
                buckets.push(S3Bucket {
                    name,
                    creation_date,
                });
            }
            pos = abs_end;
        } else {
            break;
        }
    }
    buckets
}

/// Result of parsing `<ListBucketResult>` XML.
struct ListObjectsResult {
    objects: Vec<S3Object>,
    next_continuation_token: String,
    is_truncated: bool,
    error: String,
}

/// Parse `<ListBucketResult>` XML into objects + pagination metadata.
fn parse_list_objects_xml(xml: &str) -> ListObjectsResult {
    let mut result = ListObjectsResult {
        objects: Vec::new(),
        next_continuation_token: String::new(),
        is_truncated: false,
        error: String::new(),
    };

    // Check for error first
    result.error = extract_error(xml);
    if !result.error.is_empty() {
        return result;
    }

    // Truncation
    let truncated = extract_tag(xml, "IsTruncated");
    result.is_truncated = truncated == "true";

    // Continuation token
    result.next_continuation_token = extract_tag(xml, "NextContinuationToken");

    // Common prefixes (folders)
    {
        let search = "<CommonPrefixes>";
        let end_tag = "</CommonPrefixes>";
        let mut pos = 0;
        while let Some(start) = xml[pos..].find(search) {
            let abs_start = pos + start;
            let after = abs_start + search.len();
            if let Some(end) = xml[after..].find(end_tag) {
                let abs_end = after + end;
                let prefix_xml = &xml[abs_start..abs_end];
                let prefix = extract_tag(prefix_xml, "Prefix");
                if !prefix.is_empty() {
                    let mut display = prefix.clone();
                    if display.ends_with('/') {
                        display.pop();
                    }
                    let display_name = match display.rfind('/') {
                        Some(idx) => display[idx + 1..].to_string(),
                        None => display,
                    };
                    result.objects.push(S3Object {
                        key: prefix,
                        display_name,
                        size: 0,
                        last_modified: String::new(),
                        is_folder: true,
                    });
                }
                pos = abs_end + end_tag.len();
            } else {
                break;
            }
        }
    }

    // Contents (files)
    {
        let search = "<Contents>";
        let end_tag = "</Contents>";
        let mut pos = 0;
        while let Some(start) = xml[pos..].find(search) {
            let abs_start = pos + start;
            let after = abs_start + search.len();
            if let Some(end) = xml[after..].find(end_tag) {
                let abs_end = after + end;
                let contents_xml = &xml[abs_start..abs_end];

                let key = extract_tag(contents_xml, "Key");
                let size_str = extract_tag(contents_xml, "Size");
                let size: i64 = size_str.parse().unwrap_or(0);
                let last_modified = extract_tag(contents_xml, "LastModified");

                let display_name = match key.rfind('/') {
                    Some(idx) => key[idx + 1..].to_string(),
                    None => key.clone(),
                };

                // Skip folder markers
                if !key.is_empty() && !key.ends_with('/') {
                    result.objects.push(S3Object {
                        key,
                        display_name,
                        size,
                        last_modified,
                        is_folder: false,
                    });
                }

                pos = abs_end + end_tag.len();
            } else {
                break;
            }
        }
    }

    result
}

/// Build the host and path for a given bucket + key combo, taking into account
/// custom endpoint vs virtual-host style.
fn build_host_path(
    profile: &AWSProfile,
    region: &str,
    bucket: &str,
    key: &str,
) -> (String, String) {
    if !profile.endpoint_url.is_empty() {
        let host = parse_endpoint_host(&profile.endpoint_url);
        let mut path = format!("/{}", bucket);
        if !key.is_empty() {
            path.push('/');
            path.push_str(key);
        }
        (host, path)
    } else {
        let host = format!("{}.s3.{}.amazonaws.com", bucket, region);
        let path = if key.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", key)
        };
        (host, path)
    }
}

/// Known AWS regions for heuristic bucket-name matching.
const KNOWN_REGIONS: &[&str] = &[
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-central-1",
    "eu-north-1",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-south-1",
    "ca-central-1",
    "sa-east-1",
];

/// Attempt to determine the correct region from a PermanentRedirect response.
fn resolve_redirect_region(response_body: &str, bucket: &str, current_region: &str) -> Option<String> {
    let correct_endpoint = extract_tag(response_body, "Endpoint");
    eprintln!(
        "S3Backend: PermanentRedirect, endpoint in response: '{}'",
        correct_endpoint
    );

    let mut correct_region = String::new();

    // 1. Try extracting from endpoint
    if !correct_endpoint.is_empty() {
        correct_region = extract_region_from_endpoint(&correct_endpoint);
    }

    // 2. Try extracting from bucket name
    if correct_region.is_empty() {
        let bucket_lower = bucket.to_lowercase();
        for &r in KNOWN_REGIONS {
            if bucket_lower.contains(r) {
                correct_region = r.to_string();
                eprintln!("S3Backend: extracted region from bucket name: {}", r);
                break;
            }
        }
    }

    // 3. Fallback to us-east-1
    if correct_region.is_empty() {
        correct_region = "us-east-1".to_string();
        eprintln!("S3Backend: falling back to default region: us-east-1");
    }

    if !correct_region.is_empty() && correct_region != current_region {
        eprintln!(
            "S3Backend: detected PermanentRedirect, retrying with region={} (was {})",
            correct_region, current_region
        );
        Some(correct_region)
    } else {
        eprintln!(
            "S3Backend: PermanentRedirect but could not determine correct region \
             (endpoint: '{}', bucket: '{}')",
            correct_endpoint, bucket
        );
        None
    }
}

/// Clear the cancel flag on a WorkItem (used when boosting from low to high priority).
fn clear_cancel_flag(item: &mut WorkItem) {
    match item {
        WorkItem::ListBuckets => {}
        WorkItem::ListObjects { cancel_flag, .. }
        | WorkItem::GetObject { cancel_flag, .. }
        | WorkItem::GetObjectRange { cancel_flag, .. }
        | WorkItem::GetObjectStreaming { cancel_flag, .. } => {
            *cancel_flag = None;
        }
    }
}

// ---------------------------------------------------------------------------
// Async worker / work-item processing
// ---------------------------------------------------------------------------

async fn worker(
    queue: Arc<Mutex<VecDeque<WorkItem>>>,
    notify: Arc<tokio::sync::Notify>,
    shutdown: Arc<AtomicBool>,
    client: reqwest::Client,
    profile: Arc<Mutex<AWSProfile>>,
    event_tx: mpsc::Sender<StateEvent>,
    region_cache: Arc<Mutex<HashMap<String, String>>>,
    request_lag: Arc<Mutex<f32>>,
) {
    loop {
        let item = {
            let mut q = queue.lock().unwrap();
            q.pop_front()
        };
        match item {
            Some(item) => {
                process_work_item(
                    item,
                    &client,
                    &profile,
                    &event_tx,
                    &region_cache,
                    &request_lag,
                )
                .await;
            }
            None => {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                notify.notified().await;
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    }
}

/// Send an event to the main thread. Silently drops if the receiver is gone.
fn push_event(tx: &mpsc::Sender<StateEvent>, event: StateEvent) {
    let _ = tx.send(event);
}

/// Build a `reqwest::RequestBuilder` with the signed headers applied.
fn build_request(
    client: &reqwest::Client,
    signed: &AWSSignedRequest,
    extra_headers: &BTreeMap<String, String>,
) -> reqwest::RequestBuilder {
    let mut builder = client.get(&signed.url);
    for (k, v) in &signed.headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    for (k, v) in extra_headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    builder
}

/// Perform a simple HTTP GET returning the response body as a string, or an
/// error prefixed with `"ERROR: "`.  If `cancel_flag` is set before/during the
/// request we return `"CANCELLED"`.
async fn http_get(
    client: &reqwest::Client,
    signed: &AWSSignedRequest,
    extra_headers: &BTreeMap<String, String>,
    cancel_flag: &Option<CancelFlag>,
) -> String {
    if let Some(cf) = cancel_flag {
        if cf.load(Ordering::Relaxed) {
            return "CANCELLED".to_string();
        }
    }

    let request = build_request(client, signed, extra_headers);
    let response = match request.send().await {
        Ok(r) => r,
        Err(e) => return format!("ERROR: {}", e),
    };

    match response.text().await {
        Ok(body) => body,
        Err(e) => format!("ERROR: {}", e),
    }
}

/// Snapshot the profile fields we need for signing under a single lock.
struct ProfileSnapshot {
    region: String,
    endpoint_url: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
}

fn snapshot_profile(profile: &Mutex<AWSProfile>) -> ProfileSnapshot {
    let p = profile.lock().unwrap();
    ProfileSnapshot {
        region: p.region.clone(),
        endpoint_url: p.endpoint_url.clone(),
        access_key_id: p.access_key_id.clone(),
        secret_access_key: p.secret_access_key.clone(),
        session_token: p.session_token.clone(),
    }
}

fn get_cached_region(cache: &Mutex<HashMap<String, String>>, bucket: &str) -> String {
    cache
        .lock()
        .unwrap()
        .get(bucket)
        .cloned()
        .unwrap_or_default()
}

fn cache_region(cache: &Mutex<HashMap<String, String>>, bucket: &str, region: &str) {
    cache
        .lock()
        .unwrap()
        .insert(bucket.to_string(), region.to_string());
    eprintln!(
        "S3Backend: cached region for bucket={} region={}",
        bucket, region
    );
}

/// Construct a fake `AWSProfile` from a snapshot (only the fields needed by
/// `build_host_path`).
fn snapshot_as_profile(snap: &ProfileSnapshot) -> AWSProfile {
    AWSProfile {
        endpoint_url: snap.endpoint_url.clone(),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// process_work_item
// ---------------------------------------------------------------------------

async fn process_work_item(
    item: WorkItem,
    client: &reqwest::Client,
    profile: &Arc<Mutex<AWSProfile>>,
    event_tx: &mpsc::Sender<StateEvent>,
    region_cache: &Arc<Mutex<HashMap<String, String>>>,
    request_lag: &Arc<Mutex<f32>>,
) {
    // Apply artificial lag for testing
    {
        let lag = *request_lag.lock().unwrap();
        if lag > 0.0 {
            let millis = (lag * 1000.0) as u64;
            eprintln!("S3Backend: applying {}ms lag to request", millis);
            tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
        }
    }

    match item {
        WorkItem::ListBuckets => {
            process_list_buckets(client, profile, event_tx).await;
        }
        WorkItem::ListObjects { bucket, prefix, continuation_token, cancel_flag } => {
            process_list_objects(&bucket, &prefix, &continuation_token, &cancel_flag, client, profile, event_tx, region_cache).await;
        }
        WorkItem::GetObject { bucket, key, max_bytes, cancel_flag } => {
            process_get_object(&bucket, &key, max_bytes, &cancel_flag, client, profile, event_tx, region_cache).await;
        }
        WorkItem::GetObjectRange { bucket, key, start_byte, end_byte, cancel_flag } => {
            process_get_object_range(&bucket, &key, start_byte, end_byte, &cancel_flag, client, profile, event_tx, region_cache).await;
        }
        WorkItem::GetObjectStreaming { bucket, key, start_byte, total_size, cancel_flag } => {
            process_get_object_streaming(&bucket, &key, start_byte, total_size, &cancel_flag, client, profile, event_tx, region_cache).await;
        }
    }
}

// ---- ListBuckets ----------------------------------------------------------

async fn process_list_buckets(
    client: &reqwest::Client,
    profile: &Arc<Mutex<AWSProfile>>,
    event_tx: &mpsc::Sender<StateEvent>,
) {
    let snap = snapshot_profile(profile);

    let host = if !snap.endpoint_url.is_empty() {
        parse_endpoint_host(&snap.endpoint_url)
    } else {
        format!("s3.{}.amazonaws.com", snap.region)
    };

    let signed = aws_sign_request(
        "GET",
        &host,
        "/",
        "",
        &snap.region,
        "s3",
        &snap.access_key_id,
        &snap.secret_access_key,
        &[],
        &snap.session_token,
    );

    let empty_headers = BTreeMap::new();
    let response = http_get(client, &signed, &empty_headers, &None).await;

    if response.starts_with("ERROR:") {
        eprintln!("S3Backend: listBuckets HTTP error: {}", response);
        push_event(
            event_tx,
            StateEvent::BucketsLoadError {
                error_message: response,
            },
        );
    } else {
        let error = extract_error(&response);
        if !error.is_empty() {
            eprintln!("S3Backend: listBuckets S3 error: {}", error);
            push_event(
                event_tx,
                StateEvent::BucketsLoadError {
                    error_message: error,
                },
            );
        } else {
            let buckets = parse_list_buckets_xml(&response);
            eprintln!("S3Backend: listBuckets success, got {} buckets", buckets.len());
            push_event(event_tx, StateEvent::BucketsLoaded { buckets });
        }
    }
}

// ---- ListObjects ----------------------------------------------------------

async fn process_list_objects(
    bucket: &str,
    prefix: &str,
    continuation_token: &str,
    cancel_flag: &Option<CancelFlag>,
    client: &reqwest::Client,
    profile: &Arc<Mutex<AWSProfile>>,
    event_tx: &mpsc::Sender<StateEvent>,
    region_cache: &Arc<Mutex<HashMap<String, String>>>,
) {
    let snap = snapshot_profile(profile);
    let cached = get_cached_region(region_cache, bucket);
    let mut region = if cached.is_empty() {
        snap.region.clone()
    } else {
        cached
    };

    if region.is_empty() {
        push_event(
            event_tx,
            StateEvent::ObjectsLoadError {
                bucket: bucket.to_string(),
                prefix: prefix.to_string(),
                error_message: "ERROR: Region not configured. Please ensure your AWS profile has a valid region.".to_string(),
            },
        );
        return;
    }

    let fake_profile = snapshot_as_profile(&snap);

    for attempt in 0..2 {
        let (host, path) = build_host_path(&fake_profile, &region, bucket, "");

        // Build query string
        let mut query = String::from("list-type=2");
        write!(query, "&delimiter={}", url_encode("/")).unwrap();
        write!(query, "&max-keys=1000").unwrap();
        if !prefix.is_empty() {
            write!(query, "&prefix={}", url_encode(prefix)).unwrap();
        }
        if !continuation_token.is_empty() {
            write!(
                query,
                "&continuation-token={}",
                url_encode(continuation_token)
            )
            .unwrap();
        }

        let signed = aws_sign_request(
            "GET",
            &host,
            &path,
            &query,
            &region,
            "s3",
            &snap.access_key_id,
            &snap.secret_access_key,
            &[],
            &snap.session_token,
        );

        let empty_headers = BTreeMap::new();
        let response = http_get(client, &signed, &empty_headers, cancel_flag).await;

        if response == "CANCELLED" {
            eprintln!(
                "S3Backend: listObjects cancelled bucket={} prefix={}",
                bucket, prefix
            );
            return;
        }

        if response.starts_with("ERROR:") {
            eprintln!("S3Backend: listObjects HTTP error: {}", response);
            push_event(
                event_tx,
                StateEvent::ObjectsLoadError {
                    bucket: bucket.to_string(),
                    prefix: prefix.to_string(),
                    error_message: response,
                },
            );
            return;
        }

        let result = parse_list_objects_xml(&response);

        if !result.error.is_empty() {
            // PermanentRedirect handling
            let error_code = extract_tag(&response, "Code");
            if error_code == "PermanentRedirect" && attempt == 0 {
                if let Some(new_region) = resolve_redirect_region(&response, bucket, &region)
                {
                    region = new_region.clone();
                    cache_region(region_cache, bucket, &new_region);
                    continue; // retry
                }
            }

            eprintln!("S3Backend: listObjects S3 error: {}", result.error);
            push_event(
                event_tx,
                StateEvent::ObjectsLoadError {
                    bucket: bucket.to_string(),
                    prefix: prefix.to_string(),
                    error_message: result.error,
                },
            );
            return;
        }

        // Cache region on success
        cache_region(region_cache, bucket, &region);

        eprintln!(
            "S3Backend: listObjects success bucket={} prefix={} count={} truncated={}",
            bucket,
            prefix,
            result.objects.len(),
            result.is_truncated
        );

        push_event(
            event_tx,
            StateEvent::ObjectsLoaded {
                bucket: bucket.to_string(),
                prefix: prefix.to_string(),
                continuation_token: continuation_token.to_string(),
                objects: result.objects,
                next_continuation_token: result.next_continuation_token,
                is_truncated: result.is_truncated,
            },
        );
        return;
    }
}

// ---- GetObject ------------------------------------------------------------

async fn process_get_object(
    bucket: &str,
    key: &str,
    max_bytes: Option<usize>,
    cancel_flag: &Option<CancelFlag>,
    client: &reqwest::Client,
    profile: &Arc<Mutex<AWSProfile>>,
    event_tx: &mpsc::Sender<StateEvent>,
    region_cache: &Arc<Mutex<HashMap<String, String>>>,
) {
    let snap = snapshot_profile(profile);
    let cached = get_cached_region(region_cache, bucket);
    let mut region = if cached.is_empty() {
        snap.region.clone()
    } else {
        cached
    };

    if region.is_empty() {
        push_event(
            event_tx,
            StateEvent::ObjectContentLoadError {
                bucket: bucket.to_string(),
                key: key.to_string(),
                error_message: "ERROR: Region not configured. Please ensure your AWS profile has a valid region.".to_string(),
            },
        );
        return;
    }

    let fake_profile = snapshot_as_profile(&snap);

    for attempt in 0..2 {
        let (host, path) = build_host_path(&fake_profile, &region, bucket, key);

        let signed = aws_sign_request(
            "GET",
            &host,
            &path,
            "",
            &region,
            "s3",
            &snap.access_key_id,
            &snap.secret_access_key,
            &[],
            &snap.session_token,
        );

        // Optional Range header (not signed)
        let mut extra_headers = BTreeMap::new();
        if let Some(mb) = max_bytes {
            extra_headers.insert(
                "Range".to_string(),
                format!("bytes=0-{}", mb - 1),
            );
        }

        let response = http_get(client, &signed, &extra_headers, cancel_flag).await;

        if response == "CANCELLED" {
            eprintln!(
                "S3Backend: getObject cancelled bucket={} key={}",
                bucket, key
            );
            return;
        }

        if response.starts_with("ERROR:") {
            eprintln!("S3Backend: getObject HTTP error: {}", response);
            push_event(
                event_tx,
                StateEvent::ObjectContentLoadError {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    error_message: response,
                },
            );
            return;
        }

        // Check for S3 XML error
        let error = extract_error(&response);
        if !error.is_empty() {
            let error_code = extract_tag(&response, "Code");

            // PermanentRedirect retry
            if error_code == "PermanentRedirect" && attempt == 0 {
                if let Some(new_region) = resolve_redirect_region(&response, bucket, &region)
                {
                    region = new_region.clone();
                    cache_region(region_cache, bucket, &new_region);
                    continue;
                }
            }

            // InvalidRange means the file is 0 bytes
            if error_code == "InvalidRange" {
                eprintln!(
                    "S3Backend: getObject empty file (InvalidRange) bucket={} key={}",
                    bucket, key
                );
                push_event(
                    event_tx,
                    StateEvent::ObjectContentLoaded {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        content: Vec::new(),
                    },
                );
                return;
            }

            eprintln!("S3Backend: getObject S3 error: {}", error);
            push_event(
                event_tx,
                StateEvent::ObjectContentLoadError {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    error_message: error,
                },
            );
            return;
        }

        // Cache region on success
        cache_region(region_cache, bucket, &region);

        eprintln!(
            "S3Backend: getObject success bucket={} key={} size={}",
            bucket,
            key,
            response.len()
        );
        push_event(
            event_tx,
            StateEvent::ObjectContentLoaded {
                bucket: bucket.to_string(),
                key: key.to_string(),
                content: response.into_bytes(),
            },
        );
        return;
    }
}

// ---- GetObjectRange -------------------------------------------------------

async fn process_get_object_range(
    bucket: &str,
    key: &str,
    start_byte: usize,
    end_byte: usize,
    cancel_flag: &Option<CancelFlag>,
    client: &reqwest::Client,
    profile: &Arc<Mutex<AWSProfile>>,
    event_tx: &mpsc::Sender<StateEvent>,
    region_cache: &Arc<Mutex<HashMap<String, String>>>,
) {
    let snap = snapshot_profile(profile);
    let cached = get_cached_region(region_cache, bucket);
    let mut region = if cached.is_empty() {
        snap.region.clone()
    } else {
        cached
    };

    if region.is_empty() {
        push_event(
            event_tx,
            StateEvent::ObjectRangeLoadError {
                bucket: bucket.to_string(),
                key: key.to_string(),
                start_byte,
                error_message: "ERROR: Region not configured. Please ensure your AWS profile has a valid region.".to_string(),
            },
        );
        return;
    }

    let fake_profile = snapshot_as_profile(&snap);

    for attempt in 0..2 {
        let (host, path) = build_host_path(&fake_profile, &region, bucket, key);

        let signed = aws_sign_request(
            "GET",
            &host,
            &path,
            "",
            &region,
            "s3",
            &snap.access_key_id,
            &snap.secret_access_key,
            &[],
            &snap.session_token,
        );

        // Range header
        let mut extra_headers = BTreeMap::new();
        extra_headers.insert(
            "Range".to_string(),
            format!("bytes={}-{}", start_byte, end_byte),
        );

        // Check cancel before request
        if let Some(cf) = cancel_flag {
            if cf.load(Ordering::Relaxed) {
                eprintln!(
                    "S3Backend: getObjectRange cancelled bucket={} key={}",
                    bucket, key
                );
                return;
            }
        }

        let request = build_request(client, &signed, &extra_headers);
        let response = match request.send().await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("S3Backend: getObjectRange HTTP error: {}", e);
                push_event(
                    event_tx,
                    StateEvent::ObjectRangeLoadError {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        start_byte,
                        error_message: format!("ERROR: {}", e),
                    },
                );
                return;
            }
        };

        // Parse Content-Range header for total size
        let content_range_total = response
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                // Format: bytes 0-1023/12345
                v.find('/').and_then(|slash| {
                    v[slash + 1..].trim().parse::<usize>().ok()
                })
            })
            .unwrap_or(0);

        let body = match response.bytes().await {
            Ok(b) => b,
            Err(e) => {
                eprintln!("S3Backend: getObjectRange body read error: {}", e);
                push_event(
                    event_tx,
                    StateEvent::ObjectRangeLoadError {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        start_byte,
                        error_message: format!("ERROR: {}", e),
                    },
                );
                return;
            }
        };

        let body_str = String::from_utf8_lossy(&body);

        // Check for S3 XML error
        let error = extract_error(&body_str);
        if !error.is_empty() {
            let error_code = extract_tag(&body_str, "Code");

            if error_code == "PermanentRedirect" && attempt == 0 {
                if let Some(new_region) =
                    resolve_redirect_region(&body_str, bucket, &region)
                {
                    region = new_region.clone();
                    cache_region(region_cache, bucket, &new_region);
                    continue;
                }
            }

            eprintln!("S3Backend: getObjectRange S3 error: {}", error);
            push_event(
                event_tx,
                StateEvent::ObjectRangeLoadError {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    start_byte,
                    error_message: error,
                },
            );
            return;
        }

        // Cache region on success
        cache_region(region_cache, bucket, &region);

        eprintln!(
            "S3Backend: getObjectRange success bucket={} key={} range={}-{} got={} total={}",
            bucket,
            key,
            start_byte,
            end_byte,
            body.len(),
            content_range_total
        );

        push_event(
            event_tx,
            StateEvent::ObjectRangeLoaded {
                bucket: bucket.to_string(),
                key: key.to_string(),
                start_byte,
                total_size: content_range_total,
                data: body.to_vec(),
            },
        );
        return;
    }
}

// ---- GetObjectStreaming ----------------------------------------------------

const STREAMING_CHUNK_SIZE: usize = 256 * 1024; // 256 KB

async fn process_get_object_streaming(
    bucket: &str,
    key: &str,
    start_byte: usize,
    total_size: usize,
    cancel_flag: &Option<CancelFlag>,
    client: &reqwest::Client,
    profile: &Arc<Mutex<AWSProfile>>,
    event_tx: &mpsc::Sender<StateEvent>,
    region_cache: &Arc<Mutex<HashMap<String, String>>>,
) {
    let snap = snapshot_profile(profile);
    let cached = get_cached_region(region_cache, bucket);
    let mut region = if cached.is_empty() {
        snap.region.clone()
    } else {
        cached
    };

    if region.is_empty() {
        push_event(
            event_tx,
            StateEvent::ObjectRangeLoadError {
                bucket: bucket.to_string(),
                key: key.to_string(),
                start_byte,
                error_message: "ERROR: Region not configured. Please ensure your AWS profile has a valid region.".to_string(),
            },
        );
        return;
    }

    let fake_profile = snapshot_as_profile(&snap);

    for attempt in 0..2 {
        let (host, path) = build_host_path(&fake_profile, &region, bucket, key);

        let signed = aws_sign_request(
            "GET",
            &host,
            &path,
            "",
            &region,
            "s3",
            &snap.access_key_id,
            &snap.secret_access_key,
            &[],
            &snap.session_token,
        );

        let mut extra_headers = BTreeMap::new();
        if start_byte > 0 {
            extra_headers.insert(
                "Range".to_string(),
                format!("bytes={}-", start_byte),
            );
        }

        // Check cancel before request
        if let Some(cf) = cancel_flag {
            if cf.load(Ordering::Relaxed) {
                eprintln!(
                    "S3Backend: getObjectStreaming cancelled bucket={} key={}",
                    bucket, key
                );
                return;
            }
        }

        let request = build_request(client, &signed, &extra_headers);
        let mut response = match request.send().await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("S3Backend: getObjectStreaming HTTP error: {}", e);
                push_event(
                    event_tx,
                    StateEvent::ObjectRangeLoadError {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        start_byte,
                        error_message: format!("ERROR: {}", e),
                    },
                );
                return;
            }
        };

        let mut buffer: Vec<u8> = Vec::new();
        let mut bytes_received: usize = 0;
        let mut cancelled = false;

        // Stream chunks from the response
        loop {
            // Check cancellation before each chunk
            if let Some(cf) = cancel_flag {
                if cf.load(Ordering::Relaxed) {
                    cancelled = true;
                    break;
                }
            }

            match response.chunk().await {
                Ok(Some(chunk)) => {
                    buffer.extend_from_slice(&chunk);

                    // Emit events for complete chunks
                    while buffer.len() >= STREAMING_CHUNK_SIZE {
                        let chunk_data: Vec<u8> =
                            buffer.drain(..STREAMING_CHUNK_SIZE).collect();
                        let chunk_offset = start_byte + bytes_received;
                        bytes_received += chunk_data.len();

                        push_event(
                            event_tx,
                            StateEvent::ObjectRangeLoaded {
                                bucket: bucket.to_string(),
                                key: key.to_string(),
                                start_byte: chunk_offset,
                                total_size,
                                data: chunk_data,
                            },
                        );
                    }
                }
                Ok(None) => {
                    // End of stream
                    break;
                }
                Err(e) => {
                    eprintln!("S3Backend: getObjectStreaming chunk error: {}", e);
                    push_event(
                        event_tx,
                        StateEvent::ObjectRangeLoadError {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                            start_byte,
                            error_message: format!("ERROR: {}", e),
                        },
                    );
                    return;
                }
            }
        }

        if cancelled {
            eprintln!(
                "S3Backend: getObjectStreaming cancelled bucket={} key={}",
                bucket, key
            );
            return;
        }

        // Check for S3 error in buffered data (errors come as XML)
        let buffer_str = String::from_utf8_lossy(&buffer);
        let error = extract_error(&buffer_str);
        if !error.is_empty() {
            let error_code = extract_tag(&buffer_str, "Code");
            if error_code == "PermanentRedirect" && attempt == 0 {
                if let Some(new_region) =
                    resolve_redirect_region(&buffer_str, bucket, &region)
                {
                    region = new_region.clone();
                    cache_region(region_cache, bucket, &new_region);
                    continue;
                }
            }

            eprintln!("S3Backend: getObjectStreaming S3 error: {}", error);
            push_event(
                event_tx,
                StateEvent::ObjectRangeLoadError {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    start_byte,
                    error_message: error,
                },
            );
            return;
        }

        // Cache region on success
        cache_region(region_cache, bucket, &region);

        // Emit any remaining data in the buffer as the final chunk
        if !buffer.is_empty() {
            let chunk_offset = start_byte + bytes_received;
            eprintln!(
                "S3Backend: emitting final chunk of {} bytes at offset {}",
                buffer.len(),
                chunk_offset
            );
            push_event(
                event_tx,
                StateEvent::ObjectRangeLoaded {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    start_byte: chunk_offset,
                    total_size,
                    data: buffer,
                },
            );
        }

        eprintln!(
            "S3Backend: getObjectStreaming complete bucket={} key={}",
            bucket, key
        );
        return;
    }
}

// ---------------------------------------------------------------------------
// S3Backend construction + Backend trait
// ---------------------------------------------------------------------------

impl S3Backend {
    /// Create a new `S3Backend` with the given profile and `num_workers`
    /// high-priority + `num_workers` low-priority tokio tasks.
    pub fn new(profile: AWSProfile, num_workers: usize) -> Self {
        eprintln!(
            "S3Backend: initializing with profile={} region={} num_workers={}",
            profile.name, profile.region, num_workers
        );

        let (event_tx, event_rx) = mpsc::channel();

        let high_queue: Arc<Mutex<VecDeque<WorkItem>>> = Arc::new(Mutex::new(VecDeque::new()));
        let low_queue: Arc<Mutex<VecDeque<WorkItem>>> = Arc::new(Mutex::new(VecDeque::new()));
        let high_notify = Arc::new(tokio::sync::Notify::new());
        let low_notify = Arc::new(tokio::sync::Notify::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let region_cache: Arc<Mutex<HashMap<String, String>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let request_lag: Arc<Mutex<f32>> = Arc::new(Mutex::new(0.0));
        let profile_arc: Arc<Mutex<AWSProfile>> = Arc::new(Mutex::new(profile));

        // Clone everything the runtime thread will own
        let rt_high_queue = Arc::clone(&high_queue);
        let rt_low_queue = Arc::clone(&low_queue);
        let rt_high_notify = Arc::clone(&high_notify);
        let rt_low_notify = Arc::clone(&low_notify);
        let rt_shutdown = Arc::clone(&shutdown);
        let rt_profile = Arc::clone(&profile_arc);
        let rt_region_cache = Arc::clone(&region_cache);
        let rt_request_lag = Arc::clone(&request_lag);
        let rt_event_tx = event_tx.clone();

        let handle = std::thread::Builder::new()
            .name("s3-runtime".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("Failed to create tokio runtime");

                rt.block_on(async move {
                    let client = reqwest::Client::builder()
                        .timeout(std::time::Duration::from_secs(30))
                        .pool_max_idle_per_host(10)
                        .build()
                        .expect("Failed to create reqwest client");

                    let mut tasks = Vec::new();

                    // Spawn high-priority workers
                    for i in 0..num_workers {
                        let q = Arc::clone(&rt_high_queue);
                        let n = Arc::clone(&rt_high_notify);
                        let s = Arc::clone(&rt_shutdown);
                        let c = client.clone();
                        let p = Arc::clone(&rt_profile);
                        let tx = rt_event_tx.clone();
                        let rc = Arc::clone(&rt_region_cache);
                        let rl = Arc::clone(&rt_request_lag);
                        tasks.push(tokio::spawn(async move {
                            eprintln!("S3Backend: High priority worker {} started", i);
                            worker(q, n, s, c, p, tx, rc, rl).await;
                            eprintln!("S3Backend: High priority worker {} exiting", i);
                        }));
                    }

                    // Spawn low-priority workers
                    for i in 0..num_workers {
                        let q = Arc::clone(&rt_low_queue);
                        let n = Arc::clone(&rt_low_notify);
                        let s = Arc::clone(&rt_shutdown);
                        let c = client.clone();
                        let p = Arc::clone(&rt_profile);
                        let tx = rt_event_tx.clone();
                        let rc = Arc::clone(&rt_region_cache);
                        let rl = Arc::clone(&rt_request_lag);
                        tasks.push(tokio::spawn(async move {
                            eprintln!("S3Backend: Low priority worker {} started", i);
                            worker(q, n, s, c, p, tx, rc, rl).await;
                            eprintln!("S3Backend: Low priority worker {} exiting", i);
                        }));
                    }

                    // Wait for all workers to finish
                    for t in tasks {
                        let _ = t.await;
                    }
                });
            })
            .expect("Failed to spawn S3 runtime thread");

        S3Backend {
            profile: profile_arc,
            _event_tx: event_tx,
            event_rx: Mutex::new(event_rx),
            high_queue,
            low_queue,
            high_notify,
            low_notify,
            shutdown,
            hover_cancel: Mutex::new(None),
            region_cache,
            _runtime_handle: handle,
            request_lag_seconds: request_lag,
        }
    }

    /// Change the active AWS profile. Cancels all pending requests and clears
    /// the region cache.
    pub fn set_profile(&self, profile: AWSProfile) {
        eprintln!(
            "S3Backend: switching profile to {} region={}",
            profile.name, profile.region
        );

        self.cancel_all();

        // Clear region cache
        {
            self.region_cache.lock().unwrap().clear();
            eprintln!("S3Backend: cleared region cache on profile switch");
        }

        // Refresh credentials from disk
        let mut new_profile = profile.clone();
        if !refresh_profile_credentials(&mut new_profile) {
            eprintln!(
                "S3Backend: failed to refresh credentials for profile {}, using cached credentials",
                profile.name
            );
            new_profile = profile;
        }

        *self.profile.lock().unwrap() = new_profile;
    }

    /// Set artificial request lag in seconds for testing.
    pub fn set_request_lag(&self, seconds: f32) {
        *self.request_lag_seconds.lock().unwrap() = seconds;
    }

    // -- Private helpers --

    fn enqueue_high(&self, item: WorkItem) {
        self.high_queue.lock().unwrap().push_back(item);
        self.high_notify.notify_one();
    }

    fn enqueue_low(&self, item: WorkItem) {
        // Push to front so most recent prefetch request is served first
        self.low_queue.lock().unwrap().push_front(item);
        self.low_notify.notify_one();
    }

    /// Search both queues for an item matching the predicate.
    fn find_in_queues<P>(&self, pred: P) -> bool
    where
        P: Fn(&WorkItem) -> bool,
    {
        self.high_queue.lock().unwrap().iter().any(&pred)
            || self.low_queue.lock().unwrap().iter().any(&pred)
    }

    /// Move a matching item from the low-priority queue to the front of the
    /// high-priority queue. Returns `true` if found (either moved or already
    /// in high queue).
    fn boost_from_low_to_high<P>(&self, pred: P) -> bool
    where
        P: Fn(&WorkItem) -> bool,
    {
        // Try to extract from low queue
        let found_item = {
            let mut q = self.low_queue.lock().unwrap();
            if let Some(pos) = q.iter().position(|item| pred(item)) {
                q.remove(pos)
            } else {
                None
            }
        };

        if let Some(mut item) = found_item {
            // Clear cancel flag so boosted requests aren't cancelled by hover
            clear_cancel_flag(&mut item);
            self.high_queue.lock().unwrap().push_front(item);
            self.high_notify.notify_one();
            return true;
        }

        // Check if already in high queue
        self.high_queue.lock().unwrap().iter().any(&pred)
    }
}

impl Backend for S3Backend {
    fn take_events(&self) -> Vec<StateEvent> {
        let rx = self.event_rx.lock().unwrap();
        let mut events = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(event) => events.push(event),
                Err(_) => break,
            }
        }
        events
    }

    fn list_buckets(&self) {
        eprintln!("S3Backend: queuing listBuckets request");
        self.enqueue_high(WorkItem::ListBuckets);
    }

    fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: &str,
        cancel_flag: Option<CancelFlag>,
    ) {
        eprintln!(
            "S3Backend: queuing listObjects bucket={} prefix={} token={}",
            bucket,
            prefix,
            if continuation_token.is_empty() {
                "(none)"
            } else {
                &continuation_token[..continuation_token.len().min(20)]
            }
        );
        self.enqueue_high(WorkItem::ListObjects {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            continuation_token: continuation_token.to_string(),
            cancel_flag,
        });
    }

    fn get_object(
        &self,
        bucket: &str,
        key: &str,
        max_bytes: Option<usize>,
        low_priority: bool,
        cancellable: bool,
    ) {
        eprintln!(
            "S3Backend: queuing getObject bucket={} key={} max_bytes={:?} priority={} cancellable={}",
            bucket,
            key,
            max_bytes,
            if low_priority { "low" } else { "high" },
            cancellable
        );

        let cancel_flag = if cancellable {
            let mut hover = self.hover_cancel.lock().unwrap();
            if let Some(ref prev) = *hover {
                prev.store(true, Ordering::SeqCst);
            }
            let flag = Arc::new(AtomicBool::new(false));
            let cf = Some(Arc::clone(&flag));
            *hover = Some(flag);
            cf
        } else {
            None
        };

        let item = WorkItem::GetObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            max_bytes,
            cancel_flag,
        };

        if low_priority {
            self.enqueue_low(item);
        } else {
            self.enqueue_high(item);
        }
    }

    fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        start_byte: usize,
        end_byte: usize,
        cancel_flag: Option<CancelFlag>,
    ) {
        eprintln!(
            "S3Backend: queuing getObjectRange bucket={} key={} range={}-{}",
            bucket, key, start_byte, end_byte
        );
        self.enqueue_high(WorkItem::GetObjectRange {
            bucket: bucket.to_string(),
            key: key.to_string(),
            start_byte,
            end_byte,
            cancel_flag,
        });
    }

    fn get_object_streaming(
        &self,
        bucket: &str,
        key: &str,
        start_byte: usize,
        total_size: usize,
        cancel_flag: Option<CancelFlag>,
    ) {
        eprintln!(
            "S3Backend: queuing getObjectStreaming bucket={} key={} startByte={} totalSize={}",
            bucket, key, start_byte, total_size
        );
        self.enqueue_high(WorkItem::GetObjectStreaming {
            bucket: bucket.to_string(),
            key: key.to_string(),
            start_byte,
            total_size,
            cancel_flag,
        });
    }

    fn cancel_all(&self) {
        self.high_queue.lock().unwrap().clear();
        self.low_queue.lock().unwrap().clear();
    }

    fn list_objects_prefetch(&self, bucket: &str, prefix: &str, cancellable: bool) {
        eprintln!(
            "S3Backend: queuing prefetch bucket={} prefix={} cancellable={}",
            bucket, prefix, cancellable
        );

        let cancel_flag = if cancellable {
            let mut hover = self.hover_cancel.lock().unwrap();
            if let Some(ref prev) = *hover {
                prev.store(true, Ordering::SeqCst);
            }
            let flag = Arc::new(AtomicBool::new(false));
            let cf = Some(Arc::clone(&flag));
            *hover = Some(flag);
            cf
        } else {
            None
        };

        self.enqueue_low(WorkItem::ListObjects {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            continuation_token: String::new(),
            cancel_flag,
        });
    }

    fn prioritize_request(&self, bucket: &str, prefix: &str) -> bool {
        let result = self.boost_from_low_to_high(|item| {
            matches!(item, WorkItem::ListObjects { bucket: b, prefix: p, .. } if b == bucket && p == prefix)
        });
        if result {
            eprintln!(
                "S3Backend: prioritized request bucket={} prefix={}",
                bucket, prefix
            );
        }
        result
    }

    fn has_pending_request(&self, bucket: &str, prefix: &str) -> bool {
        self.find_in_queues(|item| {
            matches!(item, WorkItem::ListObjects { bucket: b, prefix: p, .. } if b == bucket && p == prefix)
        })
    }

    fn has_pending_object_request(&self, bucket: &str, key: &str) -> bool {
        self.find_in_queues(|item| {
            matches!(item, WorkItem::GetObject { bucket: b, key: k, .. } if b == bucket && k == key)
        })
    }

    fn prioritize_object_request(&self, bucket: &str, key: &str) -> bool {
        let result = self.boost_from_low_to_high(|item| {
            matches!(item, WorkItem::GetObject { bucket: b, key: k, .. } if b == bucket && k == key)
        });
        if result {
            eprintln!(
                "S3Backend: prioritized object request bucket={} key={}",
                bucket, key
            );
        }
        result
    }
}
