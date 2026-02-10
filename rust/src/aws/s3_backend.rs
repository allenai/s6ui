use crate::aws::credentials::AwsProfile;
use crate::aws::signer;
use crate::aws::xml;
use crate::backend::Backend;
use crate::events::{S3Bucket, S3Object, StateEvent};
use crate::preview::{create_transform, StreamingFilePreview, StreamingStatus};

use futures_util::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;
use tokio::sync::Notify;
use winit::event_loop::EventLoopProxy;

/// Work item types for the background workers
enum WorkItem {
    ListBuckets,
    ListObjects {
        bucket: String,
        prefix: String,
        continuation_token: String,
    },
    GetObject {
        bucket: String,
        key: String,
        max_bytes: usize,
    },
    StreamingGetObject {
        bucket: String,
        key: String,
        preview: Arc<StreamingFilePreview>,
        range_start: u64,
        max_bytes: Option<u64>,
    },
    Shutdown,
}

/// Shared inner state for S3Backend (accessed by tokio tasks)
struct S3BackendInner {
    profile: AwsProfile,
    high_queue: Mutex<VecDeque<WorkItem>>,
    low_queue: Mutex<VecDeque<WorkItem>>,
    high_notify: Notify,
    low_notify: Notify,
    event_tx: mpsc::Sender<StateEvent>,
    event_proxy: EventLoopProxy<()>,
    region_cache: Mutex<HashMap<String, String>>,
    client: reqwest::Client,
    shutdown: std::sync::atomic::AtomicBool,
}

impl S3BackendInner {
    fn push_event(&self, event: StateEvent) {
        let _ = self.event_tx.send(event);
        let _ = self.event_proxy.send_event(());
    }

    fn get_cached_region(&self, bucket: &str) -> Option<String> {
        let cache = self.region_cache.lock().unwrap();
        cache.get(bucket).cloned()
    }

    fn cache_region(&self, bucket: &str, region: &str) {
        let mut cache = self.region_cache.lock().unwrap();
        cache.insert(bucket.to_string(), region.to_string());
    }
}

/// S3 backend implementation using tokio tasks + reqwest
pub struct S3Backend {
    inner: Arc<S3BackendInner>,
    event_rx: mpsc::Receiver<StateEvent>,
    _runtime_handle: Handle,
}

impl S3Backend {
    pub fn new(
        profile: AwsProfile,
        runtime_handle: Handle,
        event_proxy: EventLoopProxy<()>,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel();
        let inner = Arc::new(S3BackendInner {
            profile,
            high_queue: Mutex::new(VecDeque::new()),
            low_queue: Mutex::new(VecDeque::new()),
            high_notify: Notify::new(),
            low_notify: Notify::new(),
            event_tx,
            event_proxy,
            region_cache: Mutex::new(HashMap::new()),
            client: reqwest::Client::new(),
            shutdown: std::sync::atomic::AtomicBool::new(false),
        });

        // Spawn high-priority workers
        for _ in 0..4 {
            let inner = Arc::clone(&inner);
            runtime_handle.spawn(worker_loop(inner, true));
        }

        // Spawn low-priority workers
        for _ in 0..2 {
            let inner = Arc::clone(&inner);
            runtime_handle.spawn(worker_loop(inner, false));
        }

        Self {
            inner,
            event_rx,
            _runtime_handle: runtime_handle,
        }
    }
}

impl Drop for S3Backend {
    fn drop(&mut self) {
        self.inner
            .shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);

        // Enqueue shutdown items for all workers
        {
            let mut q = self.inner.high_queue.lock().unwrap();
            for _ in 0..4 {
                q.push_back(WorkItem::Shutdown);
            }
        }
        self.inner.high_notify.notify_waiters();

        {
            let mut q = self.inner.low_queue.lock().unwrap();
            for _ in 0..2 {
                q.push_back(WorkItem::Shutdown);
            }
        }
        self.inner.low_notify.notify_waiters();
    }
}

impl Backend for S3Backend {
    fn take_events(&self) -> Vec<StateEvent> {
        self.event_rx.try_iter().collect()
    }

    fn list_buckets(&self) {
        let mut q = self.inner.high_queue.lock().unwrap();
        q.push_back(WorkItem::ListBuckets);
        drop(q);
        self.inner.high_notify.notify_one();
    }

    fn list_objects(&self, bucket: &str, prefix: &str, continuation_token: &str) {
        let mut q = self.inner.high_queue.lock().unwrap();
        q.push_back(WorkItem::ListObjects {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            continuation_token: continuation_token.to_string(),
        });
        drop(q);
        self.inner.high_notify.notify_one();
    }

    fn get_object(&self, bucket: &str, key: &str, max_bytes: usize) {
        let mut q = self.inner.high_queue.lock().unwrap();
        q.push_back(WorkItem::GetObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            max_bytes,
        });
        drop(q);
        self.inner.high_notify.notify_one();
    }

    fn streaming_get_object(
        &self,
        bucket: &str,
        key: &str,
        preview: Arc<StreamingFilePreview>,
        range_start: u64,
        max_bytes: Option<u64>,
    ) {
        let mut q = self.inner.high_queue.lock().unwrap();
        q.push_back(WorkItem::StreamingGetObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            preview,
            range_start,
            max_bytes,
        });
        drop(q);
        self.inner.high_notify.notify_one();
    }

    fn cancel_all(&self) {
        self.inner.high_queue.lock().unwrap().clear();
        self.inner.low_queue.lock().unwrap().clear();
    }
}

/// Worker loop that pulls items from the appropriate queue and processes them
async fn worker_loop(inner: Arc<S3BackendInner>, high_priority: bool) {
    loop {
        // Wait for work
        let notify = if high_priority {
            &inner.high_notify
        } else {
            &inner.low_notify
        };
        notify.notified().await;

        // Process all available items
        loop {
            if inner.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }

            let item = {
                let queue = if high_priority {
                    &inner.high_queue
                } else {
                    &inner.low_queue
                };
                queue.lock().unwrap().pop_front()
            };

            let item = match item {
                Some(item) => item,
                None => break,
            };

            match item {
                WorkItem::Shutdown => return,
                WorkItem::ListBuckets => process_list_buckets(&inner).await,
                WorkItem::ListObjects {
                    bucket,
                    prefix,
                    continuation_token,
                } => {
                    process_list_objects(&inner, &bucket, &prefix, &continuation_token).await;
                }
                WorkItem::GetObject {
                    bucket,
                    key,
                    max_bytes,
                } => {
                    process_get_object(&inner, &bucket, &key, max_bytes).await;
                }
                WorkItem::StreamingGetObject {
                    bucket,
                    key,
                    preview,
                    range_start,
                    max_bytes,
                } => {
                    process_streaming_get_object(&inner, &bucket, &key, preview, range_start, max_bytes).await;
                }
            }
        }
    }
}

/// Parse endpoint URL to extract host (with port)
fn parse_endpoint_host(endpoint_url: &str) -> String {
    let mut url = endpoint_url;
    if let Some(rest) = url.strip_prefix("https://") {
        url = rest;
    } else if let Some(rest) = url.strip_prefix("http://") {
        url = rest;
    }
    let url = url.trim_end_matches('/');
    // Remove path if present
    if let Some(pos) = url.find('/') {
        url[..pos].to_string()
    } else {
        url.to_string()
    }
}

/// Build host and path for an S3 request
fn build_host_path(
    profile: &AwsProfile,
    bucket: &str,
    key: &str,
    region: &str,
) -> (String, String) {
    if !profile.endpoint_url.is_empty() {
        // Path-style for custom endpoints
        let host = parse_endpoint_host(&profile.endpoint_url);
        let path = if key.is_empty() {
            format!("/{}", bucket)
        } else {
            format!("/{}/{}", bucket, key)
        };
        (host, path)
    } else {
        // Virtual-hosted style
        let host = format!("{}.s3.{}.amazonaws.com", bucket, region);
        let path = if key.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", key)
        };
        (host, path)
    }
}

/// Extract region from S3 endpoint like "bucket.s3.us-west-2.amazonaws.com"
fn extract_region_from_endpoint(endpoint: &str) -> Option<String> {
    // Look for "s3." pattern
    let s3_pos = endpoint.find("s3.")?;
    let region_start = s3_pos + 3;
    let rest = &endpoint[region_start..];
    let region_end = rest.find('.')?;
    if region_end == 0 {
        return None;
    }
    let region = &rest[..region_end];
    // Valid regions contain at least one dash
    if region.contains('-') {
        Some(region.to_string())
    } else {
        None
    }
}

/// Resolve the region for a bucket: cached > profile default
fn resolve_region(inner: &S3BackendInner, bucket: &str) -> String {
    if let Some(cached) = inner.get_cached_region(bucket) {
        return cached;
    }
    if inner.profile.region.is_empty() {
        "us-east-1".to_string()
    } else {
        inner.profile.region.clone()
    }
}

async fn process_list_buckets(inner: &S3BackendInner) {
    let profile = &inner.profile;
    let region = if profile.region.is_empty() {
        "us-east-1".to_string()
    } else {
        profile.region.clone()
    };
    let host = if !profile.endpoint_url.is_empty() {
        parse_endpoint_host(&profile.endpoint_url)
    } else {
        format!("s3.{}.amazonaws.com", region)
    };
    let access_key = &profile.access_key_id;
    let secret_key = &profile.secret_access_key;
    let session_token = &profile.session_token;
    let endpoint_url = &profile.endpoint_url;

    let signed = signer::sign_request(
        "GET",
        &host,
        "/",
        "",
        &region,
        "s3",
        &access_key,
        &secret_key,
        "",
        &session_token,
    );

    let url = if !endpoint_url.is_empty() {
        // For custom endpoints, use the endpoint URL directly
        let scheme = if endpoint_url.starts_with("http://") {
            "http"
        } else {
            "https"
        };
        format!("{}://{}/", scheme, host)
    } else {
        signed.url.clone()
    };

    match do_http_get(inner, &url, &signed.headers).await {
        Ok(body) => {
            if let Some(error) = xml::extract_error(&body) {
                inner.push_event(StateEvent::BucketsError { error });
            } else {
                let buckets = parse_list_buckets_xml(&body);
                inner.push_event(StateEvent::BucketsLoaded { buckets });
            }
        }
        Err(e) => {
            inner.push_event(StateEvent::BucketsError {
                error: format!("HTTP error: {}", e),
            });
        }
    }
}

async fn process_list_objects(
    inner: &S3BackendInner,
    bucket: &str,
    prefix: &str,
    continuation_token: &str,
) {
    let mut region = resolve_region(inner, bucket);

    let profile = &inner.profile;

    for attempt in 0..2 {
        let (host, path) = build_host_path(profile, bucket, "", &region);

        // Build query string
        let mut query = format!("delimiter={}&list-type=2&max-keys=1000", signer::url_encode("/"));
        if !prefix.is_empty() {
            query.push_str(&format!("&prefix={}", signer::url_encode(prefix)));
        }
        if !continuation_token.is_empty() {
            query.push_str(&format!(
                "&continuation-token={}",
                signer::url_encode(continuation_token)
            ));
        }

        let signed = signer::sign_request(
            "GET",
            &host,
            &path,
            &query,
            &region,
            "s3",
            &profile.access_key_id,
            &profile.secret_access_key,
            "",
            &profile.session_token,
        );

        let url = if !profile.endpoint_url.is_empty() {
            let scheme = if profile.endpoint_url.starts_with("http://") {
                "http"
            } else {
                "https"
            };
            let mut u = format!("{}://{}{}", scheme, host, path);
            if !query.is_empty() {
                u.push('?');
                u.push_str(&query);
            }
            u
        } else {
            signed.url.clone()
        };

        match do_http_get(inner, &url, &signed.headers).await {
            Ok(body) => {
                // Check for PermanentRedirect
                if let Some(error_code) = xml::extract_tag(&body, "Code") {
                    if error_code == "PermanentRedirect" && attempt == 0 {
                        if let Some(new_region) = try_extract_redirect_region(&body, bucket) {
                            if new_region != region {
                                region = new_region;
                                inner.cache_region(bucket, &region);
                                continue;
                            }
                        }
                    }
                }

                if let Some(error) = xml::extract_error(&body) {
                    inner.push_event(StateEvent::ObjectsError {
                        bucket: bucket.to_string(),
                        prefix: prefix.to_string(),
                        error,
                    });
                } else {
                    let result = parse_list_objects_xml(&body, prefix);
                    inner.cache_region(bucket, &region);
                    inner.push_event(StateEvent::ObjectsLoaded {
                        bucket: bucket.to_string(),
                        prefix: prefix.to_string(),
                        continuation_token: continuation_token.to_string(),
                        objects: result.objects,
                        next_continuation_token: result.next_continuation_token,
                        is_truncated: result.is_truncated,
                    });
                }
            }
            Err(e) => {
                inner.push_event(StateEvent::ObjectsError {
                    bucket: bucket.to_string(),
                    prefix: prefix.to_string(),
                    error: format!("HTTP error: {}", e),
                });
            }
        }
        return;
    }
}

async fn process_get_object(inner: &S3BackendInner, bucket: &str, key: &str, max_bytes: usize) {
    let mut region = resolve_region(inner, bucket);

    let profile = &inner.profile;

    for attempt in 0..2 {
        let (host, path) = build_host_path(profile, bucket, key, &region);

        let signed = signer::sign_request(
            "GET",
            &host,
            &path,
            "",
            &region,
            "s3",
            &profile.access_key_id,
            &profile.secret_access_key,
            "",
            &profile.session_token,
        );

        let url = if !profile.endpoint_url.is_empty() {
            let scheme = if profile.endpoint_url.starts_with("http://") {
                "http"
            } else {
                "https"
            };
            format!("{}://{}{}", scheme, host, path)
        } else {
            signed.url.clone()
        };

        // Build request with optional Range header
        let mut req = inner.client.get(&url);
        for (k, v) in &signed.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        if max_bytes > 0 {
            req = req.header("Range", format!("bytes=0-{}", max_bytes - 1));
        }

        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();

                if status.is_success() || status.as_u16() == 206 {
                    inner.cache_region(bucket, &region);
                    inner.push_event(StateEvent::ObjectContentLoaded {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        content: body,
                    });
                    return;
                }

                // Check for PermanentRedirect
                if let Some(error_code) = xml::extract_tag(&body, "Code") {
                    if error_code == "PermanentRedirect" && attempt == 0 {
                        if let Some(new_region) = try_extract_redirect_region(&body, bucket) {
                            if new_region != region {
                                region = new_region;
                                inner.cache_region(bucket, &region);
                                continue;
                            }
                        }
                    }

                    // InvalidRange means empty file
                    if error_code == "InvalidRange" {
                        inner.push_event(StateEvent::ObjectContentLoaded {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                            content: String::new(),
                        });
                        return;
                    }
                }

                let error = xml::extract_error(&body)
                    .unwrap_or_else(|| format!("HTTP {}", status.as_u16()));
                inner.push_event(StateEvent::ObjectContentError {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    error,
                });
            }
            Err(e) => {
                inner.push_event(StateEvent::ObjectContentError {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    error: format!("HTTP error: {}", e),
                });
            }
        }
        return;
    }
}

/// Process streaming GET object request with on-the-fly decompression
async fn process_streaming_get_object(
    inner: &S3BackendInner,
    bucket: &str,
    key: &str,
    preview: Arc<StreamingFilePreview>,
    range_start: u64,
    max_bytes: Option<u64>,
) {
    let mut region = resolve_region(inner, bucket);
    let profile = &inner.profile;
    let is_prefetch = max_bytes.is_some();

    // Update status
    if is_prefetch {
        preview.set_status(StreamingStatus::Prefetching);
    } else {
        preview.set_downloading();
    }

    for attempt in 0..2 {
        let (host, path) = build_host_path(profile, bucket, key, &region);

        // Build Range header
        let range_header = match max_bytes {
            Some(n) => format!("bytes={}-{}", range_start, range_start + n - 1),
            None if range_start > 0 => format!("bytes={}-", range_start),
            None => String::new(),
        };

        let signed = signer::sign_request(
            "GET",
            &host,
            &path,
            "",
            &region,
            "s3",
            &profile.access_key_id,
            &profile.secret_access_key,
            "",
            &profile.session_token,
        );

        let url = if !profile.endpoint_url.is_empty() {
            let scheme = if profile.endpoint_url.starts_with("http://") {
                "http"
            } else {
                "https"
            };
            format!("{}://{}{}", scheme, host, path)
        } else {
            signed.url.clone()
        };

        // Build request
        let mut req = inner.client.get(&url);
        for (k, v) in &signed.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        if !range_header.is_empty() {
            req = req.header("Range", &range_header);
        }

        match req.send().await {
            Ok(resp) => {
                let status = resp.status();

                if !status.is_success() && status.as_u16() != 206 {
                    // Check for redirect or error
                    let body = resp.text().await.unwrap_or_default();

                    if let Some(error_code) = xml::extract_tag(&body, "Code") {
                        if error_code == "PermanentRedirect" && attempt == 0 {
                            if let Some(new_region) = try_extract_redirect_region(&body, bucket) {
                                if new_region != region {
                                    region = new_region;
                                    inner.cache_region(bucket, &region);
                                    continue;
                                }
                            }
                        }

                        // InvalidRange means empty file
                        if error_code == "InvalidRange" {
                            preview.set_complete();
                            inner.push_event(StateEvent::PreviewProgress {
                                bucket: bucket.to_string(),
                                key: key.to_string(),
                                decompressed_bytes: 0,
                                source_bytes: 0,
                                line_count: 1,
                                status: StreamingStatus::Complete,
                            });
                            return;
                        }
                    }

                    let error = xml::extract_error(&body)
                        .unwrap_or_else(|| format!("HTTP {}", status.as_u16()));
                    preview.set_status(StreamingStatus::Error(error.clone()));
                    inner.push_event(StateEvent::PreviewError {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        error,
                    });
                    return;
                }

                // Get content-length if available
                if let Some(content_length) = resp.content_length() {
                    if range_start == 0 {
                        preview.set_total_source_size(content_length);
                    }
                }

                inner.cache_region(bucket, &region);

                // Create decompression transform
                let compression = preview.compression();
                let mut transform = match create_transform(compression) {
                    Ok(t) => t,
                    Err(e) => {
                        preview.set_status(StreamingStatus::Error(e.clone()));
                        inner.push_event(StateEvent::PreviewError {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                            error: e,
                        });
                        return;
                    }
                };

                // Stream the response body
                let mut stream = resp.bytes_stream();
                let mut decompress_buf = Vec::with_capacity(64 * 1024);

                while let Some(chunk_result) = stream.next().await {
                    match chunk_result {
                        Ok(chunk) => {
                            // Track source bytes
                            preview.add_source_bytes(chunk.len() as u64);

                            // Decompress
                            decompress_buf.clear();
                            if let Err(e) = transform.process(&chunk, &mut decompress_buf) {
                                preview.set_status(StreamingStatus::Error(e.clone()));
                                inner.push_event(StateEvent::PreviewError {
                                    bucket: bucket.to_string(),
                                    key: key.to_string(),
                                    error: e,
                                });
                                return;
                            }

                            // Write to temp file + index newlines
                            if let Err(e) = preview.append_data(&decompress_buf) {
                                preview.set_status(StreamingStatus::Error(e.clone()));
                                inner.push_event(StateEvent::PreviewError {
                                    bucket: bucket.to_string(),
                                    key: key.to_string(),
                                    error: e,
                                });
                                return;
                            }

                            // Emit progress event
                            inner.push_event(StateEvent::PreviewProgress {
                                bucket: bucket.to_string(),
                                key: key.to_string(),
                                decompressed_bytes: preview.bytes_written(),
                                source_bytes: preview.source_bytes(),
                                line_count: preview.line_count(),
                                status: preview.status(),
                            });
                        }
                        Err(e) => {
                            let error = format!("Stream error: {}", e);
                            preview.set_status(StreamingStatus::Error(error.clone()));
                            inner.push_event(StateEvent::PreviewError {
                                bucket: bucket.to_string(),
                                key: key.to_string(),
                                error,
                            });
                            return;
                        }
                    }
                }

                // Finalize decompression
                decompress_buf.clear();
                if let Err(e) = transform.finish(&mut decompress_buf) {
                    preview.set_status(StreamingStatus::Error(e.clone()));
                    inner.push_event(StateEvent::PreviewError {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        error: e,
                    });
                    return;
                }

                if !decompress_buf.is_empty() {
                    if let Err(e) = preview.append_data(&decompress_buf) {
                        preview.set_status(StreamingStatus::Error(e.clone()));
                        inner.push_event(StateEvent::PreviewError {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                            error: e,
                        });
                        return;
                    }
                }

                // Set final status
                if is_prefetch {
                    preview.set_prefetch_ready();
                } else {
                    preview.set_complete();
                }

                // Final progress event
                inner.push_event(StateEvent::PreviewProgress {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    decompressed_bytes: preview.bytes_written(),
                    source_bytes: preview.source_bytes(),
                    line_count: preview.line_count(),
                    status: preview.status(),
                });

                return;
            }
            Err(e) => {
                let error = format!("HTTP error: {}", e);
                preview.set_status(StreamingStatus::Error(error.clone()));
                inner.push_event(StateEvent::PreviewError {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    error,
                });
                return;
            }
        }
    }
}

/// Try to extract the correct region from a PermanentRedirect response
fn try_extract_redirect_region(body: &str, bucket: &str) -> Option<String> {
    // Try from Endpoint tag
    if let Some(endpoint) = xml::extract_tag(body, "Endpoint") {
        if let Some(region) = extract_region_from_endpoint(&endpoint) {
            return Some(region);
        }
    }

    // Try common region patterns in bucket name
    let bucket_lower = bucket.to_lowercase();
    let regions = [
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
    for r in &regions {
        if bucket_lower.contains(r) {
            return Some(r.to_string());
        }
    }

    // Last resort default
    Some("us-east-1".to_string())
}

/// Perform an HTTP GET request using reqwest
async fn do_http_get(
    inner: &S3BackendInner,
    url: &str,
    headers: &std::collections::BTreeMap<String, String>,
) -> Result<String, String> {
    let mut req = inner.client.get(url);
    for (k, v) in headers {
        req = req.header(k.as_str(), v.as_str());
    }

    match req.send().await {
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if status.is_success() {
                Ok(body)
            } else {
                // Return the body even on error status (contains XML error info)
                Ok(body)
            }
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Parse ListBuckets XML response
fn parse_list_buckets_xml(xml_body: &str) -> Vec<S3Bucket> {
    let mut buckets = Vec::new();

    let bucket_tag = "<Bucket>";
    let bucket_end = "</Bucket>";
    let mut pos = 0;

    while let Some(start) = xml_body[pos..].find(bucket_tag) {
        let abs_start = pos + start;
        if let Some(end) = xml_body[abs_start..].find(bucket_end) {
            let bucket_xml = &xml_body[abs_start..abs_start + end + bucket_end.len()];
            let name = xml::extract_tag(bucket_xml, "Name").unwrap_or_default();
            let creation_date = xml::extract_tag(bucket_xml, "CreationDate").unwrap_or_default();

            if !name.is_empty() {
                buckets.push(S3Bucket {
                    name,
                    creation_date,
                });
            }
            pos = abs_start + end + bucket_end.len();
        } else {
            break;
        }
    }

    buckets
}

struct ListObjectsResult {
    objects: Vec<S3Object>,
    next_continuation_token: String,
    is_truncated: bool,
}

/// Parse ListObjectsV2 XML response
fn parse_list_objects_xml(xml_body: &str, _prefix: &str) -> ListObjectsResult {
    let mut result = ListObjectsResult {
        objects: Vec::new(),
        next_continuation_token: xml::extract_tag(xml_body, "NextContinuationToken")
            .unwrap_or_default(),
        is_truncated: xml::extract_tag(xml_body, "IsTruncated")
            .map(|s| s == "true")
            .unwrap_or(false),
    };

    // Parse CommonPrefixes (folders)
    let prefix_tag = "<CommonPrefixes>";
    let prefix_end = "</CommonPrefixes>";
    let mut pos = 0;
    while let Some(start) = xml_body[pos..].find(prefix_tag) {
        let abs_start = pos + start;
        if let Some(end) = xml_body[abs_start..].find(prefix_end) {
            let prefix_xml = &xml_body[abs_start..abs_start + end];
            if let Some(prefix) = xml::extract_tag(prefix_xml, "Prefix") {
                let display_name = {
                    let mut p = prefix.clone();
                    if p.ends_with('/') {
                        p.pop();
                    }
                    match p.rfind('/') {
                        Some(i) => p[i + 1..].to_string(),
                        None => p,
                    }
                };
                result.objects.push(S3Object {
                    key: prefix,
                    display_name,
                    size: 0,
                    last_modified: String::new(),
                    is_folder: true,
                });
            }
            pos = abs_start + end + prefix_end.len();
        } else {
            break;
        }
    }

    // Parse Contents (files)
    let contents_tag = "<Contents>";
    let contents_end = "</Contents>";
    pos = 0;
    while let Some(start) = xml_body[pos..].find(contents_tag) {
        let abs_start = pos + start;
        if let Some(end) = xml_body[abs_start..].find(contents_end) {
            let contents_xml = &xml_body[abs_start..abs_start + end];
            let key = xml::extract_tag(contents_xml, "Key").unwrap_or_default();
            let size: i64 = xml::extract_tag(contents_xml, "Size")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let last_modified = xml::extract_tag(contents_xml, "LastModified").unwrap_or_default();

            // Skip folder markers
            if !key.is_empty() && !key.ends_with('/') {
                let display_name = match key.rfind('/') {
                    Some(i) => key[i + 1..].to_string(),
                    None => key.clone(),
                };
                result.objects.push(S3Object {
                    key,
                    display_name,
                    size,
                    last_modified,
                    is_folder: false,
                });
            }
            pos = abs_start + end + contents_end.len();
        } else {
            break;
        }
    }

    result
}
