use crate::backend::{Backend, RequestPriority};
use crate::events::{S3Bucket, S3Object, StateEvent};
use crate::preview::{Compression, PREFETCH_BYTES, StreamingFilePreview, StreamingStatus};
use crate::settings::{AppSettings, PathEntry};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Loading state for folder data (orthogonal to `loading: bool` which tracks in-flight requests)
#[derive(Clone, PartialEq)]
pub enum DataStatus {
    /// Never loaded
    Empty,
    /// Has data, more available
    Partial,
    /// Has data, fully loaded
    Complete,
    /// Failed to load (no data)
    Error(String),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FolderLoadMode {
    FirstPage,
    Full,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PreviewLoadMode {
    Prefetch,
    Full,
}

/// A cached folder's contents
pub struct FolderNode {
    pub objects: Vec<S3Object>,
    pub next_continuation_token: String,
    pub loading: bool,
    pub status: DataStatus,

    // Cached sorted view: indices into objects[] (folders first, then files)
    pub sorted_view: Vec<usize>,
    pub folder_count: usize,
    cached_objects_size: usize,
    request_id: u64,
    mode: FolderLoadMode,
    child_preloads_scheduled: bool,
}

impl FolderNode {
    fn new(_bucket: String, _prefix: String) -> Self {
        Self {
            objects: Vec::new(),
            next_continuation_token: String::new(),
            loading: false,
            status: DataStatus::Empty,
            sorted_view: Vec::new(),
            folder_count: 0,
            cached_objects_size: 0,
            request_id: 0,
            mode: FolderLoadMode::FirstPage,
            child_preloads_scheduled: false,
        }
    }

    fn reset_for_request(&mut self, request_id: u64, mode: FolderLoadMode) {
        self.objects.clear();
        self.next_continuation_token.clear();
        self.loading = true;
        self.status = DataStatus::Empty;
        self.sorted_view.clear();
        self.folder_count = 0;
        self.cached_objects_size = 0;
        self.request_id = request_id;
        self.mode = mode;
        self.child_preloads_scheduled = false;
    }

    pub fn is_loaded(&self) -> bool {
        matches!(self.status, DataStatus::Partial | DataStatus::Complete)
    }

    pub fn is_truncated(&self) -> bool {
        matches!(self.status, DataStatus::Partial)
    }

    pub fn error(&self) -> Option<&str> {
        match &self.status {
            DataStatus::Error(e) => Some(e),
            _ => None,
        }
    }

    pub fn rebuild_sorted_view_if_needed(&mut self) {
        if self.cached_objects_size == self.objects.len() {
            return;
        }

        self.sorted_view.clear();
        self.sorted_view.reserve(self.objects.len());

        // Folders first
        for (i, obj) in self.objects.iter().enumerate() {
            if obj.is_folder {
                self.sorted_view.push(i);
            }
        }
        self.folder_count = self.sorted_view.len();

        // Files second
        for (i, obj) in self.objects.iter().enumerate() {
            if !obj.is_folder {
                self.sorted_view.push(i);
            }
        }

        self.cached_objects_size = self.objects.len();
    }
}

/// Status of a file preview (compatibility wrapper for UI)
#[derive(Clone, PartialEq)]
pub enum PreviewStatus {
    /// Request in flight (prefetching or downloading)
    Loading,
    /// Content loaded successfully (prefetch or complete)
    Ready,
    /// File type not supported for preview
    Unsupported,
    /// Error loading content
    Error(String),
}

impl From<&StreamingStatus> for PreviewStatus {
    fn from(status: &StreamingStatus) -> Self {
        match status {
            StreamingStatus::Prefetching | StreamingStatus::Downloading => PreviewStatus::Loading,
            StreamingStatus::PrefetchReady | StreamingStatus::Complete => PreviewStatus::Ready,
            StreamingStatus::Error(e) => PreviewStatus::Error(e.clone()),
        }
    }
}

/// A cached file preview using streaming infrastructure
pub struct PreviewNode {
    pub bucket: String,
    pub key: String,
    pub preview: Arc<StreamingFilePreview>,
    last_accessed: Instant,
    /// True if this file type is not supported for preview
    unsupported: bool,
    request_id: u64,
    desired_mode: PreviewLoadMode,
}

impl PreviewNode {
    fn new(
        bucket: String,
        key: String,
        preview: Arc<StreamingFilePreview>,
        request_id: u64,
        desired_mode: PreviewLoadMode,
    ) -> Self {
        Self {
            bucket,
            key,
            preview,
            last_accessed: Instant::now(),
            unsupported: false,
            request_id,
            desired_mode,
        }
    }

    fn new_unsupported(bucket: String, key: String) -> Self {
        // Create a dummy preview for unsupported files
        let preview = Arc::new(
            StreamingFilePreview::new(Compression::None).expect("Failed to create dummy preview"),
        );
        Self {
            bucket,
            key,
            preview,
            last_accessed: Instant::now(),
            unsupported: true,
            request_id: 0,
            desired_mode: PreviewLoadMode::Prefetch,
        }
    }

    pub fn touch(&mut self) {
        self.last_accessed = Instant::now();
    }

    /// Get the preview status for UI display
    pub fn status(&self) -> PreviewStatus {
        if self.unsupported {
            PreviewStatus::Unsupported
        } else {
            PreviewStatus::from(&self.preview.status())
        }
    }

    /// Get the streaming status
    pub fn streaming_status(&self) -> StreamingStatus {
        self.preview.status()
    }

    /// Get decompressed bytes written
    pub fn bytes_written(&self) -> u64 {
        self.preview.bytes_written()
    }

    /// Get source bytes downloaded
    pub fn source_bytes(&self) -> u64 {
        self.preview.source_bytes()
    }

    /// Check if download is complete
    pub fn is_complete(&self) -> bool {
        matches!(self.preview.status(), StreamingStatus::Complete)
    }
}

const PREVIEW_CACHE_MAX_ENTRIES: usize = 50;
const CHILD_PRELOAD_COUNT: usize = 20;
const MAX_FRECENT_PATHS: usize = 500;

fn current_unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn frecency_score(entry: &PathEntry, now: i64) -> f64 {
    let age = now.saturating_sub(entry.last_accessed);
    let weight = if age < 3_600 {
        4.0
    } else if age < 86_400 {
        2.0
    } else if age < 604_800 {
        1.0
    } else {
        0.5
    };

    entry.score * weight
}

/// The browser model - owns state and processes commands
pub struct BrowserModel {
    backend: Option<Box<dyn Backend>>,
    settings: AppSettings,

    // Profiles
    pub profiles: Vec<crate::aws::credentials::AwsProfile>,
    pub selected_profile_idx: usize,

    // Buckets
    pub buckets: Vec<S3Bucket>,
    pub buckets_loading: bool,
    pub buckets_error: String,
    buckets_complete: bool,
    buckets_request_id: u64,

    // Folder nodes cache
    nodes: HashMap<String, FolderNode>,

    // Current navigation path
    pub current_bucket: String,
    pub current_prefix: String,

    // Preview cache with LRU eviction
    previews: HashMap<String, PreviewNode>,
    pub selected_preview: Option<String>, // key into previews hashmap
    next_request_id: u64,
}

impl BrowserModel {
    pub fn new() -> Self {
        Self {
            backend: None,
            settings: AppSettings::default(),
            profiles: Vec::new(),
            selected_profile_idx: 0,
            buckets: Vec::new(),
            buckets_loading: false,
            buckets_error: String::new(),
            buckets_complete: false,
            buckets_request_id: 0,
            nodes: HashMap::new(),
            current_bucket: String::new(),
            current_prefix: String::new(),
            previews: HashMap::new(),
            selected_preview: None,
            next_request_id: 1,
        }
    }

    pub fn set_backend(&mut self, backend: Box<dyn Backend>) {
        self.backend = Some(backend);
    }

    pub fn set_settings(&mut self, settings: AppSettings) {
        self.settings = settings;
    }

    pub fn settings(&self) -> &AppSettings {
        &self.settings
    }

    pub fn settings_mut(&mut self) -> &mut AppSettings {
        &mut self.settings
    }

    pub fn is_at_root(&self) -> bool {
        self.current_bucket.is_empty()
    }

    /// Get the currently selected preview node
    pub fn selected_preview(&self) -> Option<&PreviewNode> {
        self.selected_preview
            .as_ref()
            .and_then(|k| self.previews.get(k))
    }

    fn next_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        request_id
    }

    /// Process pending events from the backend. Returns true if any were processed.
    pub fn process_events(&mut self) -> bool {
        let events = match &self.backend {
            Some(b) => b.take_events(),
            None => return false,
        };

        if events.is_empty() {
            return false;
        }

        for event in events {
            match event {
                StateEvent::BucketsLoaded {
                    request_id,
                    buckets,
                } => {
                    if request_id != self.buckets_request_id {
                        continue;
                    }
                    self.buckets = buckets;
                    self.buckets_loading = false;
                    self.buckets_error.clear();
                    self.buckets_complete = true;
                }
                StateEvent::BucketsError { request_id, error } => {
                    if request_id != self.buckets_request_id {
                        continue;
                    }
                    self.buckets_loading = false;
                    self.buckets_error = error;
                    self.buckets_complete = false;
                }
                StateEvent::ObjectsLoaded {
                    bucket,
                    prefix,
                    request_id,
                    continuation_token,
                    objects,
                    next_continuation_token,
                    is_truncated,
                } => {
                    let key = Self::make_node_key(&bucket, &prefix);
                    let mut should_continue_full = false;
                    let mut preload_children = Vec::new();

                    if let Some(node) = self.nodes.get_mut(&key) {
                        if node.request_id != request_id {
                            continue;
                        }

                        if continuation_token.is_empty() {
                            node.objects = objects;
                        } else {
                            let existing_keys: HashSet<String> =
                                node.objects.iter().map(|o| o.key.clone()).collect();
                            for obj in objects {
                                if !existing_keys.contains(&obj.key) {
                                    node.objects.push(obj);
                                }
                            }
                        }

                        node.next_continuation_token = next_continuation_token;
                        node.loading = false;
                        node.status = if is_truncated {
                            DataStatus::Partial
                        } else {
                            DataStatus::Complete
                        };

                        let is_current_folder =
                            bucket == self.current_bucket && prefix == self.current_prefix;
                        if is_current_folder
                            && node.mode == FolderLoadMode::Full
                            && !node.child_preloads_scheduled
                        {
                            node.rebuild_sorted_view_if_needed();
                            preload_children = node
                                .sorted_view
                                .iter()
                                .take(CHILD_PRELOAD_COUNT)
                                .map(|&idx| {
                                    let obj = &node.objects[idx];
                                    (obj.is_folder, obj.key.clone())
                                })
                                .collect();
                            node.child_preloads_scheduled = true;
                        }

                        should_continue_full = is_current_folder
                            && node.mode == FolderLoadMode::Full
                            && node.is_truncated();
                    }

                    for (is_folder, child_key) in preload_children {
                        if is_folder {
                            self.prefetch_folder(&bucket, &child_key);
                        } else {
                            self.prefetch_file(&bucket, &child_key);
                        }
                    }

                    if should_continue_full {
                        self.load_more_with_priority(&bucket, &prefix, RequestPriority::High);
                    }
                }
                StateEvent::ObjectsError {
                    bucket,
                    prefix,
                    request_id,
                    error,
                } => {
                    let key = Self::make_node_key(&bucket, &prefix);
                    if let Some(node) = self.nodes.get_mut(&key) {
                        if node.request_id != request_id {
                            continue;
                        }
                        node.loading = false;
                        if !node.is_loaded() {
                            node.status = DataStatus::Error(error);
                        }
                    }
                }
                StateEvent::PreviewProgress {
                    bucket,
                    key,
                    request_id,
                    status,
                } => {
                    let cache_key = Self::make_preview_cache_key(&bucket, &key);
                    let mut continue_download = None;

                    if let Some(node) = self.previews.get_mut(&cache_key) {
                        if node.request_id != request_id {
                            continue;
                        }
                        node.touch();
                        if node.desired_mode == PreviewLoadMode::Full
                            && matches!(status, StreamingStatus::PrefetchReady)
                        {
                            continue_download = Some((
                                node.bucket.clone(),
                                node.key.clone(),
                                Arc::clone(&node.preview),
                                node.source_bytes(),
                            ));
                        }
                    }

                    if let Some((bucket, key, preview, source_bytes)) = continue_download {
                        let new_request_id = self.next_request_id();
                        if let Some(node) = self.previews.get_mut(&cache_key) {
                            if node.request_id == request_id {
                                node.request_id = new_request_id;
                                node.desired_mode = PreviewLoadMode::Full;
                            } else {
                                continue;
                            }
                        }

                        if let Some(b) = &self.backend {
                            b.streaming_get_object(
                                &bucket,
                                &key,
                                preview,
                                source_bytes,
                                None,
                                RequestPriority::High,
                                new_request_id,
                            );
                        }
                    }
                }
                StateEvent::PreviewError {
                    bucket,
                    key,
                    request_id,
                } => {
                    let cache_key = Self::make_preview_cache_key(&bucket, &key);
                    if let Some(node) = self.previews.get_mut(&cache_key) {
                        if node.request_id != request_id {
                            continue;
                        }
                        node.touch();
                    }
                }
            }
        }
        true
    }

    pub fn refresh(&mut self) {
        self.buckets.clear();
        self.buckets_error.clear();
        self.buckets_complete = false;
        self.nodes.clear();
        self.previews.clear();
        self.selected_preview = None;

        if let Some(b) = &self.backend {
            b.cancel_all();
        }
        self.request_buckets();
    }

    pub fn ensure_buckets_loaded(&mut self) {
        if self.buckets_loading || self.buckets_complete || !self.buckets_error.is_empty() {
            return;
        }
        self.request_buckets();
    }

    pub fn ensure_current_folder_loaded(&mut self) {
        if self.current_bucket.is_empty() {
            return;
        }
        let bucket = self.current_bucket.clone();
        let prefix = self.current_prefix.clone();
        self.ensure_folder_full(&bucket, &prefix);
    }

    pub fn prefetch_folder(&mut self, bucket: &str, prefix: &str) {
        let key = Self::make_node_key(bucket, prefix);
        if let Some(node) = self.nodes.get(&key) {
            if node.loading || node.is_loaded() {
                return;
            }
        }

        self.start_folder_request(
            bucket,
            prefix,
            FolderLoadMode::FirstPage,
            RequestPriority::Low,
        );
    }

    pub fn load_more(&mut self, bucket: &str, prefix: &str) {
        self.load_more_with_priority(bucket, prefix, RequestPriority::High);
    }

    fn load_more_with_priority(&mut self, bucket: &str, prefix: &str, priority: RequestPriority) {
        let key = Self::make_node_key(bucket, prefix);
        let (cont_token, request_id) = {
            let node = match self.nodes.get(&key) {
                Some(n) => n,
                None => return,
            };
            if !node.is_truncated() || node.loading {
                return;
            }
            (node.next_continuation_token.clone(), node.request_id)
        };

        if let Some(node) = self.nodes.get_mut(&key) {
            node.loading = true;
        }

        if let Some(b) = &self.backend {
            b.list_objects(bucket, prefix, &cont_token, priority, request_id);
        }
    }

    pub fn navigate_to(&mut self, s3_path: &str) {
        let (bucket, prefix) = match Self::parse_s3_path(s3_path) {
            Some(bp) => bp,
            None => return,
        };

        if bucket.is_empty() {
            self.cancel_pending_requests();
            self.clear_selection();
            self.current_bucket.clear();
            self.current_prefix.clear();
            self.ensure_buckets_loaded();
            return;
        }

        // Add bucket if not in list
        self.add_manual_bucket(&bucket);
        self.navigate_into(&bucket, &prefix);
    }

    pub fn navigate_up(&mut self) {
        if self.current_bucket.is_empty() {
            return;
        }

        if self.current_prefix.is_empty() {
            self.cancel_pending_requests();
            self.clear_selection();
            self.current_bucket.clear();
            self.current_prefix.clear();
            self.ensure_buckets_loaded();
            return;
        }

        let mut new_prefix = self.current_prefix.clone();
        if new_prefix.ends_with('/') {
            new_prefix.pop();
        }

        new_prefix = match new_prefix.rfind('/') {
            Some(pos) => new_prefix[..pos + 1].to_string(),
            None => String::new(),
        };

        let bucket = self.current_bucket.clone();
        self.navigate_into(&bucket, &new_prefix);
    }

    pub fn navigate_into(&mut self, bucket: &str, prefix: &str) {
        self.cancel_pending_requests();
        self.clear_selection();
        self.current_bucket = bucket.to_string();
        self.current_prefix = prefix.to_string();
        if !bucket.is_empty() {
            let path = if prefix.is_empty() {
                format!("s3://{bucket}/")
            } else {
                format!("s3://{bucket}/{prefix}")
            };
            self.record_recent_path(&path);
        }
        self.ensure_folder_full(bucket, prefix);
        self.prefetch_parent_folder(bucket, prefix);
    }

    pub fn add_manual_bucket(&mut self, bucket_name: &str) {
        if self.buckets.iter().any(|b| b.name == bucket_name) {
            return;
        }
        self.buckets.push(S3Bucket {
            name: bucket_name.to_string(),
        });
    }

    pub fn select_file(&mut self, bucket: &str, key: &str) {
        let cache_key = Self::make_preview_cache_key(bucket, key);
        self.selected_preview = Some(cache_key.clone());

        if !Self::is_preview_supported(key) {
            self.previews.entry(cache_key).or_insert_with(|| {
                PreviewNode::new_unsupported(bucket.to_string(), key.to_string())
            });
            return;
        }

        let existing = self.previews.get(&cache_key).map(|node| {
            (
                node.streaming_status(),
                node.request_id,
                Arc::clone(&node.preview),
                node.source_bytes(),
            )
        });

        if let Some((status, request_id, preview, source_bytes)) = existing {
            if let Some(node) = self.previews.get_mut(&cache_key) {
                node.touch();
                node.desired_mode = PreviewLoadMode::Full;
            }

            match status {
                StreamingStatus::PrefetchReady => {
                    self.start_preview_download(
                        &cache_key,
                        &bucket.to_string(),
                        &key.to_string(),
                        preview,
                        source_bytes,
                        RequestPriority::High,
                    );
                }
                StreamingStatus::Downloading | StreamingStatus::Complete => {}
                StreamingStatus::Prefetching | StreamingStatus::Error(_) => {
                    if let Some(node) = self.previews.get(&cache_key) {
                        if node.request_id != request_id {
                            return;
                        }
                    }
                    self.start_new_preview_request(
                        bucket,
                        key,
                        PreviewLoadMode::Full,
                        RequestPriority::High,
                        None,
                    );
                }
            }
            return;
        }

        self.start_new_preview_request(
            bucket,
            key,
            PreviewLoadMode::Full,
            RequestPriority::High,
            None,
        );
    }

    pub fn prefetch_file(&mut self, bucket: &str, key: &str) {
        if !Self::is_preview_supported(key) {
            return;
        }

        let cache_key = Self::make_preview_cache_key(bucket, key);
        if let Some(node) = self.previews.get_mut(&cache_key) {
            node.touch();
            return;
        }

        self.start_new_preview_request(
            bucket,
            key,
            PreviewLoadMode::Prefetch,
            RequestPriority::Low,
            Some(PREFETCH_BYTES),
        );
    }

    pub fn clear_selection(&mut self) {
        self.selected_preview = None;
    }

    pub fn record_recent_path(&mut self, path: &str) {
        if path.is_empty() || path == "s3://" {
            return;
        }

        let Some(profile_name) = self.selected_profile_name().map(str::to_owned) else {
            return;
        };

        let entries = self.settings.frecent_paths.entry(profile_name).or_default();
        let now = current_unix_timestamp();

        if let Some(entry) = entries.iter_mut().find(|entry| entry.path == path) {
            entry.score += 1.0;
            entry.last_accessed = now;
        } else {
            entries.push(PathEntry {
                path: path.to_string(),
                score: 1.0,
                last_accessed: now,
            });
        }

        if entries.len() > MAX_FRECENT_PATHS {
            entries.sort_by(|a, b| b.score.total_cmp(&a.score));
            entries.truncate(MAX_FRECENT_PATHS);
        }
    }

    pub fn top_frecent_paths(&self, count: usize) -> Vec<String> {
        let Some(profile_name) = self.selected_profile_name() else {
            return Vec::new();
        };
        let Some(entries) = self.settings.frecent_paths.get(profile_name) else {
            return Vec::new();
        };

        let now = current_unix_timestamp();
        let mut scored: Vec<(f64, &str)> = entries
            .iter()
            .map(|entry| (frecency_score(entry, now), entry.path.as_str()))
            .collect();
        scored.sort_by(|a, b| b.0.total_cmp(&a.0));
        scored
            .into_iter()
            .take(count)
            .map(|(_, path)| path.to_string())
            .collect()
    }

    /// Evict oldest preview entries if cache exceeds limit
    fn evict_old_previews(&mut self) {
        while self.previews.len() > PREVIEW_CACHE_MAX_ENTRIES {
            // Find oldest entry (excluding selected)
            let oldest = self
                .previews
                .iter()
                .filter(|(k, _)| self.selected_preview.as_ref() != Some(*k))
                .min_by_key(|(_, v)| v.last_accessed)
                .map(|(k, _)| k.clone());

            if let Some(key) = oldest {
                self.previews.remove(&key);
            } else {
                break;
            }
        }
    }

    fn request_buckets(&mut self) {
        if self.backend.is_none() {
            return;
        }

        self.buckets_loading = true;
        self.buckets_error.clear();
        let request_id = self.next_request_id();
        self.buckets_request_id = request_id;

        if let Some(backend) = &self.backend {
            backend.list_buckets(request_id);
        }
    }

    fn cancel_pending_requests(&mut self) {
        if let Some(b) = &self.backend {
            b.cancel_pending(RequestPriority::High);
            b.cancel_pending(RequestPriority::Low);
        }

        for node in self.nodes.values_mut() {
            if !node.loading {
                continue;
            }

            node.loading = false;
            node.request_id = 0;
            node.child_preloads_scheduled = false;

            if !node.is_loaded() {
                node.status = DataStatus::Empty;
                node.next_continuation_token.clear();
            }

            if node.mode == FolderLoadMode::Full {
                node.mode = FolderLoadMode::FirstPage;
            }
        }

        for preview in self.previews.values_mut() {
            if matches!(
                preview.streaming_status(),
                StreamingStatus::Prefetching | StreamingStatus::Downloading
            ) {
                preview
                    .preview
                    .set_status(StreamingStatus::Error("Cancelled".to_string()));
                preview.request_id = 0;
                preview.desired_mode = PreviewLoadMode::Prefetch;
            }
        }
    }

    fn ensure_folder_full(&mut self, bucket: &str, prefix: &str) {
        enum Action {
            StartFresh,
            ContinuePartial,
            ScheduleChildrenOnly,
            None,
        }

        let key = Self::make_node_key(bucket, prefix);
        let action = match self.nodes.get(&key) {
            None => Action::StartFresh,
            Some(node) if node.loading && node.mode == FolderLoadMode::Full => Action::None,
            Some(node) if node.loading => Action::StartFresh,
            Some(node) if matches!(node.status, DataStatus::Complete) => {
                Action::ScheduleChildrenOnly
            }
            Some(node) if node.is_truncated() => Action::ContinuePartial,
            Some(_) => Action::StartFresh,
        };

        match action {
            Action::StartFresh => self.start_folder_request(
                bucket,
                prefix,
                FolderLoadMode::Full,
                RequestPriority::High,
            ),
            Action::ContinuePartial => {
                let request_id = self.next_request_id();
                let key = Self::make_node_key(bucket, prefix);
                let continuation_token = match self.nodes.get_mut(&key) {
                    Some(node) => {
                        node.loading = true;
                        node.mode = FolderLoadMode::Full;
                        node.request_id = request_id;
                        node.child_preloads_scheduled = false;
                        node.next_continuation_token.clone()
                    }
                    None => return,
                };

                self.schedule_child_preloads(bucket, prefix);

                if let Some(b) = &self.backend {
                    b.list_objects(
                        bucket,
                        prefix,
                        &continuation_token,
                        RequestPriority::High,
                        request_id,
                    );
                }
            }
            Action::ScheduleChildrenOnly => {
                if let Some(node) = self.nodes.get_mut(&key) {
                    node.mode = FolderLoadMode::Full;
                }
                self.schedule_child_preloads(bucket, prefix);
            }
            Action::None => {}
        }
    }

    fn start_folder_request(
        &mut self,
        bucket: &str,
        prefix: &str,
        mode: FolderLoadMode,
        priority: RequestPriority,
    ) {
        let request_id = self.next_request_id();
        let node = self.get_or_create_node(bucket, prefix);
        node.reset_for_request(request_id, mode);

        if let Some(b) = &self.backend {
            b.list_objects(bucket, prefix, "", priority, request_id);
        }
    }

    fn prefetch_parent_folder(&mut self, bucket: &str, prefix: &str) {
        if let Some(parent_prefix) = Self::parent_prefix(prefix) {
            self.prefetch_folder(bucket, &parent_prefix);
        }
    }

    fn schedule_child_preloads(&mut self, bucket: &str, prefix: &str) {
        let key = Self::make_node_key(bucket, prefix);
        let children = {
            let node = match self.nodes.get_mut(&key) {
                Some(node) => node,
                None => return,
            };

            if node.child_preloads_scheduled {
                return;
            }

            node.rebuild_sorted_view_if_needed();
            node.child_preloads_scheduled = true;
            node.sorted_view
                .iter()
                .take(CHILD_PRELOAD_COUNT)
                .map(|&idx| {
                    let obj = &node.objects[idx];
                    (obj.is_folder, obj.key.clone())
                })
                .collect::<Vec<_>>()
        };

        for (is_folder, child_key) in children {
            if is_folder {
                self.prefetch_folder(bucket, &child_key);
            } else {
                self.prefetch_file(bucket, &child_key);
            }
        }
    }

    fn start_new_preview_request(
        &mut self,
        bucket: &str,
        key: &str,
        desired_mode: PreviewLoadMode,
        priority: RequestPriority,
        max_bytes: Option<u64>,
    ) {
        let cache_key = Self::make_preview_cache_key(bucket, key);
        let request_id = self.next_request_id();
        // WARC objects are parsed (and gzip-member-decoded) by the WARC gallery
        // itself, so keep the on-disk preview as the raw object bytes rather than
        // letting the single-stream gz/zstd transform touch them.
        let lower = key.to_lowercase();
        let compression = if lower.ends_with(".warc") || lower.ends_with(".warc.gz") {
            Compression::None
        } else {
            Compression::from_filename(key)
        };

        let preview = match StreamingFilePreview::new(compression) {
            Ok(preview) => Arc::new(preview),
            Err(error) => {
                let preview = Arc::new(
                    StreamingFilePreview::new(Compression::None)
                        .expect("Failed to create fallback preview"),
                );
                preview.set_status(StreamingStatus::Error(error));
                self.previews.insert(
                    cache_key,
                    PreviewNode::new(
                        bucket.to_string(),
                        key.to_string(),
                        preview,
                        request_id,
                        desired_mode,
                    ),
                );
                self.evict_old_previews();
                return;
            }
        };

        self.previews.insert(
            cache_key,
            PreviewNode::new(
                bucket.to_string(),
                key.to_string(),
                Arc::clone(&preview),
                request_id,
                desired_mode,
            ),
        );
        self.evict_old_previews();

        if let Some(b) = &self.backend {
            b.streaming_get_object(bucket, key, preview, 0, max_bytes, priority, request_id);
        }
    }

    fn start_preview_download(
        &mut self,
        cache_key: &str,
        bucket: &str,
        key: &str,
        preview: Arc<StreamingFilePreview>,
        range_start: u64,
        priority: RequestPriority,
    ) {
        let request_id = self.next_request_id();
        if let Some(node) = self.previews.get_mut(cache_key) {
            node.request_id = request_id;
            node.desired_mode = PreviewLoadMode::Full;
            node.touch();
        } else {
            return;
        }

        if let Some(b) = &self.backend {
            b.streaming_get_object(
                bucket,
                key,
                preview,
                range_start,
                None,
                priority,
                request_id,
            );
        }
    }

    pub fn select_profile(&mut self, index: usize) {
        if index >= self.profiles.len() || index == self.selected_profile_idx {
            return;
        }
        self.selected_profile_idx = index;
        if let Some(b) = &self.backend {
            b.cancel_all();
        }
        self.buckets.clear();
        self.buckets_error.clear();
        self.buckets_complete = false;
        self.nodes.clear();
        self.previews.clear();
        self.selected_preview = None;
        self.current_bucket.clear();
        self.current_prefix.clear();
        // Note: the backend needs to be recreated by the caller with the new profile
    }

    pub fn get_node(&self, bucket: &str, prefix: &str) -> Option<&FolderNode> {
        self.nodes.get(&Self::make_node_key(bucket, prefix))
    }

    pub fn get_node_mut(&mut self, bucket: &str, prefix: &str) -> Option<&mut FolderNode> {
        self.nodes.get_mut(&Self::make_node_key(bucket, prefix))
    }

    fn get_or_create_node(&mut self, bucket: &str, prefix: &str) -> &mut FolderNode {
        let key = Self::make_node_key(bucket, prefix);
        self.nodes
            .entry(key)
            .or_insert_with(|| FolderNode::new(bucket.to_string(), prefix.to_string()))
    }

    fn make_node_key(bucket: &str, prefix: &str) -> String {
        format!("{}/{}", bucket, prefix)
    }

    fn make_preview_cache_key(bucket: &str, key: &str) -> String {
        format!("{}/{}", bucket, key)
    }

    fn parent_prefix(prefix: &str) -> Option<String> {
        if prefix.is_empty() {
            return None;
        }

        let trimmed = prefix.trim_end_matches('/');
        if trimmed.is_empty() {
            return Some(String::new());
        }

        match trimmed.rfind('/') {
            Some(pos) => Some(trimmed[..pos + 1].to_string()),
            None => Some(String::new()),
        }
    }

    fn parse_s3_path(path: &str) -> Option<(String, String)> {
        let mut p = path;
        if let Some(rest) = p.strip_prefix("s3://") {
            p = rest;
        } else if let Some(rest) = p.strip_prefix("s3:") {
            p = rest;
        }
        let p = p.trim_start_matches('/');

        if p.is_empty() {
            return Some((String::new(), String::new()));
        }

        match p.find('/') {
            Some(slash) => Some((p[..slash].to_string(), p[slash + 1..].to_string())),
            None => Some((p.to_string(), String::new())),
        }
    }

    fn is_preview_supported(key: &str) -> bool {
        let dot_pos = match key.rfind('.') {
            Some(p) => p,
            // Extension-less objects (e.g. Firehose-written WARC batches) are allowed
            // through so the WARC gallery can content-sniff them; otherwise they'd
            // fall back to the (harmless) text preview.
            None => return true,
        };
        let mut ext = key[dot_pos..].to_lowercase();

        // Handle compressed files
        if matches!(ext.as_str(), ".gz" | ".zst" | ".zstd") && dot_pos > 0 {
            let inner = &key[..dot_pos];
            if let Some(inner_dot) = inner.rfind('.') {
                ext = inner[inner_dot..].to_lowercase();
            } else {
                return false;
            }
        }

        matches!(
            ext.as_str(),
            ".txt"
                | ".md"
                | ".markdown"
                | ".rst"
                | ".log"
                | ".html"
                | ".htm"
                | ".xml"
                | ".svg"
                | ".css"
                | ".scss"
                | ".json"
                | ".jsonl"
                | ".ndjson"
                | ".yaml"
                | ".yml"
                | ".toml"
                | ".csv"
                | ".tsv"
                | ".ini"
                | ".cfg"
                | ".conf"
                | ".env"
                | ".c"
                | ".h"
                | ".cpp"
                | ".hpp"
                | ".cc"
                | ".java"
                | ".py"
                | ".pyw"
                | ".js"
                | ".mjs"
                | ".jsx"
                | ".ts"
                | ".tsx"
                | ".rb"
                | ".php"
                | ".lua"
                | ".go"
                | ".rs"
                | ".swift"
                | ".zig"
                | ".sh"
                | ".bash"
                | ".zsh"
                | ".sql"
                | ".dockerfile"
                | ".tf"
                | ".proto"
                | ".diff"
                | ".patch"
                | ".cmake"
                | ".makefile"
                | ".mk"
                | ".gitignore"
                | ".properties"
                | ".warc"
        )
    }

    fn selected_profile_name(&self) -> Option<&str> {
        self.profiles
            .get(self.selected_profile_idx)
            .map(|profile| profile.name.as_str())
            .filter(|name| !name.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum RecordedRequest {
        Buckets {
            request_id: u64,
        },
        List {
            bucket: String,
            prefix: String,
            continuation_token: String,
            priority: RequestPriority,
            request_id: u64,
        },
        Stream {
            bucket: String,
            key: String,
            range_start: u64,
            max_bytes: Option<u64>,
            priority: RequestPriority,
            request_id: u64,
        },
    }

    #[derive(Default)]
    struct MockBackendState {
        requests: Mutex<Vec<RecordedRequest>>,
        events: Mutex<Vec<StateEvent>>,
    }

    #[derive(Clone, Default)]
    struct MockBackend {
        state: Arc<MockBackendState>,
    }

    impl MockBackend {
        fn new() -> Self {
            Self::default()
        }

        fn requests(&self) -> Vec<RecordedRequest> {
            self.state.lock_requests().clone()
        }

        fn drain_requests(&self) -> Vec<RecordedRequest> {
            let mut requests = self.state.lock_requests();
            std::mem::take(&mut *requests)
        }

        fn push_event(&self, event: StateEvent) {
            self.state.lock_events().push(event);
        }
    }

    impl MockBackendState {
        fn lock_requests(&self) -> std::sync::MutexGuard<'_, Vec<RecordedRequest>> {
            self.requests.lock().unwrap()
        }

        fn lock_events(&self) -> std::sync::MutexGuard<'_, Vec<StateEvent>> {
            self.events.lock().unwrap()
        }
    }

    impl Backend for MockBackend {
        fn take_events(&self) -> Vec<StateEvent> {
            let mut events = self.state.lock_events();
            std::mem::take(&mut *events)
        }

        fn list_buckets(&self, request_id: u64) {
            self.state
                .lock_requests()
                .push(RecordedRequest::Buckets { request_id });
        }

        fn list_objects(
            &self,
            bucket: &str,
            prefix: &str,
            continuation_token: &str,
            priority: RequestPriority,
            request_id: u64,
        ) {
            self.state.lock_requests().push(RecordedRequest::List {
                bucket: bucket.to_string(),
                prefix: prefix.to_string(),
                continuation_token: continuation_token.to_string(),
                priority,
                request_id,
            });
        }

        fn streaming_get_object(
            &self,
            bucket: &str,
            key: &str,
            _preview: Arc<StreamingFilePreview>,
            range_start: u64,
            max_bytes: Option<u64>,
            priority: RequestPriority,
            request_id: u64,
        ) {
            self.state.lock_requests().push(RecordedRequest::Stream {
                bucket: bucket.to_string(),
                key: key.to_string(),
                range_start,
                max_bytes,
                priority,
                request_id,
            });
        }

        fn cancel_all(&self) {
            self.state.lock_requests().clear();
        }

        fn cancel_pending(&self, _priority: RequestPriority) {}
    }

    fn model_with_backend() -> (BrowserModel, MockBackend) {
        let backend = MockBackend::new();
        let mut model = BrowserModel::new();
        model.set_backend(Box::new(backend.clone()));
        (model, backend)
    }

    #[test]
    fn refresh_ignores_stale_bucket_results() {
        let (mut model, backend) = model_with_backend();

        model.refresh();
        let first_request_id = match &backend.requests()[0] {
            RecordedRequest::Buckets { request_id } => *request_id,
            other => panic!("unexpected request: {other:?}"),
        };

        backend.drain_requests();
        model.refresh();
        let second_request_id = match &backend.requests()[0] {
            RecordedRequest::Buckets { request_id } => *request_id,
            other => panic!("unexpected request: {other:?}"),
        };

        backend.push_event(StateEvent::BucketsLoaded {
            request_id: first_request_id,
            buckets: vec![S3Bucket {
                name: "stale".to_string(),
            }],
        });
        model.process_events();
        assert!(model.buckets.is_empty());
        assert!(model.buckets_loading);

        backend.push_event(StateEvent::BucketsLoaded {
            request_id: second_request_id,
            buckets: vec![S3Bucket {
                name: "fresh".to_string(),
            }],
        });
        model.process_events();

        assert_eq!(model.buckets.len(), 1);
        assert_eq!(model.buckets[0].name, "fresh");
        assert!(!model.buckets_loading);
    }

    #[test]
    fn file_hover_prefetch_then_click_reissues_high_priority_full_download() {
        let (mut model, backend) = model_with_backend();

        model.prefetch_file("bucket", "path/file.txt");
        assert_eq!(
            backend.requests(),
            vec![RecordedRequest::Stream {
                bucket: "bucket".to_string(),
                key: "path/file.txt".to_string(),
                range_start: 0,
                max_bytes: Some(PREFETCH_BYTES),
                priority: RequestPriority::Low,
                request_id: 1,
            }]
        );

        model.select_file("bucket", "path/file.txt");
        assert_eq!(
            backend.requests(),
            vec![
                RecordedRequest::Stream {
                    bucket: "bucket".to_string(),
                    key: "path/file.txt".to_string(),
                    range_start: 0,
                    max_bytes: Some(PREFETCH_BYTES),
                    priority: RequestPriority::Low,
                    request_id: 1,
                },
                RecordedRequest::Stream {
                    bucket: "bucket".to_string(),
                    key: "path/file.txt".to_string(),
                    range_start: 0,
                    max_bytes: None,
                    priority: RequestPriority::High,
                    request_id: 2,
                },
            ]
        );
    }

    #[test]
    fn folder_hover_prefetch_then_click_reissues_high_priority_full_listing() {
        let (mut model, backend) = model_with_backend();

        model.prefetch_folder("bucket", "prefix/");
        assert_eq!(
            backend.requests(),
            vec![RecordedRequest::List {
                bucket: "bucket".to_string(),
                prefix: "prefix/".to_string(),
                continuation_token: String::new(),
                priority: RequestPriority::Low,
                request_id: 1,
            }]
        );

        model.navigate_into("bucket", "prefix/");
        assert_eq!(
            backend.requests(),
            vec![
                RecordedRequest::List {
                    bucket: "bucket".to_string(),
                    prefix: "prefix/".to_string(),
                    continuation_token: String::new(),
                    priority: RequestPriority::Low,
                    request_id: 1,
                },
                RecordedRequest::List {
                    bucket: "bucket".to_string(),
                    prefix: "prefix/".to_string(),
                    continuation_token: String::new(),
                    priority: RequestPriority::High,
                    request_id: 2,
                },
                RecordedRequest::List {
                    bucket: "bucket".to_string(),
                    prefix: String::new(),
                    continuation_token: String::new(),
                    priority: RequestPriority::Low,
                    request_id: 3,
                },
            ]
        );
    }

    #[test]
    fn navigating_into_folder_prefetches_parent_for_dotdot_entry() {
        let (mut model, backend) = model_with_backend();

        model.navigate_into("bucket", "parent/child/");

        assert_eq!(
            backend.requests(),
            vec![
                RecordedRequest::List {
                    bucket: "bucket".to_string(),
                    prefix: "parent/child/".to_string(),
                    continuation_token: String::new(),
                    priority: RequestPriority::High,
                    request_id: 1,
                },
                RecordedRequest::List {
                    bucket: "bucket".to_string(),
                    prefix: "parent/".to_string(),
                    continuation_token: String::new(),
                    priority: RequestPriority::Low,
                    request_id: 2,
                },
            ]
        );
    }

    #[test]
    fn active_folder_first_page_schedules_child_preloads() {
        let (mut model, backend) = model_with_backend();

        model.navigate_into("bucket", "prefix/");
        backend.drain_requests();

        let request_id = model.get_node("bucket", "prefix/").unwrap().request_id;
        backend.push_event(StateEvent::ObjectsLoaded {
            bucket: "bucket".to_string(),
            prefix: "prefix/".to_string(),
            request_id,
            continuation_token: String::new(),
            objects: vec![
                S3Object {
                    key: "prefix/child/".to_string(),
                    display_name: "child".to_string(),
                    size: 0,
                    is_folder: true,
                },
                S3Object {
                    key: "prefix/file.txt".to_string(),
                    display_name: "file.txt".to_string(),
                    size: 16,
                    is_folder: false,
                },
            ],
            next_continuation_token: String::new(),
            is_truncated: false,
        });

        model.process_events();

        assert_eq!(
            backend.requests(),
            vec![
                RecordedRequest::List {
                    bucket: "bucket".to_string(),
                    prefix: "prefix/child/".to_string(),
                    continuation_token: String::new(),
                    priority: RequestPriority::Low,
                    request_id: 3,
                },
                RecordedRequest::Stream {
                    bucket: "bucket".to_string(),
                    key: "prefix/file.txt".to_string(),
                    range_start: 0,
                    max_bytes: Some(PREFETCH_BYTES),
                    priority: RequestPriority::Low,
                    request_id: 4,
                },
            ]
        );
    }

    #[test]
    fn navigation_cancels_inflight_preview_and_allows_reload() {
        let (mut model, backend) = model_with_backend();

        model.select_file("bucket", "path/file.txt");
        assert_eq!(backend.requests().len(), 1);

        model.navigate_into("bucket", "other/");

        let preview_key = BrowserModel::make_preview_cache_key("bucket", "path/file.txt");
        let preview = model.previews.get(&preview_key).unwrap();
        assert!(matches!(
            preview.streaming_status(),
            StreamingStatus::Error(ref error) if error == "Cancelled"
        ));

        backend.drain_requests();
        model.select_file("bucket", "path/file.txt");
        assert_eq!(backend.requests().len(), 1);
        match &backend.requests()[0] {
            RecordedRequest::Stream {
                bucket,
                key,
                priority,
                ..
            } => {
                assert_eq!(bucket, "bucket");
                assert_eq!(key, "path/file.txt");
                assert_eq!(*priority, RequestPriority::High);
            }
            other => panic!("unexpected request: {other:?}"),
        }
    }

    #[test]
    fn recent_paths_are_tracked_per_profile() {
        let mut model = BrowserModel::new();
        model.profiles = vec![
            crate::aws::credentials::AwsProfile {
                name: "default".to_string(),
                ..Default::default()
            },
            crate::aws::credentials::AwsProfile {
                name: "other".to_string(),
                ..Default::default()
            },
        ];

        model.navigate_into("alpha", "first/");
        assert_eq!(model.top_frecent_paths(5), vec!["s3://alpha/first/"]);

        model.select_profile(1);
        assert!(model.top_frecent_paths(5).is_empty());

        model.navigate_into("beta", "");
        assert_eq!(model.top_frecent_paths(5), vec!["s3://beta/"]);

        model.select_profile(0);
        assert_eq!(model.top_frecent_paths(5), vec!["s3://alpha/first/"]);
    }

    #[test]
    fn root_path_is_not_recorded_as_recent() {
        let mut model = BrowserModel::new();
        model.profiles = vec![crate::aws::credentials::AwsProfile {
            name: "default".to_string(),
            ..Default::default()
        }];

        model.navigate_to("s3://");
        assert!(model.top_frecent_paths(5).is_empty());
    }

    #[test]
    fn navigating_up_to_root_requests_full_bucket_list_after_manual_bucket_navigation() {
        let (mut model, backend) = model_with_backend();

        model.navigate_to("s3://manual-bucket/prefix/");
        assert_eq!(model.buckets.len(), 1);
        assert_eq!(model.buckets[0].name, "manual-bucket");

        backend.drain_requests();
        model.navigate_up();
        backend.drain_requests();

        model.navigate_up();

        assert!(model.is_at_root());
        assert_eq!(backend.requests().len(), 1);
        assert!(matches!(
            backend.requests().as_slice(),
            [RecordedRequest::Buckets { .. }]
        ));
    }
}
