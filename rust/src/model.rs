use crate::backend::{Backend, BackendDebugSnapshot, RequestPriority};
use crate::events::{S3Bucket, S3Object, StateEvent};
use crate::preview::{Compression, PREFETCH_BYTES, StreamingFilePreview, StreamingStatus};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

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

/// A cached folder's contents
pub struct FolderNode {
    pub bucket: String,
    pub prefix: String,
    pub objects: Vec<S3Object>,
    pub next_continuation_token: String,
    pub loading: bool,
    pub status: DataStatus,

    // Cached sorted view: indices into objects[] (folders first, then files)
    pub sorted_view: Vec<usize>,
    pub folder_count: usize,
    cached_objects_size: usize,
}

impl FolderNode {
    fn new(bucket: String, prefix: String) -> Self {
        Self {
            bucket,
            prefix,
            objects: Vec::new(),
            next_continuation_token: String::new(),
            loading: false,
            status: DataStatus::Empty,
            sorted_view: Vec::new(),
            folder_count: 0,
            cached_objects_size: 0,
        }
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
#[derive(Clone, PartialEq, Debug)]
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

#[derive(Clone, Debug)]
pub struct SelectedPreviewDebug {
    pub bucket: String,
    pub key: String,
    pub status: PreviewStatus,
    pub bytes_written: u64,
    pub source_bytes: u64,
    pub total_source_size: Option<u64>,
    pub can_continue_download: bool,
    pub is_complete: bool,
    pub request_started: bool,
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
    /// True while a backend streaming request is active for this preview object.
    request_started: bool,
    /// If set, start full download as soon as prefetch reaches PrefetchReady.
    promote_to_full_on_ready: bool,
}

impl PreviewNode {
    fn new(bucket: String, key: String, preview: Arc<StreamingFilePreview>) -> Self {
        Self {
            bucket,
            key,
            preview,
            last_accessed: Instant::now(),
            unsupported: false,
            request_started: false,
            promote_to_full_on_ready: false,
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
            request_started: false,
            promote_to_full_on_ready: false,
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

    pub fn is_loading(&self) -> bool {
        matches!(self.status(), PreviewStatus::Loading)
    }

    pub fn error(&self) -> Option<String> {
        match self.preview.status() {
            StreamingStatus::Error(e) => Some(e),
            _ => None,
        }
    }

    /// Get line count
    pub fn line_count(&self) -> usize {
        self.preview.line_count()
    }

    /// Get decompressed bytes written
    pub fn bytes_written(&self) -> u64 {
        self.preview.bytes_written()
    }

    /// Get source bytes downloaded
    pub fn source_bytes(&self) -> u64 {
        self.preview.source_bytes()
    }

    /// Read lines for display
    pub fn read_lines(&self, start_line: usize, count: usize) -> Vec<String> {
        self.preview.read_lines(start_line, count)
    }

    /// Check if more data can be downloaded
    pub fn can_continue_download(&self) -> bool {
        matches!(self.preview.status(), StreamingStatus::PrefetchReady)
    }

    /// Check if download is complete
    pub fn is_complete(&self) -> bool {
        matches!(self.preview.status(), StreamingStatus::Complete)
    }
}

const PREVIEW_CACHE_MAX_ENTRIES: usize = 50;
const DEFAULT_AUTO_PRELOAD_COUNT: usize = 20;

/// The browser model - owns state and processes commands
pub struct BrowserModel {
    backend: Option<Box<dyn Backend>>,

    // Profiles
    pub profiles: Vec<crate::aws::credentials::AwsProfile>,
    pub selected_profile_idx: usize,

    // Buckets
    pub buckets: Vec<S3Bucket>,
    pub buckets_loading: bool,
    pub buckets_error: String,

    // Folder nodes cache
    nodes: HashMap<String, FolderNode>,

    // Current navigation path
    pub current_bucket: String,
    pub current_prefix: String,
    current_folder_load_all: bool,

    // Preview cache with LRU eviction
    previews: HashMap<String, PreviewNode>,
    pub selected_preview: Option<String>, // key into previews hashmap

    // Preload tuning
    auto_preload_count: usize,

    // Request tracking and stale-result filtering
    request_generation: u64,
    folder_requests: HashMap<String, (u64, RequestPriority)>,
}

impl BrowserModel {
    pub fn new() -> Self {
        Self {
            backend: None,
            profiles: Vec::new(),
            selected_profile_idx: 0,
            buckets: Vec::new(),
            buckets_loading: false,
            buckets_error: String::new(),
            nodes: HashMap::new(),
            current_bucket: String::new(),
            current_prefix: String::new(),
            current_folder_load_all: false,
            previews: HashMap::new(),
            selected_preview: None,
            auto_preload_count: DEFAULT_AUTO_PRELOAD_COUNT,
            request_generation: 0,
            folder_requests: HashMap::new(),
        }
    }

    pub fn set_backend(&mut self, backend: Box<dyn Backend>) {
        self.backend = Some(backend);
        self.request_generation = self.request_generation.wrapping_add(1);
        self.folder_requests.clear();
    }

    pub fn is_at_root(&self) -> bool {
        self.current_bucket.is_empty()
    }

    pub fn has_selection(&self) -> bool {
        self.selected_preview.is_some()
    }

    pub fn set_auto_preload_count(&mut self, count: usize) {
        self.auto_preload_count = count.max(1);
    }

    pub fn auto_preload_count(&self) -> usize {
        self.auto_preload_count
    }

    pub fn debug_request_generation(&self) -> u64 {
        self.request_generation
    }

    pub fn debug_folder_request_count(&self) -> usize {
        self.folder_requests.len()
    }

    pub fn backend_debug_snapshot(&self) -> Option<BackendDebugSnapshot> {
        self.backend.as_ref().map(|b| b.debug_snapshot())
    }

    pub fn selected_preview_debug(&self) -> Option<SelectedPreviewDebug> {
        let node = self.selected_preview()?;
        Some(SelectedPreviewDebug {
            bucket: node.bucket.clone(),
            key: node.key.clone(),
            status: node.status(),
            bytes_written: node.bytes_written(),
            source_bytes: node.source_bytes(),
            total_source_size: node.preview.total_source_size(),
            can_continue_download: node.can_continue_download(),
            is_complete: node.is_complete(),
            request_started: node.request_started,
        })
    }

    /// Get the currently selected preview node
    pub fn selected_preview(&self) -> Option<&PreviewNode> {
        self.selected_preview
            .as_ref()
            .and_then(|k| self.previews.get(k))
    }

    /// Get the currently selected preview node mutably
    pub fn selected_preview_mut(&mut self) -> Option<&mut PreviewNode> {
        match &self.selected_preview {
            Some(k) => self.previews.get_mut(k),
            None => None,
        }
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

        let mut pending_auto_preloads: Vec<(String, String)> = Vec::new();
        let mut pending_promotions: Vec<(String, String, Arc<StreamingFilePreview>)> = Vec::new();

        for event in events {
            match event {
                StateEvent::BucketsLoaded { buckets } => {
                    self.buckets = buckets;
                    self.buckets_loading = false;
                    self.buckets_error.clear();
                }
                StateEvent::BucketsError { error } => {
                    self.buckets_loading = false;
                    self.buckets_error = error;
                }
                StateEvent::ObjectsLoaded {
                    bucket,
                    prefix,
                    continuation_token,
                    objects,
                    next_continuation_token,
                    is_truncated,
                } => {
                    if !self.complete_folder_request_if_current(
                        &bucket,
                        &prefix,
                        &continuation_token,
                    ) {
                        continue;
                    }

                    let node = self.get_or_create_node(&bucket, &prefix);

                    if continuation_token.is_empty() {
                        node.objects = objects;
                        node.sorted_view.clear();
                        node.folder_count = 0;
                        node.cached_objects_size = 0;
                    } else {
                        // Append, deduplicating
                        let mut existing_keys: HashSet<String> =
                            node.objects.iter().map(|o| o.key.clone()).collect();
                        for obj in objects {
                            if existing_keys.insert(obj.key.clone()) {
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

                    if continuation_token.is_empty()
                        && bucket == self.current_bucket
                        && prefix == self.current_prefix
                    {
                        pending_auto_preloads.push((bucket.clone(), prefix.clone()));
                    }

                    if bucket == self.current_bucket
                        && prefix == self.current_prefix
                        && self.current_folder_load_all
                        && is_truncated
                    {
                        self.load_more(&bucket, &prefix);
                    }
                }
                StateEvent::ObjectsError {
                    bucket,
                    prefix,
                    continuation_token,
                    error,
                } => {
                    if !self.complete_folder_request_if_current(
                        &bucket,
                        &prefix,
                        &continuation_token,
                    ) {
                        continue;
                    }

                    if let Some(node) = self.get_node_mut(&bucket, &prefix) {
                        node.loading = false;
                        // Only set error status if we don't already have data
                        if !node.is_loaded() {
                            node.status = DataStatus::Error(error);
                        }
                    } else if continuation_token.is_empty()
                        && bucket == self.current_bucket
                        && prefix == self.current_prefix
                    {
                        let node = self.get_or_create_node(&bucket, &prefix);
                        node.loading = false;
                        node.status = DataStatus::Error(error);
                    }
                }
                StateEvent::ObjectContentLoaded {
                    bucket,
                    key,
                    content,
                } => {
                    // Legacy event - no longer used for streaming previews
                    // but kept for compatibility
                    let _ = (bucket, key, content);
                }
                StateEvent::ObjectContentError { bucket, key, error } => {
                    // Legacy event - no longer used for streaming previews
                    let _ = (bucket, key, error);
                }
                StateEvent::PreviewProgress {
                    bucket,
                    key,
                    decompressed_bytes: _,
                    source_bytes: _,
                    line_count: _,
                    status,
                } => {
                    // Preview progress is stored in the Arc<StreamingFilePreview>
                    let cache_key = Self::make_preview_cache_key(&bucket, &key);
                    if let Some(node) = self.previews.get_mut(&cache_key) {
                        node.touch();

                        if status == StreamingStatus::PrefetchReady && node.promote_to_full_on_ready
                        {
                            node.promote_to_full_on_ready = false;
                            pending_promotions.push((
                                node.bucket.clone(),
                                node.key.clone(),
                                Arc::clone(&node.preview),
                            ));
                        } else if matches!(
                            status,
                            StreamingStatus::Complete | StreamingStatus::Error(_)
                        ) {
                            node.promote_to_full_on_ready = false;
                        }

                        if matches!(
                            status,
                            StreamingStatus::PrefetchReady
                                | StreamingStatus::Complete
                                | StreamingStatus::Error(_)
                        ) {
                            node.request_started = false;
                        }
                    }
                }
                StateEvent::PreviewError {
                    bucket,
                    key,
                    error: _,
                } => {
                    // Error is stored in the Arc<StreamingFilePreview>
                    let cache_key = Self::make_preview_cache_key(&bucket, &key);
                    if let Some(node) = self.previews.get_mut(&cache_key) {
                        node.touch();
                        node.request_started = false;
                        node.promote_to_full_on_ready = false;
                    }
                }
            }
        }

        for (bucket, prefix) in pending_auto_preloads {
            self.preload_first_children(&bucket, &prefix);
        }

        for (bucket, key, preview) in pending_promotions {
            let range_start = preview.source_bytes();
            self.start_streaming_download(
                &bucket,
                &key,
                preview,
                range_start,
                None,
                RequestPriority::High,
            );
        }

        true
    }

    pub fn refresh(&mut self) {
        self.bump_request_generation_and_cancel();
        self.buckets.clear();
        self.buckets_error.clear();
        self.buckets_loading = true;
        self.current_folder_load_all = false;
        self.nodes.clear();
        self.previews.clear();
        self.selected_preview = None;

        if let Some(b) = &self.backend {
            b.list_buckets();
        }
    }

    /// Navigate to a folder/prefix and load its first page at high priority.
    pub fn open_folder(&mut self, bucket: &str, prefix: &str) {
        self.bump_request_generation_and_cancel();
        self.clear_selection();
        self.current_bucket = bucket.to_string();
        self.current_prefix = prefix.to_string();
        self.current_folder_load_all = true;
        self.load_folder_with_priority(bucket, prefix, RequestPriority::High);
        if let Some(parent_prefix) = Self::parent_prefix(prefix) {
            self.preload_folder_page(bucket, &parent_prefix);
        }

        let is_loaded = self
            .get_node(bucket, prefix)
            .map(|n| n.is_loaded())
            .unwrap_or(false);
        if is_loaded {
            let should_continue = self
                .get_node(bucket, prefix)
                .map(|n| n.is_truncated() && !n.loading)
                .unwrap_or(false);
            if should_continue {
                self.load_more(bucket, prefix);
            }
            self.preload_first_children(bucket, prefix);
        }
    }

    /// Preload the first page of a folder/prefix at low priority.
    pub fn preload_folder_page(&mut self, bucket: &str, prefix: &str) {
        self.load_folder_with_priority(bucket, prefix, RequestPriority::Low);
    }

    pub fn load_folder(&mut self, bucket: &str, prefix: &str) {
        self.load_folder_with_priority(bucket, prefix, RequestPriority::High);
    }

    pub fn load_more(&mut self, bucket: &str, prefix: &str) {
        let key = Self::make_node_key(bucket, prefix);
        let cont_token = {
            let node = match self.nodes.get(&key) {
                Some(n) => n,
                None => return,
            };
            if !node.is_truncated() || node.loading {
                return;
            }
            node.next_continuation_token.clone()
        };

        if let Some(node) = self.nodes.get_mut(&key) {
            node.loading = true;
        }

        self.start_folder_request(bucket, prefix, &cont_token, RequestPriority::High);
    }

    pub fn navigate_to(&mut self, s3_path: &str) {
        let (bucket, prefix) = match Self::parse_s3_path(s3_path) {
            Some(bp) => bp,
            None => return,
        };

        if bucket.is_empty() {
            self.bump_request_generation_and_cancel();
            self.clear_selection();
            self.current_bucket.clear();
            self.current_prefix.clear();
            self.current_folder_load_all = false;
            return;
        }

        // Add bucket if not in list
        self.add_manual_bucket(&bucket);
        self.open_folder(&bucket, &prefix);
    }

    pub fn navigate_up(&mut self) {
        if self.current_bucket.is_empty() {
            return;
        }

        if self.current_prefix.is_empty() {
            self.bump_request_generation_and_cancel();
            self.clear_selection();
            self.current_bucket.clear();
            self.current_prefix.clear();
            self.current_folder_load_all = false;
            return;
        }

        let new_prefix = Self::parent_prefix(&self.current_prefix).unwrap_or_default();

        let bucket = self.current_bucket.clone();
        self.open_folder(&bucket, &new_prefix);
    }

    pub fn navigate_into(&mut self, bucket: &str, prefix: &str) {
        self.open_folder(bucket, prefix);
    }

    pub fn add_manual_bucket(&mut self, bucket_name: &str) {
        if self.buckets.iter().any(|b| b.name == bucket_name) {
            return;
        }
        self.buckets.push(S3Bucket {
            name: bucket_name.to_string(),
            creation_date: "(manually added)".to_string(),
        });
    }

    pub fn select_file(&mut self, bucket: &str, key: &str) {
        let cache_key = Self::make_preview_cache_key(bucket, key);
        let previous_selection = self.selected_preview.clone();

        if let Some(previous_key) = previous_selection {
            if previous_key != cache_key {
                if let Some(previous_node) = self.previews.get_mut(&previous_key) {
                    previous_node.preview.cancel_requests();
                    previous_node.request_started = false;
                    previous_node.promote_to_full_on_ready = false;
                }
            }
        }

        if let Some(b) = &self.backend {
            b.cancel_streaming_requests_except(bucket, key);
        }

        self.selected_preview = Some(cache_key.clone());

        // Already in cache? Touch and promote if needed.
        if let Some(node) = self.previews.get_mut(&cache_key) {
            let mut request: Option<(
                Arc<StreamingFilePreview>,
                u64,
                Option<u64>,
                RequestPriority,
            )> = None;
            let mut restart_full = false;
            node.touch();
            if !node.unsupported {
                let status = node.preview.status();
                if status == StreamingStatus::PrefetchReady {
                    let range_start = node.preview.source_bytes();
                    request = Some((
                        Arc::clone(&node.preview),
                        range_start,
                        None,
                        RequestPriority::High,
                    ));
                } else if status == StreamingStatus::Prefetching {
                    if node.preview.source_bytes() == 0 {
                        restart_full = true;
                    } else {
                        node.promote_to_full_on_ready = true;
                    }
                }
            }

            if restart_full {
                if let Some(preview) = self.replace_preview_for_full_download(bucket, key) {
                    request = Some((preview, 0, None, RequestPriority::High));
                }
            }

            if let Some((preview, range_start, max_bytes, priority)) = request {
                self.start_streaming_download(
                    bucket,
                    key,
                    preview,
                    range_start,
                    max_bytes,
                    priority,
                );
            }
        } else {
            // Not cached: create preview and request full file immediately.
            if !Self::is_preview_supported(key) {
                let node = PreviewNode::new_unsupported(bucket.to_string(), key.to_string());
                self.previews.insert(cache_key.clone(), node);
                self.evict_old_previews();
                return;
            }

            let compression = Compression::from_filename(key);
            let preview = match StreamingFilePreview::new(compression) {
                Ok(p) => Arc::new(p),
                Err(e) => {
                    let preview = Arc::new(
                        StreamingFilePreview::new(Compression::None)
                            .expect("Failed to create fallback preview"),
                    );
                    preview.set_status(StreamingStatus::Error(e));
                    let node = PreviewNode::new(bucket.to_string(), key.to_string(), preview);
                    self.previews.insert(cache_key.clone(), node);
                    self.evict_old_previews();
                    return;
                }
            };

            let node = PreviewNode::new(bucket.to_string(), key.to_string(), Arc::clone(&preview));
            self.previews.insert(cache_key.clone(), node);
            self.evict_old_previews();

            self.start_streaming_download(bucket, key, preview, 0, None, RequestPriority::High);
        }
    }

    /// Preload first 64KB of a file preview at low priority.
    pub fn preload_file_head(&mut self, bucket: &str, key: &str) {
        if !Self::is_preview_supported(key) {
            return;
        }

        let cache_key = Self::make_preview_cache_key(bucket, key);
        let (preview, status, request_started) = match self.previews.get_mut(&cache_key) {
            Some(node) => {
                node.touch();
                (
                    Arc::clone(&node.preview),
                    node.preview.status(),
                    node.request_started,
                )
            }
            None => {
                let compression = Compression::from_filename(key);
                let preview = match StreamingFilePreview::new(compression) {
                    Ok(p) => Arc::new(p),
                    Err(_) => return,
                };
                let node =
                    PreviewNode::new(bucket.to_string(), key.to_string(), Arc::clone(&preview));
                self.previews.insert(cache_key.clone(), node);
                self.evict_old_previews();
                (preview, StreamingStatus::Prefetching, false)
            }
        };

        if request_started {
            return;
        }

        match status {
            StreamingStatus::Complete
            | StreamingStatus::Downloading
            | StreamingStatus::PrefetchReady => {}
            _ => {
                self.start_streaming_download(
                    bucket,
                    key,
                    preview,
                    0,
                    Some(PREFETCH_BYTES),
                    RequestPriority::Low,
                );
            }
        }
    }

    /// Continue downloading the currently selected preview (full file)
    pub fn continue_download(&mut self) {
        let (bucket, key, preview, status) = {
            let node = match self.selected_preview() {
                Some(n) => n,
                None => return,
            };

            (
                node.bucket.clone(),
                node.key.clone(),
                Arc::clone(&node.preview),
                node.preview.status(),
            )
        };

        match status {
            StreamingStatus::PrefetchReady => {
                let source_bytes = preview.source_bytes();
                self.start_streaming_download(
                    &bucket,
                    &key,
                    preview,
                    source_bytes,
                    None,
                    RequestPriority::High,
                );
            }
            StreamingStatus::Prefetching => {
                let mut restart_full = false;
                if let Some(node) = self
                    .previews
                    .get_mut(&Self::make_preview_cache_key(&bucket, &key))
                {
                    if node.preview.source_bytes() == 0 {
                        restart_full = true;
                    } else {
                        node.promote_to_full_on_ready = true;
                    }
                }

                if restart_full {
                    if let Some(preview) = self.replace_preview_for_full_download(&bucket, &key) {
                        self.start_streaming_download(
                            &bucket,
                            &key,
                            preview,
                            0,
                            None,
                            RequestPriority::High,
                        );
                    }
                }
            }
            _ => {}
        }
    }

    pub fn clear_selection(&mut self) {
        self.selected_preview = None;
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

    pub fn select_profile(&mut self, index: usize) {
        if index >= self.profiles.len() || index == self.selected_profile_idx {
            return;
        }
        self.bump_request_generation_and_cancel();
        self.selected_profile_idx = index;
        self.buckets.clear();
        self.buckets_error.clear();
        self.nodes.clear();
        self.previews.clear();
        self.selected_preview = None;
        self.current_bucket.clear();
        self.current_prefix.clear();
        self.current_folder_load_all = false;
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

    fn replace_preview_for_full_download(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Option<Arc<StreamingFilePreview>> {
        if !Self::is_preview_supported(key) {
            return None;
        }

        let compression = Compression::from_filename(key);
        let preview = StreamingFilePreview::new(compression).ok().map(Arc::new)?;
        let cache_key = Self::make_preview_cache_key(bucket, key);

        if let Some(node) = self.previews.get_mut(&cache_key) {
            node.preview = Arc::clone(&preview);
            node.request_started = false;
            node.promote_to_full_on_ready = false;
            node.unsupported = false;
            node.touch();
            return Some(preview);
        }

        let node = PreviewNode::new(bucket.to_string(), key.to_string(), Arc::clone(&preview));
        self.previews.insert(cache_key, node);
        self.evict_old_previews();
        Some(preview)
    }

    fn load_folder_with_priority(&mut self, bucket: &str, prefix: &str, priority: RequestPriority) {
        let key = Self::make_node_key(bucket, prefix);
        if let Some(node) = self.nodes.get(&key) {
            if node.is_loaded() {
                return;
            }
            if node.loading {
                if priority == RequestPriority::High {
                    self.start_folder_request(bucket, prefix, "", RequestPriority::High);
                }
                return;
            }
        }

        let node = self.get_or_create_node(bucket, prefix);
        node.objects.clear();
        node.sorted_view.clear();
        node.folder_count = 0;
        node.cached_objects_size = 0;
        node.next_continuation_token.clear();
        node.status = DataStatus::Empty;
        node.loading = true;

        self.start_folder_request(bucket, prefix, "", priority);
    }

    fn start_folder_request(
        &mut self,
        bucket: &str,
        prefix: &str,
        continuation_token: &str,
        priority: RequestPriority,
    ) {
        if !self.register_folder_request(bucket, prefix, continuation_token, priority) {
            return;
        }

        if let Some(b) = &self.backend {
            b.list_objects_with_priority(bucket, prefix, continuation_token, priority);
        }
    }

    fn start_streaming_download(
        &mut self,
        bucket: &str,
        key: &str,
        preview: Arc<StreamingFilePreview>,
        range_start: u64,
        max_bytes: Option<u64>,
        priority: RequestPriority,
    ) {
        preview.clear_cancel_request();

        let cache_key = Self::make_preview_cache_key(bucket, key);
        if let Some(node) = self.previews.get_mut(&cache_key) {
            node.request_started = true;
            node.promote_to_full_on_ready = false;
            node.touch();
        }

        if max_bytes.is_none() {
            preview.set_downloading();
        } else {
            preview.set_status(StreamingStatus::Prefetching);
        }

        if let Some(b) = &self.backend {
            b.streaming_get_object_with_priority(
                bucket,
                key,
                preview,
                range_start,
                max_bytes,
                priority,
            );
        }
    }

    fn bump_request_generation_and_cancel(&mut self) {
        self.request_generation = self.request_generation.wrapping_add(1);
        self.folder_requests.clear();
        for node in self.nodes.values_mut() {
            node.loading = false;
        }
        for node in self.previews.values_mut() {
            node.preview.cancel_requests();
            node.request_started = false;
            node.promote_to_full_on_ready = false;
        }
        if let Some(b) = &self.backend {
            b.cancel_all();
        }
    }

    fn register_folder_request(
        &mut self,
        bucket: &str,
        prefix: &str,
        continuation_token: &str,
        priority: RequestPriority,
    ) -> bool {
        let key = Self::make_folder_request_key(bucket, prefix, continuation_token);
        match self.folder_requests.get_mut(&key) {
            Some((generation, existing_priority)) if *generation == self.request_generation => {
                if *existing_priority == RequestPriority::Low && priority == RequestPriority::High {
                    *existing_priority = RequestPriority::High;
                    true
                } else {
                    false
                }
            }
            _ => {
                self.folder_requests
                    .insert(key, (self.request_generation, priority));
                true
            }
        }
    }

    fn complete_folder_request_if_current(
        &mut self,
        bucket: &str,
        prefix: &str,
        continuation_token: &str,
    ) -> bool {
        let key = Self::make_folder_request_key(bucket, prefix, continuation_token);
        match self.folder_requests.remove(&key) {
            Some((generation, _)) => generation == self.request_generation,
            None => false,
        }
    }

    fn preload_first_children(&mut self, bucket: &str, prefix: &str) {
        let preload_count = self.auto_preload_count;
        let entries: Vec<(bool, String)> = {
            let node = match self.get_node_mut(bucket, prefix) {
                Some(n) => n,
                None => return,
            };
            node.rebuild_sorted_view_if_needed();
            node.sorted_view
                .iter()
                .take(preload_count)
                .map(|idx| {
                    let obj = &node.objects[*idx];
                    (obj.is_folder, obj.key.clone())
                })
                .collect()
        };

        for (is_folder, key) in entries {
            if is_folder {
                self.preload_folder_page(bucket, &key);
            } else {
                self.preload_file_head(bucket, &key);
            }
        }
    }

    fn make_node_key(bucket: &str, prefix: &str) -> String {
        format!("{}/{}", bucket, prefix)
    }

    fn make_preview_cache_key(bucket: &str, key: &str) -> String {
        format!("{}/{}", bucket, key)
    }

    fn make_folder_request_key(bucket: &str, prefix: &str, continuation_token: &str) -> String {
        format!("{}/{}/{}", bucket, prefix, continuation_token)
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
            None => return false,
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
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum BackendCall {
        ListBuckets,
        ListObjects {
            bucket: String,
            prefix: String,
            continuation_token: String,
            priority: RequestPriority,
        },
        StreamingGetObject {
            bucket: String,
            key: String,
            range_start: u64,
            max_bytes: Option<u64>,
            priority: RequestPriority,
        },
        CancelAll,
        CancelStreamingRequestsExcept {
            bucket: String,
            key: String,
        },
    }

    #[derive(Default)]
    struct MockBackend {
        events: Mutex<Vec<StateEvent>>,
        calls: Mutex<Vec<BackendCall>>,
    }

    impl MockBackend {
        fn push_event(&self, event: StateEvent) {
            self.events.lock().unwrap().push(event);
        }

        fn calls(&self) -> Vec<BackendCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    struct MockBackendHandle {
        inner: Arc<MockBackend>,
    }

    impl Backend for MockBackendHandle {
        fn take_events(&self) -> Vec<StateEvent> {
            self.events_lock().drain(..).collect()
        }

        fn list_buckets(&self) {
            self.calls_lock().push(BackendCall::ListBuckets);
        }

        fn list_objects_with_priority(
            &self,
            bucket: &str,
            prefix: &str,
            continuation_token: &str,
            priority: RequestPriority,
        ) {
            self.calls_lock().push(BackendCall::ListObjects {
                bucket: bucket.to_string(),
                prefix: prefix.to_string(),
                continuation_token: continuation_token.to_string(),
                priority,
            });
        }

        fn get_object(&self, _bucket: &str, _key: &str, _max_bytes: usize) {}

        fn streaming_get_object_with_priority(
            &self,
            bucket: &str,
            key: &str,
            _preview: Arc<StreamingFilePreview>,
            range_start: u64,
            max_bytes: Option<u64>,
            priority: RequestPriority,
        ) {
            self.calls_lock().push(BackendCall::StreamingGetObject {
                bucket: bucket.to_string(),
                key: key.to_string(),
                range_start,
                max_bytes,
                priority,
            });
        }

        fn cancel_all(&self) {
            self.calls_lock().push(BackendCall::CancelAll);
        }

        fn cancel_streaming_requests_except(&self, bucket: &str, key: &str) {
            self.calls_lock()
                .push(BackendCall::CancelStreamingRequestsExcept {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
        }

        fn debug_snapshot(&self) -> BackendDebugSnapshot {
            BackendDebugSnapshot::default()
        }
    }

    impl MockBackendHandle {
        fn events_lock(&self) -> std::sync::MutexGuard<'_, Vec<StateEvent>> {
            self.inner.events.lock().unwrap()
        }

        fn calls_lock(&self) -> std::sync::MutexGuard<'_, Vec<BackendCall>> {
            self.inner.calls.lock().unwrap()
        }
    }

    fn setup_model() -> (BrowserModel, Arc<MockBackend>) {
        let backend = Arc::new(MockBackend::default());
        let mut model = BrowserModel::new();
        model.set_backend(Box::new(MockBackendHandle {
            inner: Arc::clone(&backend),
        }));
        (model, backend)
    }

    fn folder_obj(key: &str) -> S3Object {
        S3Object {
            key: key.to_string(),
            display_name: key.to_string(),
            size: 0,
            last_modified: String::new(),
            is_folder: true,
        }
    }

    fn file_obj(key: &str, size: i64) -> S3Object {
        S3Object {
            key: key.to_string(),
            display_name: key.to_string(),
            size,
            last_modified: String::new(),
            is_folder: false,
        }
    }

    #[test]
    fn dedupes_preload_file_head_requests() {
        let (mut model, backend) = setup_model();

        model.preload_file_head("bucket-a", "foo.txt");
        model.preload_file_head("bucket-a", "foo.txt");

        let calls = backend.calls();
        let streaming_calls: Vec<_> = calls
            .into_iter()
            .filter(|c| {
                matches!(
                    c,
                    BackendCall::StreamingGetObject {
                        bucket,
                        key,
                        range_start: 0,
                        max_bytes: Some(PREFETCH_BYTES),
                        priority: RequestPriority::Low,
                    } if bucket == "bucket-a" && key == "foo.txt"
                )
            })
            .collect();
        assert_eq!(streaming_calls.len(), 1);
    }

    #[test]
    fn click_promotes_prefetch_to_full_download() {
        let (mut model, backend) = setup_model();

        model.preload_file_head("bucket-a", "foo.txt");
        let cache_key = BrowserModel::make_preview_cache_key("bucket-a", "foo.txt");
        model
            .previews
            .get(&cache_key)
            .unwrap()
            .preview
            .append_chunk(b"hello")
            .unwrap();
        model.select_file("bucket-a", "foo.txt");

        backend.push_event(StateEvent::PreviewProgress {
            bucket: "bucket-a".to_string(),
            key: "foo.txt".to_string(),
            decompressed_bytes: 5,
            source_bytes: 5,
            line_count: 1,
            status: StreamingStatus::PrefetchReady,
        });

        assert!(model.process_events());

        let calls = backend.calls();
        assert!(calls.iter().any(|c| {
            matches!(
                c,
                BackendCall::StreamingGetObject {
                    bucket,
                    key,
                    range_start: 5,
                    max_bytes: None,
                    priority: RequestPriority::High,
                } if bucket == "bucket-a" && key == "foo.txt"
            )
        }));
    }

    #[test]
    fn click_on_prefetching_file_restarts_as_full_download() {
        let (mut model, backend) = setup_model();

        model.preload_file_head("bucket-a", "foo.txt");
        model.select_file("bucket-a", "foo.txt");

        let calls = backend.calls();
        assert!(calls.iter().any(|c| {
            matches!(
                c,
                BackendCall::StreamingGetObject {
                    bucket,
                    key,
                    range_start: 0,
                    max_bytes: None,
                    priority: RequestPriority::High,
                } if bucket == "bucket-a" && key == "foo.txt"
            )
        }));

        let high_prefetch_calls: Vec<_> = calls
            .iter()
            .filter(|c| {
                matches!(
                    c,
                    BackendCall::StreamingGetObject {
                        bucket,
                        key,
                        range_start: 0,
                        max_bytes: Some(PREFETCH_BYTES),
                        priority: RequestPriority::High,
                    } if bucket == "bucket-a" && key == "foo.txt"
                )
            })
            .collect();
        assert!(high_prefetch_calls.is_empty());
    }

    #[test]
    fn ignores_stale_folder_results_after_navigation() {
        let (mut model, backend) = setup_model();

        model.open_folder("bucket-a", "old/");
        model.open_folder("bucket-a", "new/");

        backend.push_event(StateEvent::ObjectsLoaded {
            bucket: "bucket-a".to_string(),
            prefix: "old/".to_string(),
            continuation_token: String::new(),
            objects: vec![file_obj("old/stale.txt", 10)],
            next_continuation_token: String::new(),
            is_truncated: false,
        });
        model.process_events();
        let stale_node = model.get_node("bucket-a", "old/").unwrap();
        assert!(stale_node.objects.is_empty());

        backend.push_event(StateEvent::ObjectsLoaded {
            bucket: "bucket-a".to_string(),
            prefix: "new/".to_string(),
            continuation_token: String::new(),
            objects: vec![file_obj("new/live.txt", 20)],
            next_continuation_token: String::new(),
            is_truncated: false,
        });
        model.process_events();
        assert!(model.get_node("bucket-a", "new/").is_some());
    }

    #[test]
    fn open_folder_preloads_parent_prefix() {
        let (mut model, backend) = setup_model();
        model.open_folder("bucket-a", "root/");

        let calls = backend.calls();
        assert!(calls.iter().any(|c| {
            matches!(
                c,
                BackendCall::ListObjects {
                    bucket,
                    prefix,
                    continuation_token,
                    priority: RequestPriority::Low,
                } if bucket == "bucket-a" && prefix.is_empty() && continuation_token.is_empty()
            )
        }));
    }

    #[test]
    fn selecting_new_file_cancels_previous_streaming_request() {
        let (mut model, backend) = setup_model();
        model.select_file("bucket-a", "first.txt");

        let first_cache_key = BrowserModel::make_preview_cache_key("bucket-a", "first.txt");
        let second_cache_key = BrowserModel::make_preview_cache_key("bucket-a", "second.txt");

        model.select_file("bucket-a", "second.txt");

        let first_node = model.previews.get(&first_cache_key).unwrap();
        assert!(first_node.preview.is_cancel_requested());
        assert!(!first_node.request_started);
        assert!(!first_node.promote_to_full_on_ready);

        let second_node = model.previews.get(&second_cache_key).unwrap();
        assert!(!second_node.preview.is_cancel_requested());

        let calls = backend.calls();
        assert!(calls.iter().any(|c| {
            matches!(
                c,
                BackendCall::CancelStreamingRequestsExcept { bucket, key }
                if bucket == "bucket-a" && key == "second.txt"
            )
        }));
    }

    #[test]
    fn navigation_cancels_active_preview_requests() {
        let (mut model, _backend) = setup_model();
        model.select_file("bucket-a", "foo.txt");

        let cache_key = BrowserModel::make_preview_cache_key("bucket-a", "foo.txt");
        assert!(
            !model
                .previews
                .get(&cache_key)
                .unwrap()
                .preview
                .is_cancel_requested()
        );

        model.open_folder("bucket-a", "root/");
        let node = model.previews.get(&cache_key).unwrap();
        assert!(node.preview.is_cancel_requested());
        assert!(!node.request_started);
    }

    #[test]
    fn auto_preloads_first_children_for_open_folder() {
        let (mut model, backend) = setup_model();
        model.set_auto_preload_count(3);
        model.open_folder("bucket-a", "root/");

        backend.push_event(StateEvent::ObjectsLoaded {
            bucket: "bucket-a".to_string(),
            prefix: "root/".to_string(),
            continuation_token: String::new(),
            objects: vec![
                folder_obj("root/child1/"),
                file_obj("root/file1.txt", 123),
                folder_obj("root/child2/"),
            ],
            next_continuation_token: String::new(),
            is_truncated: false,
        });

        model.process_events();

        let calls = backend.calls();
        assert!(calls.iter().any(|c| {
            matches!(
                c,
                BackendCall::ListObjects {
                    bucket,
                    prefix,
                    continuation_token,
                    priority: RequestPriority::Low,
                } if bucket == "bucket-a" && prefix == "root/child1/" && continuation_token.is_empty()
            )
        }));
        assert!(calls.iter().any(|c| {
            matches!(
                c,
                BackendCall::ListObjects {
                    bucket,
                    prefix,
                    continuation_token,
                    priority: RequestPriority::Low,
                } if bucket == "bucket-a" && prefix == "root/child2/" && continuation_token.is_empty()
            )
        }));
        assert!(calls.iter().any(|c| {
            matches!(
                c,
                BackendCall::StreamingGetObject {
                    bucket,
                    key,
                    range_start: 0,
                    max_bytes: Some(PREFETCH_BYTES),
                    priority: RequestPriority::Low,
                } if bucket == "bucket-a" && key == "root/file1.txt"
            )
        }));
    }

    #[test]
    fn clicked_folder_auto_loads_all_pages() {
        let (mut model, backend) = setup_model();
        model.open_folder("bucket-a", "root/");

        backend.push_event(StateEvent::ObjectsLoaded {
            bucket: "bucket-a".to_string(),
            prefix: "root/".to_string(),
            continuation_token: String::new(),
            objects: vec![file_obj("root/file1.txt", 10)],
            next_continuation_token: "next-token".to_string(),
            is_truncated: true,
        });

        model.process_events();

        let calls = backend.calls();
        assert!(calls.iter().any(|c| {
            matches!(
                c,
                BackendCall::ListObjects {
                    bucket,
                    prefix,
                    continuation_token,
                    priority: RequestPriority::High,
                } if bucket == "bucket-a" && prefix == "root/" && continuation_token == "next-token"
            )
        }));
    }
}
