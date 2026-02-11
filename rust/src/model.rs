use crate::backend::Backend;
use crate::events::{S3Bucket, S3Object, StateEvent};
use crate::preview::{Compression, StreamingFilePreview, StreamingStatus};
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
}

impl PreviewNode {
    fn new(bucket: String, key: String, preview: Arc<StreamingFilePreview>) -> Self {
        Self {
            bucket,
            key,
            preview,
            last_accessed: Instant::now(),
            unsupported: false,
        }
    }

    fn new_unsupported(bucket: String, key: String) -> Self {
        // Create a dummy preview for unsupported files
        let preview = Arc::new(
            StreamingFilePreview::new(Compression::None)
                .expect("Failed to create dummy preview"),
        );
        Self {
            bucket,
            key,
            preview,
            last_accessed: Instant::now(),
            unsupported: true,
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

    // Preview cache with LRU eviction
    previews: HashMap<String, PreviewNode>,
    pub selected_preview: Option<String>, // key into previews hashmap
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
            previews: HashMap::new(),
            selected_preview: None,
        }
    }

    pub fn set_backend(&mut self, backend: Box<dyn Backend>) {
        self.backend = Some(backend);
    }

    pub fn is_at_root(&self) -> bool {
        self.current_bucket.is_empty()
    }

    pub fn has_selection(&self) -> bool {
        self.selected_preview.is_some()
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
                    let node = self.get_or_create_node(&bucket, &prefix);

                    if continuation_token.is_empty() {
                        node.objects = objects;
                    } else {
                        // Append, deduplicating
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

                    // Auto-continue pagination for current folder
                    if bucket == self.current_bucket && prefix == self.current_prefix {
                        let should_load_more = {
                            let node = self.nodes.get(&Self::make_node_key(&bucket, &prefix));
                            node.map(|n| n.is_truncated()).unwrap_or(false)
                        };
                        if should_load_more {
                            self.load_more(&bucket, &prefix);
                        }
                    }
                }
                StateEvent::ObjectsError {
                    bucket,
                    prefix,
                    error,
                } => {
                    let node = self.get_or_create_node(&bucket, &prefix);
                    node.loading = false;
                    // Only set error status if we don't already have data
                    if !node.is_loaded() {
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
                StateEvent::ObjectContentError {
                    bucket,
                    key,
                    error,
                } => {
                    // Legacy event - no longer used for streaming previews
                    let _ = (bucket, key, error);
                }
                StateEvent::PreviewProgress {
                    bucket,
                    key,
                    decompressed_bytes: _,
                    source_bytes: _,
                    line_count: _,
                    status: _,
                } => {
                    // Preview progress is stored in the Arc<StreamingFilePreview>
                    // Just touch the node for LRU tracking
                    let cache_key = Self::make_preview_cache_key(&bucket, &key);
                    if let Some(node) = self.previews.get_mut(&cache_key) {
                        node.touch();
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
                    }
                }
            }
        }
        true
    }

    pub fn refresh(&mut self) {
        self.buckets.clear();
        self.buckets_error.clear();
        self.buckets_loading = true;
        self.nodes.clear();
        self.previews.clear();
        self.selected_preview = None;

        if let Some(b) = &self.backend {
            b.list_buckets();
        }
    }

    pub fn load_folder(&mut self, bucket: &str, prefix: &str) {
        let key = Self::make_node_key(bucket, prefix);
        if let Some(node) = self.nodes.get(&key) {
            if node.is_loaded() {
                return;
            }
        }

        let node = self.get_or_create_node(bucket, prefix);
        node.objects.clear();
        node.status = DataStatus::Empty;
        node.loading = true;

        if let Some(b) = &self.backend {
            b.list_objects(bucket, prefix, "");
        }
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

        if let Some(b) = &self.backend {
            b.list_objects(bucket, prefix, &cont_token);
        }
    }

    pub fn navigate_to(&mut self, s3_path: &str) {
        let (bucket, prefix) = match Self::parse_s3_path(s3_path) {
            Some(bp) => bp,
            None => return,
        };

        if bucket.is_empty() {
            self.clear_selection();
            self.current_bucket.clear();
            self.current_prefix.clear();
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
            self.clear_selection();
            self.current_bucket.clear();
            self.current_prefix.clear();
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
        self.clear_selection();
        self.current_bucket = bucket.to_string();
        self.current_prefix = prefix.to_string();
        self.load_folder(bucket, prefix);

        // Resume pagination if folder was already loaded but truncated
        let key = Self::make_node_key(bucket, prefix);
        let should_load_more = self
            .nodes
            .get(&key)
            .map(|n| n.is_loaded() && n.is_truncated() && !n.loading)
            .unwrap_or(false);
        if should_load_more {
            self.load_more(bucket, prefix);
        }
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

        // Already selected?
        if self.selected_preview.as_ref() == Some(&cache_key) {
            // Touch for LRU
            if let Some(node) = self.previews.get_mut(&cache_key) {
                node.touch();
            }
            return;
        }

        self.selected_preview = Some(cache_key.clone());

        // Check if already in cache
        if let Some(node) = self.previews.get_mut(&cache_key) {
            node.touch();
            return;
        }

        // Check if file type is supported
        if !Self::is_preview_supported(key) {
            let node = PreviewNode::new_unsupported(bucket.to_string(), key.to_string());
            self.previews.insert(cache_key.clone(), node);
            self.evict_old_previews();
            return;
        }

        // Detect compression from filename
        let compression = Compression::from_filename(key);

        // Create streaming preview with temp file
        let preview = match StreamingFilePreview::new(compression) {
            Ok(p) => Arc::new(p),
            Err(e) => {
                // Create a failed preview node
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

        // Create node
        let node = PreviewNode::new(bucket.to_string(), key.to_string(), Arc::clone(&preview));
        self.previews.insert(cache_key.clone(), node);

        // Evict old entries if cache is too large
        self.evict_old_previews();

        // Request full file download
        if let Some(b) = &self.backend {
            b.streaming_get_object(bucket, key, preview, 0, None);
        }
    }

    /// Continue downloading the currently selected preview (full file)
    pub fn continue_download(&mut self) {
        let (bucket, key, preview) = {
            let node = match self.selected_preview() {
                Some(n) => n,
                None => return,
            };

            if !node.can_continue_download() {
                return;
            }

            (
                node.bucket.clone(),
                node.key.clone(),
                Arc::clone(&node.preview),
            )
        };

        let source_bytes = preview.source_bytes();

        if let Some(b) = &self.backend {
            b.streaming_get_object(&bucket, &key, preview, source_bytes, None);
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
        self.selected_profile_idx = index;
        self.buckets.clear();
        self.buckets_error.clear();
        self.nodes.clear();
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
