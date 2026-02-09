use crate::aws::credentials::{AWSProfile, load_aws_profiles};
use crate::backend::{Backend, CancelFlag};
use crate::events::{S3Bucket, S3Object, StateEvent};
use crate::settings::{AppSettings, PathEntry};
use crate::streaming_preview::{
    GzipTransform, StreamTransform, StreamingFilePreview, ZstdTransform,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Node representing a folder's contents.
pub struct FolderNode {
    pub bucket: String,
    pub prefix: String,
    pub objects: Vec<S3Object>,
    pub next_continuation_token: String,
    pub is_truncated: bool,
    pub loading: bool,
    pub loaded: bool,
    pub error: String,

    // Cached sorted view: indices into objects[]
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
            is_truncated: false,
            loading: false,
            loaded: false,
            error: String::new(),
            sorted_view: Vec::new(),
            folder_count: 0,
            cached_objects_size: 0,
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

const PREVIEW_MAX_BYTES: usize = 64 * 1024;

/// The browser model - owns state and processes commands.
pub struct BrowserModel {
    backend: Option<Box<dyn Backend>>,
    pub settings: AppSettings,

    // Profiles
    profiles: Vec<AWSProfile>,
    selected_profile_idx: i32,

    // Buckets
    buckets: Vec<S3Bucket>,
    buckets_loading: bool,
    buckets_error: String,

    // Folder nodes
    nodes: HashMap<String, FolderNode>,

    // Current navigation path
    current_bucket: String,
    current_prefix: String,

    // File selection and preview
    selected_bucket: String,
    selected_key: String,
    selected_file_size: i64,
    preview_loading: bool,
    preview_supported: bool,
    preview_content: Vec<u8>,
    preview_error: String,

    // Streaming preview
    streaming_preview: Option<Arc<StreamingFilePreview>>,
    streaming_cancel_flag: Option<CancelFlag>,
    streaming_enabled: bool,

    // Preview cache
    preview_cache: HashMap<String, Vec<u8>>,
    pending_object_requests: HashSet<String>,
    last_hovered_file: String,
    last_hovered_folder: String,

    // Pagination cancel
    pagination_cancel_flag: Option<CancelFlag>,
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
            nodes: HashMap::new(),
            current_bucket: String::new(),
            current_prefix: String::new(),
            selected_bucket: String::new(),
            selected_key: String::new(),
            selected_file_size: 0,
            preview_loading: false,
            preview_supported: false,
            preview_content: Vec::new(),
            preview_error: String::new(),
            streaming_preview: None,
            streaming_cancel_flag: None,
            streaming_enabled: false,
            preview_cache: HashMap::new(),
            pending_object_requests: HashSet::new(),
            last_hovered_file: String::new(),
            last_hovered_folder: String::new(),
            pagination_cancel_flag: None,
        }
    }

    pub fn set_backend(&mut self, backend: Box<dyn Backend>) {
        self.backend = Some(backend);
    }

    // Profile management
    pub fn load_profiles(&mut self) {
        self.profiles = load_aws_profiles();
        self.selected_profile_idx = 0;

        // Check AWS_PROFILE env
        if let Ok(env_profile) = std::env::var("AWS_PROFILE") {
            for (i, p) in self.profiles.iter().enumerate() {
                if p.name == env_profile {
                    self.selected_profile_idx = i as i32;
                    break;
                }
            }
        } else {
            // Look for "default" profile
            for (i, p) in self.profiles.iter().enumerate() {
                if p.name == "default" {
                    self.selected_profile_idx = i as i32;
                    break;
                }
            }
        }
    }

    pub fn select_profile(&mut self, index: i32) {
        if index < 0 || index >= self.profiles.len() as i32 {
            return;
        }
        if index == self.selected_profile_idx {
            return;
        }

        self.selected_profile_idx = index;
        self.buckets.clear();
        self.buckets_error.clear();
        self.nodes.clear();
        self.current_bucket.clear();
        self.current_prefix.clear();

        // The UI layer should handle calling set_profile on the backend
        self.refresh();
    }

    pub fn selected_profile_index(&self) -> i32 {
        self.selected_profile_idx
    }

    pub fn profiles(&self) -> &[AWSProfile] {
        &self.profiles
    }

    pub fn profiles_mut(&mut self) -> &mut Vec<AWSProfile> {
        &mut self.profiles
    }

    pub fn set_settings(&mut self, settings: AppSettings) {
        self.settings = settings;
    }

    pub fn record_recent_path(&mut self, path: &str) {
        if path.is_empty() || path == "s3://" {
            return;
        }

        let profile_name = if self.selected_profile_idx >= 0
            && (self.selected_profile_idx as usize) < self.profiles.len()
        {
            self.profiles[self.selected_profile_idx as usize]
                .name
                .clone()
        } else {
            return;
        };

        if profile_name.is_empty() {
            return;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let entries = self
            .settings
            .frecent_paths
            .entry(profile_name)
            .or_default();

        if let Some(entry) = entries.iter_mut().find(|e| e.path == path) {
            entry.score += 1.0;
            entry.last_accessed = now;
        } else {
            entries.push(PathEntry {
                path: path.to_string(),
                score: 1.0,
                last_accessed: now,
            });
        }

        if entries.len() > 500 {
            entries.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
            entries.truncate(500);
        }
    }

    pub fn top_frecent_paths(&self, count: usize) -> Vec<String> {
        let profile_name = if self.selected_profile_idx >= 0
            && (self.selected_profile_idx as usize) < self.profiles.len()
        {
            &self.profiles[self.selected_profile_idx as usize].name
        } else {
            return vec![];
        };

        let entries = match self.settings.frecent_paths.get(profile_name) {
            Some(e) if !e.is_empty() => e,
            _ => return vec![],
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let mut scored: Vec<(f64, usize)> = entries
            .iter()
            .enumerate()
            .map(|(i, e)| (frecency_score(e, now), i))
            .collect();

        let n = count.min(scored.len());
        scored.select_nth_unstable_by(n.saturating_sub(1), |a, b| {
            b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal)
        });

        scored[..n]
            .iter()
            .map(|(_, i)| entries[*i].path.clone())
            .collect()
    }

    // Commands
    pub fn refresh(&mut self) {
        self.buckets.clear();
        self.buckets_error.clear();
        self.buckets_loading = true;
        self.nodes.clear();
        self.preview_cache.clear();
        self.pending_object_requests.clear();
        self.last_hovered_file.clear();
        self.last_hovered_folder.clear();

        if let Some(ref flag) = self.pagination_cancel_flag {
            flag.store(true, Ordering::Relaxed);
        }
        self.pagination_cancel_flag = None;

        if let Some(ref backend) = self.backend {
            backend.list_buckets();
        }
    }

    pub fn load_folder(&mut self, bucket: &str, prefix: &str) {
        let node = self.get_or_create_node(bucket, prefix);

        if node.loaded {
            return;
        }

        if let Some(ref backend) = self.backend {
            if backend.prioritize_request(bucket, prefix) {
                let node = self.get_or_create_node(bucket, prefix);
                node.loading = true;
                return;
            }
        }

        let node = self.get_or_create_node(bucket, prefix);
        node.objects.clear();
        node.error.clear();
        node.loading = true;

        if let Some(ref backend) = self.backend {
            backend.list_objects(bucket, prefix, "", None);
        }
    }

    pub fn load_more(&mut self, bucket: &str, prefix: &str) {
        let key = make_node_key(bucket, prefix);
        let (is_truncated, loading, token) = {
            let node = match self.nodes.get(&key) {
                Some(n) => n,
                None => return,
            };
            (
                node.is_truncated,
                node.loading,
                node.next_continuation_token.clone(),
            )
        };

        if !is_truncated || loading {
            return;
        }

        if let Some(node) = self.nodes.get_mut(&key) {
            node.loading = true;
        }

        if let Some(ref backend) = self.backend {
            backend.list_objects(bucket, prefix, &token, self.pagination_cancel_flag.clone());
        }
    }

    pub fn navigate_to(&mut self, s3_path: &str) {
        let (bucket, prefix) = match parse_s3_path(s3_path) {
            Some(p) => p,
            None => return,
        };

        if bucket.is_empty() {
            self.clear_selection();
            self.set_current_path("", "");
            return;
        }

        self.add_manual_bucket(&bucket);
        self.navigate_into(&bucket, &prefix);
    }

    pub fn navigate_up(&mut self) {
        if self.current_bucket.is_empty() {
            return;
        }

        if self.current_prefix.is_empty() {
            self.clear_selection();
            self.set_current_path("", "");
            return;
        }

        let mut new_prefix = self.current_prefix.clone();
        if new_prefix.ends_with('/') {
            new_prefix.pop();
        }

        new_prefix = match new_prefix.rfind('/') {
            None => String::new(),
            Some(pos) => new_prefix[..pos + 1].to_string(),
        };

        let bucket = self.current_bucket.clone();
        self.navigate_into(&bucket, &new_prefix);
    }

    pub fn navigate_into(&mut self, bucket: &str, prefix: &str) {
        self.clear_selection();
        self.set_current_path(bucket, prefix);
        self.load_folder(bucket, prefix);

        if !bucket.is_empty() {
            let path = format!("s3://{}/{}", bucket, prefix);
            self.record_recent_path(&path);
        }

        // Check if folder already loaded for prefetch/pagination resume
        let key = make_node_key(bucket, prefix);
        if let Some(node) = self.nodes.get(&key) {
            if node.loaded {
                let objects: Vec<S3Object> = node.objects.clone();
                let is_truncated = node.is_truncated;
                let loading = node.loading;

                self.trigger_prefetch(bucket, &objects);

                if is_truncated && !loading {
                    self.load_more(bucket, prefix);
                }
            }
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

    // File selection and preview
    pub fn select_file(&mut self, bucket: &str, key: &str) {
        if self.selected_bucket == bucket && self.selected_key == key {
            return;
        }

        self.cancel_streaming_download();

        self.selected_bucket = bucket.to_string();
        self.selected_key = key.to_string();
        self.preview_content.clear();
        self.preview_error.clear();
        self.preview_supported = is_preview_supported(key);

        // Find file size
        self.selected_file_size = 0;
        let node_key = make_node_key(&self.current_bucket, &self.current_prefix);
        if let Some(node) = self.nodes.get(&node_key) {
            for obj in &node.objects {
                if !obj.is_folder && obj.key == key {
                    self.selected_file_size = obj.size;
                    break;
                }
            }
        }

        if self.preview_supported {
            let cache_key = make_preview_cache_key(bucket, key);

            // Check cache
            if let Some(content) = self.preview_cache.get(&cache_key).cloned() {
                self.preview_content = content;
                self.preview_loading = false;
                self.start_streaming_download(self.selected_file_size as usize);
                return;
            }

            // Check pending prefetch
            if let Some(ref backend) = self.backend {
                if backend.prioritize_object_request(bucket, key) {
                    self.preview_loading = true;
                    return;
                }
            }

            self.preview_loading = true;
            self.pending_object_requests.insert(cache_key);
            if let Some(ref backend) = self.backend {
                backend.get_object(bucket, key, PREVIEW_MAX_BYTES, false, false);
            }
        } else {
            self.preview_loading = false;
        }
    }

    pub fn prefetch_file_preview(&mut self, bucket: &str, key: &str) {
        if self.backend.is_none() {
            return;
        }
        if !is_preview_supported(key) {
            return;
        }

        let cache_key = make_preview_cache_key(bucket, key);
        if self.preview_cache.contains_key(&cache_key) {
            return;
        }
        if self.selected_bucket == bucket && self.selected_key == key {
            return;
        }
        if self.last_hovered_file == cache_key {
            return;
        }

        self.last_hovered_file = cache_key;
        if let Some(ref backend) = self.backend {
            backend.get_object(bucket, key, PREVIEW_MAX_BYTES, true, true);
        }
    }

    pub fn prefetch_folder(&mut self, bucket: &str, prefix: &str) {
        if self.backend.is_none() {
            return;
        }

        let key = make_node_key(bucket, prefix);
        if let Some(node) = self.nodes.get(&key) {
            if node.loaded || node.loading {
                return;
            }
        }

        let folder_key = format!("{}/{}", bucket, prefix);
        if self.last_hovered_folder == folder_key {
            return;
        }

        // Reset old hover node
        if !self.last_hovered_folder.is_empty() {
            if let Some(slash_pos) = self.last_hovered_folder.find('/') {
                let old_bucket = &self.last_hovered_folder[..slash_pos];
                let old_prefix = &self.last_hovered_folder[slash_pos + 1..];
                let old_key = make_node_key(old_bucket, old_prefix);
                if let Some(old_node) = self.nodes.get_mut(&old_key) {
                    if old_node.loading && !old_node.loaded {
                        old_node.loading = false;
                    }
                }
            }
        }

        let node = self.get_or_create_node(bucket, prefix);
        node.loading = true;

        self.last_hovered_folder = folder_key;
        if let Some(ref backend) = self.backend {
            backend.list_objects_prefetch(bucket, prefix, true);
        }
    }

    pub fn clear_selection(&mut self) {
        self.cancel_streaming_download();
        self.selected_bucket.clear();
        self.selected_key.clear();
        self.selected_file_size = 0;
        self.preview_content.clear();
        self.preview_error.clear();
        self.preview_loading = false;
        self.preview_supported = false;
    }

    // State accessors
    pub fn is_at_root(&self) -> bool {
        self.current_bucket.is_empty()
    }

    pub fn has_selection(&self) -> bool {
        !self.selected_key.is_empty()
    }

    pub fn selected_bucket(&self) -> &str {
        &self.selected_bucket
    }

    pub fn selected_key(&self) -> &str {
        &self.selected_key
    }

    pub fn preview_loading(&self) -> bool {
        self.preview_loading
    }

    pub fn preview_content(&self) -> &[u8] {
        &self.preview_content
    }

    pub fn preview_error(&self) -> &str {
        &self.preview_error
    }

    pub fn preview_supported(&self) -> bool {
        self.preview_supported
    }

    pub fn has_streaming_preview(&self) -> bool {
        self.streaming_preview.is_some()
    }

    pub fn streaming_preview(&self) -> Option<&Arc<StreamingFilePreview>> {
        self.streaming_preview.as_ref()
    }

    pub fn is_streaming_enabled(&self) -> bool {
        self.streaming_enabled
    }

    pub fn selected_file_size(&self) -> i64 {
        self.selected_file_size
    }

    pub fn buckets(&self) -> &[S3Bucket] {
        &self.buckets
    }

    pub fn buckets_loading(&self) -> bool {
        self.buckets_loading
    }

    pub fn buckets_error(&self) -> &str {
        &self.buckets_error
    }

    pub fn get_node(&self, bucket: &str, prefix: &str) -> Option<&FolderNode> {
        let key = make_node_key(bucket, prefix);
        self.nodes.get(&key)
    }

    pub fn get_node_mut(&mut self, bucket: &str, prefix: &str) -> Option<&mut FolderNode> {
        let key = make_node_key(bucket, prefix);
        self.nodes.get_mut(&key)
    }

    pub fn current_bucket(&self) -> &str {
        &self.current_bucket
    }

    pub fn current_prefix(&self) -> &str {
        &self.current_prefix
    }

    pub fn set_current_path(&mut self, bucket: &str, prefix: &str) {
        if bucket != self.current_bucket || prefix != self.current_prefix {
            if let Some(ref flag) = self.pagination_cancel_flag {
                flag.store(true, Ordering::Relaxed);
                let old_key = make_node_key(&self.current_bucket, &self.current_prefix);
                if let Some(old_node) = self.nodes.get_mut(&old_key) {
                    old_node.loading = false;
                }
            }
            self.pagination_cancel_flag = Some(Arc::new(AtomicBool::new(false)));
        }

        self.current_bucket = bucket.to_string();
        self.current_prefix = prefix.to_string();
    }

    /// Process events from backend. Returns true if any events were processed.
    pub fn process_events(&mut self) -> bool {
        let backend = match &self.backend {
            Some(b) => b,
            None => return false,
        };

        let events = backend.take_events();
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
                StateEvent::BucketsLoadError { error_message } => {
                    self.buckets_loading = false;
                    self.buckets_error = error_message;
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
                        let existing_keys: HashSet<String> =
                            node.objects.iter().map(|o| o.key.clone()).collect();
                        for obj in objects {
                            if !existing_keys.contains(&obj.key) {
                                node.objects.push(obj);
                            }
                        }
                    }

                    node.next_continuation_token = next_continuation_token;
                    node.is_truncated = is_truncated;
                    node.loading = false;
                    node.loaded = true;
                    node.error.clear();

                    if bucket == self.current_bucket && prefix == self.current_prefix {
                        if is_truncated {
                            self.load_more(&bucket, &prefix);
                        }
                        if continuation_token.is_empty() {
                            let objects_clone: Vec<S3Object> = self
                                .nodes
                                .get(&make_node_key(&bucket, &prefix))
                                .map(|n| n.objects.clone())
                                .unwrap_or_default();
                            self.trigger_prefetch(&bucket, &objects_clone);
                        }
                    }
                }
                StateEvent::ObjectsLoadError {
                    bucket,
                    prefix,
                    error_message,
                } => {
                    let node = self.get_or_create_node(&bucket, &prefix);
                    node.loading = false;
                    node.error = error_message;
                }
                StateEvent::ObjectContentLoaded {
                    bucket,
                    key,
                    content,
                } => {
                    let cache_key = make_preview_cache_key(&bucket, &key);
                    self.preview_cache.insert(cache_key.clone(), content.clone());
                    self.pending_object_requests.remove(&cache_key);

                    if bucket == self.selected_bucket && key == self.selected_key {
                        self.preview_content = content;
                        self.preview_loading = false;
                        self.preview_error.clear();

                        let should_start = self.streaming_preview.is_none()
                            || self
                                .streaming_preview
                                .as_ref()
                                .map(|sp| sp.bucket() != bucket || sp.key() != key)
                                .unwrap_or(true);

                        if should_start {
                            self.start_streaming_download(self.selected_file_size as usize);
                        }
                    }
                }
                StateEvent::ObjectContentLoadError {
                    bucket,
                    key,
                    error_message,
                } => {
                    let cache_key = make_preview_cache_key(&bucket, &key);
                    self.pending_object_requests.remove(&cache_key);

                    if bucket == self.selected_bucket && key == self.selected_key {
                        self.preview_loading = false;
                        self.preview_error = error_message;
                    }
                }
                StateEvent::ObjectRangeLoaded {
                    bucket,
                    key,
                    start_byte,
                    data,
                    ..
                } => {
                    if let Some(ref sp) = self.streaming_preview {
                        if sp.bucket() == bucket && sp.key() == key {
                            sp.append_chunk(&data, start_byte);
                        }
                    }
                }
                StateEvent::ObjectRangeLoadError {
                    bucket,
                    key,
                    start_byte,
                    error_message,
                } => {
                    if let Some(ref sp) = self.streaming_preview {
                        if sp.bucket() == bucket && sp.key() == key {
                            eprintln!(
                                "Streaming error at offset {}: {}",
                                start_byte, error_message
                            );
                        }
                    }
                }
            }
        }
        true
    }

    // Private helpers
    fn get_or_create_node(&mut self, bucket: &str, prefix: &str) -> &mut FolderNode {
        let key = make_node_key(bucket, prefix);
        self.nodes
            .entry(key)
            .or_insert_with(|| FolderNode::new(bucket.to_string(), prefix.to_string()))
    }

    fn trigger_prefetch(&self, bucket: &str, objects: &[S3Object]) {
        let backend = match &self.backend {
            Some(b) => b,
            None => return,
        };

        const MAX_PREFETCH: usize = 20;
        let mut count = 0;

        for obj in objects {
            if !obj.is_folder {
                continue;
            }
            if count >= MAX_PREFETCH {
                break;
            }

            let key = make_node_key(bucket, &obj.key);
            if let Some(node) = self.nodes.get(&key) {
                if node.loaded || node.loading {
                    continue;
                }
            }

            if backend.has_pending_request(bucket, &obj.key) {
                continue;
            }

            backend.list_objects_prefetch(bucket, &obj.key, false);
            count += 1;
        }
    }

    fn start_streaming_download(&mut self, total_file_size: usize) {
        if self.backend.is_none() || self.selected_bucket.is_empty() || self.selected_key.is_empty()
        {
            return;
        }

        self.cancel_streaming_download();

        // Detect compression
        let transform: Option<Box<dyn StreamTransform>> = {
            let ext = self
                .selected_key
                .rsplit('.')
                .next()
                .unwrap_or("")
                .to_lowercase();
            match ext.as_str() {
                "gz" => Some(Box::new(GzipTransform::new())),
                "zst" | "zstd" => Some(Box::new(ZstdTransform::new())),
                _ => None,
            }
        };

        let preview = Arc::new(StreamingFilePreview::new(
            self.selected_bucket.clone(),
            self.selected_key.clone(),
            &self.preview_content,
            total_file_size,
            transform,
        ));

        self.streaming_preview = Some(preview.clone());
        self.streaming_enabled = true;
        self.streaming_cancel_flag = Some(Arc::new(AtomicBool::new(false)));

        let start_byte = preview.next_byte_needed();
        if start_byte < total_file_size {
            if let Some(ref backend) = self.backend {
                backend.get_object_streaming(
                    &self.selected_bucket,
                    &self.selected_key,
                    start_byte,
                    total_file_size,
                    self.streaming_cancel_flag.clone(),
                );
            }
        }
    }

    fn cancel_streaming_download(&mut self) {
        if let Some(ref flag) = self.streaming_cancel_flag {
            flag.store(true, Ordering::Relaxed);
        }
        self.streaming_cancel_flag = None;
        self.streaming_preview = None;
        self.streaming_enabled = false;
    }
}

impl Drop for BrowserModel {
    fn drop(&mut self) {
        if let Some(ref flag) = self.streaming_cancel_flag {
            flag.store(true, Ordering::Relaxed);
        }
        if let Some(ref flag) = self.pagination_cancel_flag {
            flag.store(true, Ordering::Relaxed);
        }
    }
}

fn frecency_score(entry: &PathEntry, now: i64) -> f64 {
    let age = now - entry.last_accessed;
    let weight = if age < 3600 {
        4.0
    } else if age < 86400 {
        2.0
    } else if age < 604800 {
        1.0
    } else {
        0.5
    };
    entry.score * weight
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

    if let Some(slash) = p.find('/') {
        Some((p[..slash].to_string(), p[slash + 1..].to_string()))
    } else {
        Some((p.to_string(), String::new()))
    }
}

pub fn is_preview_supported(key: &str) -> bool {
    let dot_pos = match key.rfind('.') {
        Some(p) => p,
        None => return false,
    };

    let mut ext = key[dot_pos..].to_lowercase();

    // If compressed, check underlying extension
    if matches!(ext.as_str(), ".gz" | ".zst" | ".zstd") && dot_pos > 0 {
        let without_compression = &key[..dot_pos];
        let inner_dot = match without_compression.rfind('.') {
            Some(p) => p,
            None => return false,
        };
        ext = without_compression[inner_dot..].to_lowercase();
    }

    matches!(
        ext.as_str(),
        ".txt"
            | ".md"
            | ".markdown"
            | ".rst"
            | ".rtf"
            | ".tex"
            | ".log"
            | ".readme"
            | ".html"
            | ".htm"
            | ".xhtml"
            | ".xml"
            | ".svg"
            | ".css"
            | ".scss"
            | ".sass"
            | ".less"
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
            | ".properties"
            | ".env"
            | ".c"
            | ".h"
            | ".cpp"
            | ".hpp"
            | ".cc"
            | ".hh"
            | ".cxx"
            | ".hxx"
            | ".m"
            | ".mm"
            | ".java"
            | ".kt"
            | ".kts"
            | ".scala"
            | ".groovy"
            | ".gradle"
            | ".py"
            | ".pyw"
            | ".pyi"
            | ".js"
            | ".mjs"
            | ".cjs"
            | ".jsx"
            | ".ts"
            | ".tsx"
            | ".mts"
            | ".cts"
            | ".rb"
            | ".rake"
            | ".gemspec"
            | ".php"
            | ".phtml"
            | ".pl"
            | ".pm"
            | ".pod"
            | ".lua"
            | ".r"
            | ".rmd"
            | ".go"
            | ".rs"
            | ".swift"
            | ".zig"
            | ".nim"
            | ".v"
            | ".d"
            | ".hs"
            | ".lhs"
            | ".ml"
            | ".mli"
            | ".fs"
            | ".fsi"
            | ".fsx"
            | ".ex"
            | ".exs"
            | ".erl"
            | ".hrl"
            | ".clj"
            | ".cljs"
            | ".cljc"
            | ".edn"
            | ".lisp"
            | ".cl"
            | ".el"
            | ".scm"
            | ".ss"
            | ".sh"
            | ".bash"
            | ".zsh"
            | ".fish"
            | ".ksh"
            | ".csh"
            | ".tcsh"
            | ".ps1"
            | ".psm1"
            | ".psd1"
            | ".bat"
            | ".cmd"
            | ".sql"
            | ".mysql"
            | ".pgsql"
            | ".sqlite"
            | ".graphql"
            | ".gql"
            | ".dockerfile"
            | ".tf"
            | ".tfvars"
            | ".hcl"
            | ".cmake"
            | ".make"
            | ".makefile"
            | ".mk"
            | ".ninja"
            | ".bazel"
            | ".bzl"
            | ".sbt"
            | ".gitignore"
            | ".gitattributes"
            | ".gitmodules"
            | ".editorconfig"
            | ".prettierrc"
            | ".eslintrc"
            | ".proto"
            | ".thrift"
            | ".avsc"
            | ".xsd"
            | ".dtd"
            | ".wsdl"
            | ".diff"
            | ".patch"
            | ".asm"
            | ".s"
            | ".png"
            | ".jpg"
            | ".jpeg"
            | ".gif"
            | ".bmp"
            | ".psd"
            | ".tga"
            | ".hdr"
            | ".pic"
            | ".pnm"
            | ".pgm"
            | ".ppm"
            | ".vim"
            | ".vimrc"
            | ".tmux"
            | ".zshrc"
            | ".bashrc"
            | ".profile"
            | ".htaccess"
            | ".nginx"
            | ".plist"
            | ".reg"
    )
}
