use crate::backend::Backend;
use crate::events::{S3Bucket, S3Object, StateEvent};
use std::collections::{HashMap, HashSet};

/// A cached folder's contents
pub struct FolderNode {
    pub bucket: String,
    pub prefix: String,
    pub objects: Vec<S3Object>,
    pub next_continuation_token: String,
    pub is_truncated: bool,
    pub loading: bool,
    pub loaded: bool,
    pub error: String,

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

    // File selection and preview
    pub selected_bucket: String,
    pub selected_key: String,
    pub preview_loading: bool,
    pub preview_supported: bool,
    pub preview_content: String,
    pub preview_error: String,

    // Preview cache
    preview_cache: HashMap<String, String>,
    pending_object_requests: HashSet<String>,
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
            selected_bucket: String::new(),
            selected_key: String::new(),
            preview_loading: false,
            preview_supported: false,
            preview_content: String::new(),
            preview_error: String::new(),
            preview_cache: HashMap::new(),
            pending_object_requests: HashSet::new(),
        }
    }

    pub fn set_backend(&mut self, backend: Box<dyn Backend>) {
        self.backend = Some(backend);
    }

    pub fn is_at_root(&self) -> bool {
        self.current_bucket.is_empty()
    }

    pub fn has_selection(&self) -> bool {
        !self.selected_key.is_empty()
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
                    node.is_truncated = is_truncated;
                    node.loading = false;
                    node.loaded = true;
                    node.error.clear();

                    // Auto-continue pagination for current folder
                    if bucket == self.current_bucket && prefix == self.current_prefix {
                        let should_load_more = {
                            let node = self.nodes.get(&Self::make_node_key(&bucket, &prefix));
                            node.map(|n| n.is_truncated).unwrap_or(false)
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
                    node.error = error;
                }
                StateEvent::ObjectContentLoaded {
                    bucket,
                    key,
                    content,
                } => {
                    let cache_key = Self::make_preview_cache_key(&bucket, &key);
                    self.preview_cache.insert(cache_key.clone(), content.clone());
                    self.pending_object_requests.remove(&cache_key);

                    if bucket == self.selected_bucket && key == self.selected_key {
                        self.preview_content = content;
                        self.preview_loading = false;
                        self.preview_error.clear();
                    }
                }
                StateEvent::ObjectContentError {
                    bucket,
                    key,
                    error,
                } => {
                    let cache_key = Self::make_preview_cache_key(&bucket, &key);
                    self.pending_object_requests.remove(&cache_key);

                    if bucket == self.selected_bucket && key == self.selected_key {
                        self.preview_loading = false;
                        self.preview_error = error;
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
        self.preview_cache.clear();
        self.pending_object_requests.clear();

        if let Some(b) = &self.backend {
            b.list_buckets();
        }
    }

    pub fn load_folder(&mut self, bucket: &str, prefix: &str) {
        let key = Self::make_node_key(bucket, prefix);
        if let Some(node) = self.nodes.get(&key) {
            if node.loaded {
                return;
            }
        }

        let node = self.get_or_create_node(bucket, prefix);
        node.objects.clear();
        node.error.clear();
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
            if !node.is_truncated || node.loading {
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
            .map(|n| n.loaded && n.is_truncated && !n.loading)
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
        if self.selected_bucket == bucket && self.selected_key == key {
            return;
        }

        self.selected_bucket = bucket.to_string();
        self.selected_key = key.to_string();
        self.preview_content.clear();
        self.preview_error.clear();
        self.preview_supported = Self::is_preview_supported(key);

        if self.preview_supported {
            // Check cache
            let cache_key = Self::make_preview_cache_key(bucket, key);
            if let Some(content) = self.preview_cache.get(&cache_key) {
                self.preview_content = content.clone();
                self.preview_loading = false;
                return;
            }

            self.preview_loading = true;
            self.pending_object_requests.insert(cache_key);
            if let Some(b) = &self.backend {
                b.get_object(bucket, key, PREVIEW_MAX_BYTES);
            }
        } else {
            self.preview_loading = false;
        }
    }

    pub fn clear_selection(&mut self) {
        self.selected_bucket.clear();
        self.selected_key.clear();
        self.preview_content.clear();
        self.preview_error.clear();
        self.preview_loading = false;
        self.preview_supported = false;
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
