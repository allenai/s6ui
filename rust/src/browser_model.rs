use crate::events::{S3Object, S3Request, StateEvent};
use crate::s3_backend::S3Backend;

pub struct BrowserModel {
    pub backend: S3Backend,
    pub profiles: Vec<String>,
    pub selected_profile_idx: usize,
    pub buckets: Vec<String>,
    pub selected_bucket_idx: usize,
    pub current_prefix: String,
    pub folders: Vec<String>,
    pub objects: Vec<S3Object>,
    pub loading: bool,
    pub error_message: Option<String>,
    pub selected_item: Option<usize>,
}

impl BrowserModel {
    pub fn new() -> Self {
        let profiles = load_aws_profiles();
        let backend = S3Backend::new();

        if !profiles.is_empty() {
            backend.send_request(S3Request::ListBuckets {
                profile: profiles[0].clone(),
            });
        }

        Self {
            backend,
            profiles,
            selected_profile_idx: 0,
            buckets: Vec::new(),
            selected_bucket_idx: 0,
            current_prefix: String::new(),
            folders: Vec::new(),
            objects: Vec::new(),
            loading: true,
            error_message: None,
            selected_item: None,
        }
    }

    pub fn process_events(&mut self) {
        while let Some(event) = self.backend.try_recv_event() {
            match event {
                StateEvent::BucketsLoaded { profile, buckets } => {
                    if self.current_profile() == profile {
                        self.buckets = buckets;
                        self.selected_bucket_idx = 0;
                        self.loading = false;
                        self.error_message = None;
                    }
                }
                StateEvent::ObjectsLoaded {
                    bucket,
                    prefix,
                    objects,
                    common_prefixes,
                    next_continuation_token,
                    is_continuation,
                } => {
                    let current_bucket = self.buckets.get(self.selected_bucket_idx).cloned();
                    if current_bucket.as_deref() == Some(&bucket)
                        && self.current_prefix == prefix
                    {
                        if is_continuation {
                            self.append_listing(objects, common_prefixes, &prefix);
                        } else {
                            self.replace_listing(objects, common_prefixes, &prefix);
                        }

                        if let Some(token) = next_continuation_token {
                            // Auto-continue pagination
                            self.backend.send_request(S3Request::ListObjects {
                                profile: self.current_profile().to_string(),
                                bucket,
                                prefix,
                                continuation_token: Some(token),
                            });
                        } else {
                            self.loading = false;
                        }
                    }
                }
                StateEvent::Error(msg) => {
                    self.error_message = Some(msg);
                    self.loading = false;
                }
            }
        }
    }

    fn replace_listing(
        &mut self,
        objects: Vec<S3Object>,
        common_prefixes: Vec<String>,
        prefix: &str,
    ) {
        self.folders = common_prefixes
            .iter()
            .filter_map(|cp| {
                let name = cp
                    .strip_prefix(prefix)
                    .unwrap_or(cp)
                    .trim_end_matches('/');
                if name.is_empty() {
                    None
                } else {
                    Some(name.to_string())
                }
            })
            .collect();
        self.objects = objects;
    }

    fn append_listing(
        &mut self,
        objects: Vec<S3Object>,
        common_prefixes: Vec<String>,
        prefix: &str,
    ) {
        for cp in &common_prefixes {
            let name = cp
                .strip_prefix(prefix)
                .unwrap_or(cp)
                .trim_end_matches('/');
            if !name.is_empty() && !self.folders.contains(&name.to_string()) {
                self.folders.push(name.to_string());
            }
        }
        self.objects.extend(objects);
    }

    pub fn current_profile(&self) -> &str {
        self.profiles
            .get(self.selected_profile_idx)
            .map(|s| s.as_str())
            .unwrap_or("default")
    }

    pub fn select_profile(&mut self, idx: usize) {
        if idx != self.selected_profile_idx && idx < self.profiles.len() {
            self.selected_profile_idx = idx;
            self.buckets.clear();
            self.clear_listing();
            self.loading = true;
            self.error_message = None;
            self.backend.send_request(S3Request::ListBuckets {
                profile: self.profiles[idx].clone(),
            });
        }
    }

    pub fn select_bucket(&mut self, idx: usize) {
        if idx < self.buckets.len() {
            self.selected_bucket_idx = idx;
            self.current_prefix.clear();
            self.clear_listing();
            self.loading = true;
            self.error_message = None;
            let profile = self.current_profile().to_string();
            self.backend.send_request(S3Request::ListObjects {
                profile,
                bucket: self.buckets[idx].clone(),
                prefix: String::new(),
                continuation_token: None,
            });
        }
    }

    pub fn navigate_to_prefix(&mut self, prefix: String) {
        self.current_prefix = prefix.clone();
        self.clear_listing();
        self.loading = true;
        self.error_message = None;
        if let Some(bucket) = self.buckets.get(self.selected_bucket_idx).cloned() {
            let profile = self.current_profile().to_string();
            self.backend.send_request(S3Request::ListObjects {
                profile,
                bucket,
                prefix,
                continuation_token: None,
            });
        }
    }

    pub fn navigate_up(&mut self) {
        if self.current_prefix.is_empty() {
            return;
        }
        let trimmed = self.current_prefix.trim_end_matches('/');
        let new_prefix = match trimmed.rfind('/') {
            Some(idx) => trimmed[..=idx].to_string(),
            None => String::new(),
        };
        self.navigate_to_prefix(new_prefix);
    }

    pub fn enter_folder(&mut self, folder_idx: usize) {
        if folder_idx < self.folders.len() {
            let new_prefix = format!("{}{}/", self.current_prefix, self.folders[folder_idx]);
            self.navigate_to_prefix(new_prefix);
        }
    }

    fn clear_listing(&mut self) {
        self.folders.clear();
        self.objects.clear();
        self.selected_item = None;
    }
}

fn load_aws_profiles() -> Vec<String> {
    let mut profiles = std::collections::BTreeSet::new();
    profiles.insert("default".to_string());

    let home = std::env::var("HOME").unwrap_or_default();
    if home.is_empty() {
        return profiles.into_iter().collect();
    }

    // Parse ~/.aws/config
    if let Ok(contents) = std::fs::read_to_string(format!("{home}/.aws/config")) {
        for line in contents.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("[profile ") {
                if let Some(name) = rest.strip_suffix(']') {
                    profiles.insert(name.trim().to_string());
                }
            }
        }
    }

    // Parse ~/.aws/credentials
    if let Ok(contents) = std::fs::read_to_string(format!("{home}/.aws/credentials")) {
        for line in contents.lines() {
            let line = line.trim();
            if line.starts_with('[') && line.ends_with(']') {
                let name = &line[1..line.len() - 1];
                profiles.insert(name.trim().to_string());
            }
        }
    }

    profiles.into_iter().collect()
}
