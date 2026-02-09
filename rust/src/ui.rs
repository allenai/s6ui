use crate::aws::signer::aws_generate_presigned_url;
use crate::model::BrowserModel;
use crate::preview::image_preview::ImagePreviewRenderer;
use crate::preview::jsonl_preview::JsonlPreviewRenderer;
use crate::preview::renderer::{PreviewContext, PreviewRenderer};
use crate::preview::text_preview::TextPreviewRenderer;
use dear_imgui_rs::*;

pub struct BrowserUI {
    path_input: String,
    preview_renderers: Vec<Box<dyn PreviewRenderer>>,
    active_renderer_idx: Option<usize>,
}

impl BrowserUI {
    pub fn new() -> Self {
        Self {
            path_input: "s3://".to_string(),
            preview_renderers: vec![
                Box::new(ImagePreviewRenderer::new()),
                Box::new(JsonlPreviewRenderer::new()),
                Box::new(TextPreviewRenderer::new()),
            ],
            active_renderer_idx: None,
        }
    }

    pub fn render(&mut self, ui: &Ui, model: &mut BrowserModel, window_width: f32, window_height: f32) {
        ui.window("S6 UI")
            .position([0.0, 0.0], Condition::Always)
            .size([window_width, window_height], Condition::Always)
            .flags(
                WindowFlags::NO_TITLE_BAR
                    | WindowFlags::NO_RESIZE
                    | WindowFlags::NO_MOVE
                    | WindowFlags::NO_COLLAPSE
                    | WindowFlags::NO_BRING_TO_FRONT_ON_FOCUS
                    | WindowFlags::NO_SCROLLBAR
                    | WindowFlags::NO_SCROLL_WITH_MOUSE,
            )
            .build(|| {
                self.render_top_bar(ui, model);
                ui.separator();

                let avail = ui.content_region_avail();
                let pane_width = avail[0] * 0.5 - 4.0;
                let pane_height = avail[1];

                self.render_left_pane(ui, model, pane_width, pane_height);
                ui.same_line();
                self.render_preview_pane(ui, model, pane_width, pane_height);
            });
    }

    fn render_top_bar(&mut self, ui: &Ui, model: &mut BrowserModel) {
        ui.text("Profile:");
        ui.same_line();

        let profile_names: Vec<String> = model.profiles().iter().map(|p| p.name.clone()).collect();
        if !profile_names.is_empty() {
            let name_refs: Vec<&str> = profile_names.iter().map(|s| s.as_str()).collect();

            let mut selected: usize = model.selected_profile_index() as usize;
            ui.set_next_item_width(150.0);
            if ui.combo("##profile", &mut selected, &name_refs, |name| (*name).into()) {
                model.select_profile(selected as i32);
                self.path_input = "s3://".to_string();
            }

            ui.same_line();
            let region = model.profiles()[selected].region.clone();
            ui.text_colored([0.5, 0.5, 0.5, 1.0], format!("({})", region));
        } else {
            ui.text_colored(
                [1.0, 0.3, 0.3, 1.0],
                "No AWS profiles found in ~/.aws/credentials",
            );
        }

        // Path input
        ui.same_line();
        ui.text("Path:");
        ui.same_line();

        let refresh_width = 70.0;
        let arrow_width = ui.frame_height();
        let path_width =
            ui.window_size()[0] - ui.cursor_pos()[0] - refresh_width - arrow_width - 24.0;
        ui.set_next_item_width(path_width);

        if ui
            .input_text("##path", &mut self.path_input)
            .enter_returns_true(true)
            .build()
        {
            model.navigate_to(&self.path_input);
        }

        // Sync path from model
        let current_path = build_s3_path(model.current_bucket(), model.current_prefix());
        if current_path != self.path_input && !ui.is_item_active() {
            self.path_input = current_path;
        }

        // Recent paths dropdown
        ui.same_line_with_spacing(0.0, 0.0);
        if ui.arrow_button("##recent_paths", Direction::Down) {
            ui.open_popup("RecentPathsPopup");
        }

        if let Some(_token) = ui.begin_popup("RecentPathsPopup") {
            let top_paths = model.top_frecent_paths(20);
            if top_paths.is_empty() {
                ui.text_disabled("No recent paths");
            } else {
                for path in &top_paths {
                    if ui.selectable(path) {
                        self.path_input = path.clone();
                        model.navigate_to(path);
                    }
                }
            }
        }

        // Refresh button
        ui.same_line();
        let cmd_r = ui.io().key_super() && ui.is_key_pressed(Key::R);
        if ui.button("Refresh") || cmd_r {
            model.refresh();
        }
    }

    fn render_left_pane(&mut self, ui: &Ui, model: &mut BrowserModel, width: f32, height: f32) {
        let status_height = ui.frame_height_with_spacing() + 4.0;
        let mut content_height = height - status_height;
        let mut has_nav_up = false;

        ui.child_window("LeftPane").size([width, height]).build(ui, || {
            // [..] nav-up button
            if !model.is_at_root() {
                has_nav_up = true;
                if ui.selectable("[..]") {
                    model.navigate_up();
                }
                if ui.is_item_hovered() {
                    let bucket = model.current_bucket().to_string();
                    let prefix = model.current_prefix().to_string();
                    let mut parent = prefix.clone();
                    if parent.ends_with('/') {
                        parent.pop();
                    }
                    parent = match parent.rfind('/') {
                        None => String::new(),
                        Some(pos) => parent[..pos + 1].to_string(),
                    };
                    model.prefetch_folder(&bucket, &parent);
                }
                content_height -= ui.frame_height_with_spacing();
            }

            // File content area
            ui.child_window("FileContent")
                .size([width, content_height])
                .border(true)
                .flags(WindowFlags::HORIZONTAL_SCROLLBAR)
                .build(ui, || {
                    self.render_content(ui, model);
                });

            // Status bar
            self.render_status_bar(ui, model);
        });
    }

    fn render_content(&self, ui: &Ui, model: &mut BrowserModel) {
        if model.is_at_root() {
            self.render_bucket_list(ui, model);
        } else {
            self.render_folder_contents(ui, model);
        }
    }

    fn render_bucket_list(&self, ui: &Ui, model: &mut BrowserModel) {
        if model.buckets_loading() {
            ui.text_colored([0.5, 0.5, 1.0, 1.0], "Loading buckets...");
            return;
        }

        if !model.buckets_error().is_empty() {
            ui.text_colored([1.0, 0.3, 0.3, 1.0], format!("Error: {}", model.buckets_error()));
            return;
        }

        let buckets: Vec<_> = model.buckets().iter().map(|b| b.name.clone()).collect();

        if buckets.is_empty() {
            ui.text_colored([0.7, 0.7, 0.7, 1.0], "No buckets found");
            return;
        }

        for bucket_name in &buckets {
            let label = format!("[B] {}", bucket_name);
            if ui.selectable(&label) {
                model.navigate_into(bucket_name, "");
            }
            // Right-click context menu
            if let Some(_popup) = ui.begin_popup_context_item() {
                if ui.menu_item("Copy path") {
                    let _path = format!("s3://{}/", bucket_name);
                    // Clipboard not directly available on Ui; would need platform integration
                }
            }
        }
    }

    fn render_folder_contents(&self, ui: &Ui, model: &mut BrowserModel) {
        let bucket = model.current_bucket().to_string();
        let prefix = model.current_prefix().to_string();

        let node = match model.get_node(&bucket, &prefix) {
            Some(n) => n,
            None => {
                model.load_folder(&bucket, &prefix);
                ui.text_colored([0.5, 0.5, 1.0, 1.0], "Loading...");
                return;
            }
        };

        if node.loading && node.objects.is_empty() {
            ui.text_colored([0.5, 0.5, 1.0, 1.0], "Loading...");
            return;
        }

        if !node.error.is_empty() {
            ui.text_colored([1.0, 0.3, 0.3, 1.0], format!("Error: {}", node.error));
            return;
        }

        // Build the display data we need (to avoid borrowing model during iteration)
        let selected_bucket = model.selected_bucket().to_string();
        let selected_key = model.selected_key().to_string();

        // We need the sorted view - first ensure it's built
        if let Some(node) = model.get_node_mut(&bucket, &prefix) {
            node.rebuild_sorted_view_if_needed();
        }

        let node = match model.get_node(&bucket, &prefix) {
            Some(n) => n,
            None => return,
        };

        let items: Vec<(usize, String, String, bool, i64)> = node
            .sorted_view
            .iter()
            .enumerate()
            .map(|(view_idx, &obj_idx)| {
                let obj = &node.objects[obj_idx];
                let is_folder = view_idx < node.folder_count;
                (
                    obj_idx,
                    obj.display_name.clone(),
                    obj.key.clone(),
                    is_folder,
                    obj.size,
                )
            })
            .collect();

        let is_loading = node.loading;
        let is_truncated = node.is_truncated;
        let next_token_empty = node.next_continuation_token.is_empty();
        let total_count = node.objects.len();

        // Use ListClipper for virtual scrolling
        let clipper = ListClipper::new(items.len() as i32);
        let clipper_token = clipper.begin(ui);

        for i in clipper_token.iter() {
            let (obj_idx, display_name, key, is_folder, size) = &items[i as usize];
            let _id = ui.push_id(*obj_idx as i32);

            if *is_folder {
                let label = format!("[D] {}", display_name);
                if ui.selectable(&label) {
                    model.navigate_into(&bucket, key);
                }
                if let Some(_popup) = ui.begin_popup_context_item() {
                    if ui.menu_item("Copy path") {
                        let _path = format!("s3://{}/{}", bucket, key);
                        // Clipboard not directly available on Ui
                    }
                }
                if ui.is_item_hovered() {
                    model.prefetch_folder(&bucket, key);
                }
            } else {
                let label = format!("    {}  ({})", display_name, format_size(*size));
                let is_selected = selected_bucket == bucket && *key == selected_key;
                if ui.selectable_config(&label).selected(is_selected).build() {
                    model.select_file(&bucket, key);
                }
                if let Some(_popup) = ui.begin_popup_context_item() {
                    if ui.menu_item("Copy path") {
                        let _path = format!("s3://{}/{}", bucket, key);
                        // Clipboard not directly available on Ui
                    }
                    if ui.menu_item("Copy pre-signed URL (7 days)") {
                        let profiles = model.profiles();
                        let idx = model.selected_profile_index() as usize;
                        if idx < profiles.len() {
                            let profile = &profiles[idx];
                            let _url = aws_generate_presigned_url(
                                &bucket,
                                key,
                                &profile.region,
                                &profile.access_key_id,
                                &profile.secret_access_key,
                                &profile.session_token,
                                604800,
                            );
                            // Clipboard not directly available on Ui
                        }
                    }
                }
                if ui.is_item_hovered() {
                    model.prefetch_file_preview(&bucket, key);
                }
            }
        }

        // Loading/load more indicators
        if is_loading {
            ui.text_colored([0.5, 0.5, 1.0, 1.0], "Loading more...");
        }

        if is_truncated && !is_loading && !next_token_empty {
            ui.spacing();
            if ui.button("Load more") {
                model.load_more(&bucket, &prefix);
            }
            ui.same_line();
            ui.text_colored(
                [0.7, 0.7, 0.7, 1.0],
                format!("({} items loaded)", format_number(total_count as i64)),
            );
        }
    }

    fn render_status_bar(&self, ui: &Ui, model: &BrowserModel) {
        ui.separator();

        if model.is_at_root() {
            if model.buckets_loading() {
                ui.text("Loading buckets...");
            } else if !model.buckets_error().is_empty() {
                ui.text_colored([1.0, 0.3, 0.3, 1.0], "Error loading buckets");
            } else {
                let count = model.buckets().len();
                ui.text(format!(
                    "{} bucket{}",
                    format_number(count as i64),
                    if count == 1 { "" } else { "s" }
                ));
            }
        } else {
            let bucket = model.current_bucket();
            let prefix = model.current_prefix();
            let node = model.get_node(bucket, prefix);

            match node {
                None => ui.text("Loading..."),
                Some(node) if node.loading && node.objects.is_empty() => {
                    ui.text("Loading...");
                }
                Some(node) if !node.error.is_empty() => {
                    ui.text_colored([1.0, 0.3, 0.3, 1.0], "Error");
                }
                Some(node) => {
                    let mut folder_count = 0usize;
                    let mut file_count = 0usize;
                    let mut total_size: i64 = 0;

                    for obj in &node.objects {
                        if obj.is_folder {
                            folder_count += 1;
                        } else {
                            file_count += 1;
                            total_size += obj.size;
                        }
                    }

                    let mut status = String::new();
                    if folder_count > 0 {
                        status.push_str(&format!(
                            "{} folder{}",
                            format_number(folder_count as i64),
                            if folder_count == 1 { "" } else { "s" }
                        ));
                    }
                    if file_count > 0 {
                        if !status.is_empty() {
                            status.push_str(", ");
                        }
                        status.push_str(&format!(
                            "{} file{} ({})",
                            format_number(file_count as i64),
                            if file_count == 1 { "" } else { "s" },
                            format_size(total_size)
                        ));
                    }
                    if status.is_empty() {
                        status = "Empty folder".to_string();
                    }

                    if node.loading {
                        status.push_str("  Loading...");
                    } else if node.is_truncated {
                        status.push_str("  [more available]");
                    }

                    ui.text(&status);
                }
            }
        }
    }

    fn render_preview_pane(&mut self, ui: &Ui, model: &mut BrowserModel, width: f32, height: f32) {
        ui.child_window("PreviewPane").size([width, height]).border(true).build(ui, || {
            if !model.has_selection() {
                if let Some(idx) = self.active_renderer_idx.take() {
                    self.preview_renderers[idx].reset();
                }
                ui.text_colored([0.5, 0.5, 0.5, 1.0], "Select a file to preview");
                return;
            }

            let key = model.selected_key().to_string();
            let last_slash = key.rfind('/');
            let filename = match last_slash {
                Some(pos) => &key[pos + 1..],
                None => &key,
            }
            .to_string();

            if !model.preview_supported() {
                if let Some(idx) = self.active_renderer_idx.take() {
                    self.preview_renderers[idx].reset();
                }
                ui.text(format!("Preview: {}", filename));
                ui.separator();
                ui.text_colored(
                    [0.7, 0.7, 0.7, 1.0],
                    "Preview not supported for this file type",
                );
                return;
            }

            if model.preview_loading() {
                ui.text(format!("Preview: {}", filename));
                ui.separator();
                ui.text_colored([0.5, 0.5, 1.0, 1.0], "Loading preview...");
                return;
            }

            if !model.preview_error().is_empty() {
                if let Some(idx) = self.active_renderer_idx.take() {
                    self.preview_renderers[idx].reset();
                }
                ui.text(format!("Preview: {}", filename));
                ui.separator();
                ui.text_colored(
                    [1.0, 0.3, 0.3, 1.0],
                    format!("Error: {}", model.preview_error()),
                );
                return;
            }

            // Find renderer
            let bucket = model.selected_bucket().to_string();
            let mut renderer_idx = None;
            for (i, r) in self.preview_renderers.iter().enumerate() {
                if r.can_handle(&key) {
                    if i == 1 && !model.has_streaming_preview() {
                        continue;
                    }
                    if r.wants_fallback(&bucket, &key) {
                        continue;
                    }
                    renderer_idx = Some(i);
                    break;
                }
            }

            if let Some(idx) = renderer_idx {
                if self.active_renderer_idx != Some(idx) {
                    if let Some(old_idx) = self.active_renderer_idx {
                        self.preview_renderers[old_idx].reset();
                    }
                    self.active_renderer_idx = Some(idx);
                }

                let avail = ui.content_region_avail();
                let streaming = model.streaming_preview().cloned();
                let ctx = PreviewContext {
                    model,
                    bucket: &bucket,
                    key: &key,
                    filename: &filename,
                    streaming_preview: streaming.as_ref(),
                    available_width: avail[0],
                    available_height: avail[1],
                };
                self.preview_renderers[idx].render(ui, &ctx);
            } else {
                ui.text(format!("Preview: {}", filename));
                ui.separator();
                ui.text_colored([0.7, 0.7, 0.7, 1.0], "No preview renderer available");
            }
        });
    }
}

fn format_number(number: i64) -> String {
    let s = number.to_string();
    let bytes = s.as_bytes();
    let mut result = String::new();
    let len = bytes.len();

    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            result.push(',');
        }
        result.push(b as char);
    }
    result
}

fn format_size(bytes: i64) -> String {
    if bytes < 1024 {
        format!("{} B", format_number(bytes))
    } else if bytes < 1024 * 1024 {
        format!("{} KB", format_number(bytes / 1024))
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{} MB", format_number(bytes / (1024 * 1024)))
    } else {
        format!("{} GB", format_number(bytes / (1024 * 1024 * 1024)))
    }
}

fn build_s3_path(bucket: &str, prefix: &str) -> String {
    if bucket.is_empty() {
        "s3://".to_string()
    } else if prefix.is_empty() {
        format!("s3://{}/", bucket)
    } else {
        format!("s3://{}/{}", bucket, prefix)
    }
}
