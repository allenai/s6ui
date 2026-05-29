use crate::app_clipboard::copy_text_to_clipboard;
use crate::jsonl_viewer::JsonlPreviewer;
use crate::model::{BrowserModel, PreviewNode, PreviewStatus};
use crate::preview::StreamingFilePreview;
use crate::text_viewer::MmapTextViewer;
use crate::warc_viewer::WarcGallery;
use dear_imgui_rs::*;
use dear_imgui_wgpu::WgpuRenderer;
use std::borrow::Cow;
use std::sync::Arc;

pub struct BrowserUI {
    path_input: String,
    /// Text viewer for file preview
    text_viewer: MmapTextViewer,
    jsonl_previewer: JsonlPreviewer,
    /// Image-WARC gallery viewer
    warc_gallery: WarcGallery,
    /// Currently loaded preview key (bucket/key)
    viewer_preview_key: Option<String>,
    /// Backing preview object for the currently loaded preview key
    viewer_preview_source_id: Option<usize>,
}

impl BrowserUI {
    pub fn new() -> Self {
        Self {
            path_input: "s3://".to_string(),
            text_viewer: MmapTextViewer::new(),
            jsonl_previewer: JsonlPreviewer::new(),
            warc_gallery: WarcGallery::new(),
            viewer_preview_key: None,
            viewer_preview_source_id: None,
        }
    }

    /// Decode + upload image-WARC textures for the selected preview. Must be called
    /// once per frame *before* the ImGui frame begins (needs GPU access).
    pub fn sync_warc_textures(
        &mut self,
        model: &BrowserModel,
        device: &wgpu::Device,
        queue: &wgpu::Queue,
        renderer: &mut WgpuRenderer,
    ) {
        self.warc_gallery.sync(model, device, queue, renderer);
    }

    pub fn render(&mut self, ui: &Ui, model: &mut BrowserModel, window_size: [f32; 2]) {
        ui.window("S6 UI")
            .position([0.0, 0.0], Condition::Always)
            .size(window_size, Condition::Always)
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
                // render_preview_pane needs mutable model for continue_download
                self.render_preview_pane(ui, model, pane_width, pane_height);
            });
    }

    fn render_top_bar(&mut self, ui: &Ui, model: &mut BrowserModel) {
        ui.text("Profile:");
        ui.same_line();

        if !model.profiles.is_empty() {
            let profile_names: Vec<String> =
                model.profiles.iter().map(|p| p.name.clone()).collect();

            let mut selected = model.selected_profile_idx;
            ui.set_next_item_width(150.0);
            if ui.combo("##profile", &mut selected, &profile_names, |s: &String| {
                Cow::Borrowed(s.as_str())
            }) {
                model.select_profile(selected);
                self.path_input = "s3://".to_string();
            }

            ui.same_line();
            let region = &model.profiles[model.selected_profile_idx].region;
            ui.text_colored([0.5, 0.5, 0.5, 1.0], format!("({})", region));
        } else {
            ui.text_colored(
                [1.0, 0.3, 0.3, 1.0],
                "No AWS profiles found in ~/.aws/credentials",
            );
        }

        ui.same_line();
        ui.text("Path:");
        ui.same_line();

        let refresh_width = 70.0;
        let recent_paths_width = ui.frame_height();
        let path_width =
            ui.window_size()[0] - ui.cursor_pos()[0] - refresh_width - recent_paths_width - 16.0;
        ui.set_next_item_width(path_width);

        if ui
            .input_text("##path", &mut self.path_input)
            .enter_returns_true(true)
            .build()
        {
            let path = self.path_input.clone();
            model.navigate_to(&path);
        }

        // Sync path display with model state (when not actively editing)
        let current_path = build_s3_path(&model.current_bucket, &model.current_prefix);
        if current_path != self.path_input && !ui.is_item_active() {
            self.path_input = current_path;
        }

        ui.same_line();
        if ui.arrow_button("##recent_paths", Direction::Down) {
            ui.open_popup("RecentPathsPopup");
        }

        if let Some(_popup) = ui.begin_popup("RecentPathsPopup") {
            let top_paths = model.top_frecent_paths(20);
            if top_paths.is_empty() {
                ui.text_colored([0.5, 0.5, 0.5, 1.0], "No recent paths");
            } else {
                for path in top_paths {
                    if ui.selectable(path.as_str()) {
                        self.path_input = path.clone();
                        model.navigate_to(&path);
                    }
                }
            }
        }

        ui.same_line();
        let cmd_r = shortcut_mod_active(ui.io()) && ui.is_key_pressed(Key::R);
        if ui.button("Refresh") || cmd_r {
            model.refresh();
        }
    }

    fn render_left_pane(&mut self, ui: &Ui, model: &mut BrowserModel, width: f32, height: f32) {
        ui.child_window("LeftPane")
            .size([width, height])
            .build(ui, || {
                let status_bar_height = ui.frame_height_with_spacing() + 4.0;
                let mut content_height = height - status_bar_height;

                // [..] navigate up
                if !model.is_at_root() {
                    let parent_path = parent_s3_path(&model.current_bucket, &model.current_prefix);
                    if ui.selectable("[..]") {
                        model.navigate_up();
                    }
                    render_copy_path_context_menu(ui, &parent_path);
                    content_height -= ui.frame_height_with_spacing();
                }

                // File browser content
                ui.child_window("FileContent")
                    .size([width, content_height])
                    .border(true)
                    .flags(WindowFlags::HORIZONTAL_SCROLLBAR)
                    .build(ui, || {
                        if model.is_at_root() {
                            self.render_bucket_list(ui, model);
                        } else {
                            self.render_folder_contents(ui, model);
                        }

                        let current_path =
                            build_s3_path(&model.current_bucket, &model.current_prefix);
                        render_browser_background_context_menu(ui, &current_path);
                    });

                // Status bar
                ui.separator();
                self.render_status_bar(ui, model);
            });
    }

    fn render_bucket_list(&self, ui: &Ui, model: &mut BrowserModel) {
        model.ensure_buckets_loaded();

        if model.buckets_loading && model.buckets.is_empty() {
            ui.text_colored([0.5, 0.5, 1.0, 1.0], "Loading buckets...");
            return;
        }

        if !model.buckets_error.is_empty() && model.buckets.is_empty() {
            ui.text_colored(
                [1.0, 0.3, 0.3, 1.0],
                format!("Error: {}", model.buckets_error),
            );
            return;
        }

        if model.buckets.is_empty() {
            ui.text_colored([0.7, 0.7, 0.7, 1.0], "No buckets found");
            return;
        }

        // Clone bucket names to avoid borrow conflict
        let bucket_names: Vec<String> = model.buckets.iter().map(|b| b.name.clone()).collect();
        let mut pending_hover_bucket: Option<String> = None;
        for name in &bucket_names {
            let label = format!("[B] {}", name);
            let bucket_path = build_s3_path(name, "");
            if ui.selectable(label) {
                model.navigate_into(name, "");
                return;
            }
            render_copy_path_context_menu(ui, &bucket_path);
            if ui.is_item_hovered() {
                pending_hover_bucket = Some(name.clone());
            }
        }

        if let Some(bucket) = pending_hover_bucket {
            model.prefetch_folder(&bucket, "");
        }
    }

    fn render_folder_contents(&self, ui: &Ui, model: &mut BrowserModel) {
        let bucket = model.current_bucket.clone();
        let prefix = model.current_prefix.clone();

        // Ensure folder is loaded
        let node_exists = model.get_node(&bucket, &prefix).is_some();
        if !node_exists {
            model.ensure_current_folder_loaded();
            ui.text_colored([0.5, 0.5, 1.0, 1.0], "Loading...");
            return;
        }

        // Check loading/error state
        {
            let node = model.get_node(&bucket, &prefix).unwrap();
            if node.loading && node.objects.is_empty() {
                ui.text_colored([0.5, 0.5, 1.0, 1.0], "Loading...");
                return;
            }
            if let Some(err) = node.error() {
                ui.text_colored([1.0, 0.3, 0.3, 1.0], format!("Error: {}", err));
                return;
            }
        }

        // Rebuild sorted view
        if let Some(node) = model.get_node_mut(&bucket, &prefix) {
            node.rebuild_sorted_view_if_needed();
        }

        // Get metadata without cloning large data structures
        let (item_count, folder_count, is_loading, is_truncated) = {
            let node = model.get_node(&bucket, &prefix).unwrap();
            (
                node.sorted_view.len(),
                node.folder_count,
                node.loading,
                node.is_truncated(),
            )
        };

        // Track pending action (can't mutate model while borrowing node)
        let mut pending_navigate: Option<String> = None;
        let mut pending_select: Option<String> = None;
        let mut pending_hover_folder: Option<String> = None;
        let mut pending_hover_file: Option<String> = None;

        // Use ListClipper for virtual scrolling - only access visible items
        {
            let node = model.get_node(&bucket, &prefix).unwrap();
            let clipper = ListClipper::new(item_count as i32).begin(ui);

            for i in clipper.iter() {
                let idx = node.sorted_view[i as usize];
                let obj = &node.objects[idx];
                let is_folder_row = (i as usize) < folder_count;

                let _id = ui.push_id(idx as i32);

                if is_folder_row {
                    let label = format!("[D] {}", obj.display_name);
                    let copy_path = build_s3_path(&bucket, &obj.key);
                    if ui.selectable(label) {
                        pending_navigate = Some(obj.key.clone());
                    } else if ui.is_item_hovered() {
                        pending_hover_folder = Some(obj.key.clone());
                    }
                    render_copy_path_context_menu(ui, &copy_path);
                } else {
                    let label = format!("    {}  ({})", obj.display_name, format_size(obj.size));
                    let preview_key = format!("{}/{}", bucket, obj.key);
                    let is_selected = model.selected_preview.as_ref() == Some(&preview_key);
                    let copy_path = build_s3_path(&bucket, &obj.key);
                    if ui.selectable_config(label).selected(is_selected).build() {
                        pending_select = Some(obj.key.clone());
                    } else if ui.is_item_hovered() {
                        pending_hover_file = Some(obj.key.clone());
                    }
                    render_copy_path_context_menu(ui, &copy_path);
                }
            }
        }

        // Apply pending actions after borrow ends
        if let Some(key) = pending_navigate {
            model.navigate_into(&bucket, &key);
        } else if let Some(key) = pending_select {
            model.select_file(&bucket, &key);
        } else if let Some(key) = pending_hover_folder {
            model.prefetch_folder(&bucket, &key);
        } else if let Some(key) = pending_hover_file {
            model.prefetch_file(&bucket, &key);
        }

        // Loading indicator at bottom
        if is_loading {
            ui.text_colored([0.5, 0.5, 1.0, 1.0], "Loading more...");
        }

        if is_truncated && !is_loading {
            ui.spacing();
            if ui.button("Load more") {
                model.load_more(&bucket, &prefix);
            }
            ui.same_line();
            ui.text_colored(
                [0.7, 0.7, 0.7, 1.0],
                format!("({} items loaded)", format_number(item_count as i64)),
            );
        }
    }

    fn render_status_bar(&self, ui: &Ui, model: &BrowserModel) {
        if model.is_at_root() {
            if model.buckets_loading {
                ui.text("Loading buckets...");
            } else if !model.buckets_error.is_empty() {
                ui.text_colored([1.0, 0.3, 0.3, 1.0], "Error loading buckets");
            } else {
                let count = model.buckets.len();
                ui.text(format!(
                    "{} bucket{}",
                    format_number(count as i64),
                    if count == 1 { "" } else { "s" }
                ));
            }
        } else {
            let node = model.get_node(&model.current_bucket, &model.current_prefix);
            match node {
                None => ui.text("Loading..."),
                Some(node) if node.loading && node.objects.is_empty() => {
                    ui.text("Loading...");
                }
                Some(node) if node.error().is_some() => {
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
                        status += &format!(
                            "{} folder{}",
                            format_number(folder_count as i64),
                            if folder_count == 1 { "" } else { "s" }
                        );
                    }
                    if file_count > 0 {
                        if !status.is_empty() {
                            status += ", ";
                        }
                        status += &format!(
                            "{} file{} ({})",
                            format_number(file_count as i64),
                            if file_count == 1 { "" } else { "s" },
                            format_size(total_size)
                        );
                    }
                    if status.is_empty() {
                        status = "Empty folder".to_string();
                    }

                    if node.loading {
                        status += "  Loading...";
                    } else if node.is_truncated() {
                        status += "  [more available]";
                    }

                    ui.text(status);
                }
            }
        }
    }

    fn render_preview_pane(&mut self, ui: &Ui, model: &mut BrowserModel, width: f32, height: f32) {
        ui.child_window("PreviewPane")
            .size([width, height])
            .border(true)
            .build(ui, || {
                // Check if we need to update the viewer's source
                let current_preview_key = model.selected_preview.clone();
                let current_preview_source_id = model
                    .selected_preview()
                    .map(|node| Self::preview_source_id(&node.preview));

                // Update viewer if preview changed
                if current_preview_key != self.viewer_preview_key
                    || current_preview_source_id != self.viewer_preview_source_id
                {
                    self.viewer_preview_key = current_preview_key.clone();
                    self.viewer_preview_source_id = current_preview_source_id;
                    if let Some(node) = model.selected_preview() {
                        self.text_viewer.open(node.preview.clone());
                    } else {
                        self.text_viewer.close();
                    }
                }

                // Refresh viewer to pick up new streaming data
                self.text_viewer.refresh();

                let preview = model.selected_preview();
                match preview {
                    None => {
                        ui.text_colored([0.5, 0.5, 0.5, 1.0], "Select a file to preview");
                    }
                    Some(node) => {
                        let filename = Self::filename_for_key(&node.key);

                        // Image-WARC objects render as a thumbnail gallery (textures
                        // were uploaded pre-frame in sync_warc_textures). Matches by
                        // name or by content-sniff (extension-less Firehose batches).
                        if WarcGallery::applies(node) {
                            self.warc_gallery.render(ui, width, height);
                            return;
                        }

                        let handled_by_jsonl = JsonlPreviewer::can_handle(&node.key)
                            && self
                                .jsonl_previewer
                                .render(ui, node, filename, width, height);
                        if handled_by_jsonl {
                            return;
                        }

                        if JsonlPreviewer::can_handle(&node.key)
                            && self
                                .jsonl_previewer
                                .is_fallback_for(&node.bucket, &node.key)
                        {
                            ui.text_colored(
                                [0.65, 0.65, 0.65, 1.0],
                                "JSONL parsing failed, showing raw preview",
                            );
                            ui.separator();
                        }

                        self.render_text_preview_content(ui, node, filename);
                    }
                }
            });
    }

    fn render_text_preview_content(&mut self, ui: &Ui, node: &PreviewNode, filename: &str) {
        // Header with filename and wrap toggle
        ui.text(format!("Preview: {}", filename));
        ui.same_line();
        let mut wrap = self.text_viewer.word_wrap();
        if ui.checkbox("Wrap", &mut wrap) {
            self.text_viewer.set_word_wrap(wrap);
        }

        // Show progress info
        let status = node.status();
        let bytes = node.bytes_written();
        let is_complete = node.is_complete();
        let is_empty_complete = is_empty_complete_preview(bytes, is_complete);
        match &status {
            PreviewStatus::Loading => {
                ui.same_line();
                let source = node.source_bytes();
                if bytes > 0 {
                    ui.text_colored(
                        [0.5, 0.5, 1.0, 1.0],
                        format!(
                            " ({} decompressed from {} source)",
                            format_size(bytes as i64),
                            format_size(source as i64)
                        ),
                    );
                } else {
                    ui.text_colored([0.5, 0.5, 1.0, 1.0], " Loading...");
                }
            }
            PreviewStatus::Ready => {
                ui.same_line();
                if is_empty_complete {
                    ui.text_colored([0.5, 0.5, 0.5, 1.0], " (empty file)");
                } else {
                    let lines = self.text_viewer.line_count();
                    ui.text_colored(
                        [0.5, 0.5, 0.5, 1.0],
                        format!(
                            " ({}, {} lines)",
                            format_size(bytes as i64),
                            format_number(lines as i64)
                        ),
                    );
                }
            }
            _ => {}
        }

        ui.separator();

        match &status {
            PreviewStatus::Unsupported => {
                ui.text_colored(
                    [0.7, 0.7, 0.7, 1.0],
                    "Preview not supported for this file type",
                );
            }
            PreviewStatus::Error(err) => {
                ui.text_colored([1.0, 0.3, 0.3, 1.0], format!("Error: {}", err));
            }
            PreviewStatus::Loading | PreviewStatus::Ready => {
                // Show loading if no data yet
                if is_empty_complete {
                    ui.text_disabled("Empty file");
                } else if self.text_viewer.file_size() == 0 {
                    ui.text_colored([0.5, 0.5, 1.0, 1.0], "Loading...");
                } else if !self.text_viewer.is_open() {
                    // File has data but mmap failed - try to re-open
                    self.text_viewer.open(node.preview.clone());
                    ui.text_colored([0.5, 0.5, 1.0, 1.0], "Loading...");
                } else {
                    // Calculate available height for content
                    let content_height = ui.content_region_avail()[1];
                    let content_width = ui.content_region_avail()[0];

                    // Render using MmapTextViewer
                    self.text_viewer.render(ui, content_width, content_height);

                    if !is_complete && matches!(status, PreviewStatus::Loading) {
                        ui.spacing();
                        ui.text_colored([0.5, 0.5, 1.0, 1.0], "Downloading...");
                    }
                }
            }
        }
    }

    fn filename_for_key(key: &str) -> &str {
        match key.rfind('/') {
            Some(i) => &key[i + 1..],
            None => key,
        }
    }

    fn preview_source_id(preview: &Arc<StreamingFilePreview>) -> usize {
        Arc::as_ptr(preview) as usize
    }
}

fn format_number(n: i64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut result = String::new();
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(b as char);
    }
    result
}

fn format_size(bytes: i64) -> String {
    if bytes < 1024 {
        return format!("{} B", format_number(bytes));
    }
    if bytes < 1024 * 1024 {
        return format!("{} KB", format_number(bytes / 1024));
    }
    if bytes < 1024 * 1024 * 1024 {
        return format!("{} MB", format_number(bytes / (1024 * 1024)));
    }
    format!("{} GB", format_number(bytes / (1024 * 1024 * 1024)))
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

fn parent_s3_path(bucket: &str, prefix: &str) -> String {
    if bucket.is_empty() || prefix.is_empty() {
        return "s3://".to_string();
    }

    let mut parent_prefix = prefix.to_string();
    if parent_prefix.ends_with('/') {
        parent_prefix.pop();
    }

    let parent_prefix = match parent_prefix.rfind('/') {
        Some(pos) => &parent_prefix[..pos + 1],
        None => "",
    };

    build_s3_path(bucket, parent_prefix)
}

fn render_copy_path_context_menu(ui: &Ui, path: &str) {
    if let Some(_popup) = ui.begin_popup_context_item() {
        render_copy_path_action(ui, path);
    }
}

fn render_browser_background_context_menu(ui: &Ui, path: &str) {
    if ui.is_window_hovered()
        && !ui.is_any_item_hovered()
        && ui.is_mouse_released(MouseButton::Right)
    {
        ui.open_popup("BrowserBackgroundContext");
    }

    if let Some(_popup) = ui.begin_popup("BrowserBackgroundContext") {
        render_copy_path_action(ui, path);
    }
}

fn render_copy_path_action(ui: &Ui, path: &str) {
    if ui.selectable("Copy path") {
        copy_text_to_clipboard(path);
        ui.close_current_popup();
    }
}

fn is_empty_complete_preview(bytes_written: u64, is_complete: bool) -> bool {
    is_complete && bytes_written == 0
}

fn shortcut_mod_active(io: &Io) -> bool {
    if io.config_macosx_behaviors() {
        io.key_super()
    } else {
        io.key_ctrl()
    }
}

#[cfg(test)]
mod tests {
    use super::{build_s3_path, is_empty_complete_preview, parent_s3_path};

    #[test]
    fn complete_zero_byte_preview_uses_empty_state() {
        assert!(is_empty_complete_preview(0, true));
        assert!(!is_empty_complete_preview(0, false));
        assert!(!is_empty_complete_preview(1, true));
    }

    #[test]
    fn build_s3_path_formats_root_bucket_and_nested_prefixes() {
        assert_eq!(build_s3_path("", ""), "s3://");
        assert_eq!(build_s3_path("bucket", ""), "s3://bucket/");
        assert_eq!(
            build_s3_path("bucket", "folder/file.txt"),
            "s3://bucket/folder/file.txt"
        );
    }

    #[test]
    fn parent_s3_path_returns_parent_folder_or_root() {
        assert_eq!(parent_s3_path("", ""), "s3://");
        assert_eq!(parent_s3_path("bucket", ""), "s3://");
        assert_eq!(parent_s3_path("bucket", "folder/"), "s3://bucket/");
        assert_eq!(
            parent_s3_path("bucket", "folder/nested/"),
            "s3://bucket/folder/"
        );
    }
}
