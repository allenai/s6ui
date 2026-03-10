use crate::model::{PreviewNode, PreviewStatus};
use crate::preview::StreamingFilePreview;
use crate::text_viewer::MmapTextViewer;
use dear_imgui_rs::*;
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq, Eq)]
enum JsonlViewMode {
    Parsed,
    Raw,
    RawWrapped,
}

impl JsonlViewMode {
    fn uses_raw_viewer(self) -> bool {
        matches!(self, Self::Raw | Self::RawWrapped)
    }

    fn raw_wrap_enabled(self) -> bool {
        matches!(self, Self::RawWrapped)
    }
}

pub struct JsonlPreviewer {
    current_key: Option<String>,
    current_source_id: Option<usize>,
    current_line: usize,
    view_mode: JsonlViewMode,
    validated_first_line: bool,
    fallback_key: Option<String>,
    raw_viewer: MmapTextViewer,
    raw_source_id: Option<usize>,
    raw_viewer_line: Option<usize>,
    formatted_viewer: MmapTextViewer,
    formatted_source: Option<Arc<StreamingFilePreview>>,
    text_viewer: MmapTextViewer,
    text_source: Option<Arc<StreamingFilePreview>>,
    cached_line_index: Option<usize>,
    cached_line_content: String,
    text_field_name: Option<String>,
}

impl JsonlPreviewer {
    pub fn new() -> Self {
        Self {
            current_key: None,
            current_source_id: None,
            current_line: 0,
            view_mode: JsonlViewMode::Parsed,
            validated_first_line: false,
            fallback_key: None,
            raw_viewer: MmapTextViewer::new(),
            raw_source_id: None,
            raw_viewer_line: None,
            formatted_viewer: MmapTextViewer::new(),
            formatted_source: None,
            text_viewer: MmapTextViewer::new(),
            text_source: None,
            cached_line_index: None,
            cached_line_content: String::new(),
            text_field_name: None,
        }
    }

    pub fn can_handle(key: &str) -> bool {
        let dot_pos = match key.rfind('.') {
            Some(pos) => pos,
            None => return false,
        };

        let mut ext = key[dot_pos..].to_lowercase();
        if matches!(ext.as_str(), ".gz" | ".zst" | ".zstd") && dot_pos > 0 {
            let inner = &key[..dot_pos];
            let Some(inner_dot) = inner.rfind('.') else {
                return false;
            };
            ext = inner[inner_dot..].to_lowercase();
        }

        matches!(ext.as_str(), ".json" | ".jsonl" | ".ndjson")
    }

    pub fn is_fallback_for(&self, bucket: &str, key: &str) -> bool {
        let full_key = Self::full_key(bucket, key);
        self.fallback_key.as_deref() == Some(full_key.as_str())
    }

    pub fn render(
        &mut self,
        ui: &Ui,
        node: &PreviewNode,
        filename: &str,
        width: f32,
        _height: f32,
    ) -> bool {
        let full_key = Self::full_key(&node.bucket, &node.key);
        self.reset_for_new_file(&full_key, Self::source_id(&node.preview));

        if self.is_fallback_for(&node.bucket, &node.key) {
            return false;
        }

        if !self.validated_first_line
            && node.preview.bytes_written() > 0
            && node.preview.is_line_complete(0)
        {
            self.validated_first_line = true;
            if !Self::is_valid_json_line(&node.preview.read_line(0)) {
                self.fallback_key = Some(full_key);
                return false;
            }
        }

        ui.text(format!("Preview: {}", filename));
        self.render_status(ui, node);
        ui.separator();

        if node.preview.bytes_written() == 0 {
            match node.status() {
                PreviewStatus::Loading => {
                    ui.text_colored([0.5, 0.5, 1.0, 1.0], "Loading...");
                }
                _ => {
                    ui.text_disabled("Empty file");
                }
            }
            return true;
        }

        let line_count = node.preview.line_count();
        if line_count == 0 {
            ui.text_disabled("No lines loaded yet...");
            return true;
        }
        if self.current_line >= line_count {
            self.current_line = line_count.saturating_sub(1);
        }

        self.render_controls(ui, line_count);
        ui.separator();

        let line_complete = node.preview.is_line_complete(self.current_line);
        let line_content = node.preview.read_line(self.current_line);

        if self.view_mode.uses_raw_viewer() {
            self.render_raw_view(ui, node, width);
            return true;
        }

        if line_content.is_empty() {
            if line_complete {
                ui.text_disabled("(empty line)");
            } else {
                ui.text_colored([0.5, 0.5, 1.0, 1.0], "Line incomplete...");
            }
            return true;
        }

        if !line_complete {
            ui.text_colored(
                [0.5, 0.5, 1.0, 1.0],
                format!(
                    "Line incomplete ({} bytes loaded so far)...",
                    line_content.len()
                ),
            );
            ui.spacing();
            self.render_partial_line(ui, &line_content);
            return true;
        }

        if self.cached_line_index != Some(self.current_line)
            || self.cached_line_content != line_content
        {
            self.rebuild_cached_line(self.current_line, &line_content);
        }

        self.render_parsed_view(ui, width);
        true
    }

    fn reset_for_new_file(&mut self, full_key: &str, source_id: usize) {
        if self.current_key.as_deref() == Some(full_key)
            && self.current_source_id == Some(source_id)
        {
            return;
        }

        self.current_key = Some(full_key.to_string());
        self.current_source_id = Some(source_id);
        self.current_line = 0;
        self.view_mode = JsonlViewMode::Parsed;
        self.validated_first_line = false;

        self.raw_viewer.close();
        self.raw_source_id = None;
        self.raw_viewer_line = None;

        self.formatted_viewer.close();
        self.formatted_source = None;
        self.text_viewer.close();
        self.text_source = None;

        self.cached_line_index = None;
        self.cached_line_content.clear();
        self.text_field_name = None;
    }

    fn render_status(&self, ui: &Ui, node: &PreviewNode) {
        match node.status() {
            PreviewStatus::Loading => {
                ui.same_line();
                let bytes = node.bytes_written();
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
                ui.text_colored(
                    [0.5, 0.5, 0.5, 1.0],
                    format!(
                        " ({}, {} lines)",
                        format_size(node.bytes_written() as i64),
                        format_number(node.preview.line_count() as i64)
                    ),
                );
            }
            _ => {}
        }
    }

    fn render_controls(&mut self, ui: &Ui, line_count: usize) {
        let _group = ui.begin_group();

        {
            let _disabled = ui.begin_disabled_with_cond(self.current_line == 0);
            if ui.button("<##jsonl_prev") {
                self.navigate(-1, line_count);
            }
        }
        ui.same_line();

        ui.text(format!("Line {} / {}", self.current_line + 1, line_count));
        ui.same_line();

        {
            let _disabled = ui.begin_disabled_with_cond(self.current_line + 1 >= line_count);
            if ui.button(">##jsonl_next") {
                self.navigate(1, line_count);
            }
        }
        ui.same_line();

        if ui.radio_button("Parsed", self.view_mode == JsonlViewMode::Parsed) {
            self.view_mode = JsonlViewMode::Parsed;
        }
        ui.same_line();
        if ui.radio_button("Wrapped", self.view_mode == JsonlViewMode::RawWrapped) {
            self.view_mode = JsonlViewMode::RawWrapped;
        }
        ui.same_line();
        if ui.radio_button("Raw", self.view_mode == JsonlViewMode::Raw) {
            self.view_mode = JsonlViewMode::Raw;
        }

        let allow_arrow_nav = (ui.is_window_hovered_with_flags(HoveredFlags::CHILD_WINDOWS)
            || ui.is_window_focused())
            && !ui.io().want_text_input();
        if allow_arrow_nav {
            if ui.is_key_pressed(Key::LeftArrow) {
                self.navigate(-1, line_count);
            }
            if ui.is_key_pressed(Key::RightArrow) {
                self.navigate(1, line_count);
            }
        }
    }

    fn render_raw_view(&mut self, ui: &Ui, node: &PreviewNode, width: f32) {
        let source_id = Self::source_id(&node.preview);
        if self.raw_source_id != Some(source_id) {
            self.raw_viewer.close();
            self.raw_viewer.open(node.preview.clone());
            self.raw_source_id = Some(source_id);
            self.raw_viewer_line = None;
        }

        self.raw_viewer.refresh();
        self.raw_viewer
            .set_word_wrap(self.view_mode.raw_wrap_enabled());

        if self.raw_viewer_line != Some(self.current_line) {
            self.raw_viewer.scroll_to_line(self.current_line as u64);
            self.raw_viewer_line = Some(self.current_line);
        }

        if self.raw_viewer.file_size() == 0 || !self.raw_viewer.is_open() {
            ui.text_colored([0.5, 0.5, 1.0, 1.0], "Loading...");
            return;
        }

        let content_height = ui.content_region_avail()[1];
        self.raw_viewer.render(ui, width, content_height);
    }

    fn render_partial_line(&self, ui: &Ui, line_content: &str) {
        let display = if line_content.len() > 4096 {
            format!(
                "{}\n...\n{}",
                &line_content[..2048],
                &line_content[line_content.len() - 2048..]
            )
        } else {
            line_content.to_string()
        };

        ui.child_window("JsonlIncompletePane")
            .size([ui.content_region_avail()[0], ui.content_region_avail()[1]])
            .border(true)
            .build(ui, || {
                ui.text_wrapped(display);
            });
    }

    fn render_parsed_view(&mut self, ui: &Ui, width: f32) {
        let content_height = ui.content_region_avail()[1];

        if self.formatted_viewer.file_size() == 0 || !self.formatted_viewer.is_open() {
            ui.text_colored([0.5, 0.5, 1.0, 1.0], "Loading...");
            return;
        }

        if self.text_source.is_some() {
            let json_height = (content_height * 0.55).max(80.0);
            let text_height =
                (content_height - json_height - ui.frame_height_with_spacing()).max(80.0);

            ui.child_window("JsonlPrettyPane")
                .size([width, json_height])
                .border(true)
                .build(ui, || {
                    self.formatted_viewer.render(ui, width, json_height);
                });

            ui.spacing();
            if let Some(name) = &self.text_field_name {
                ui.text_disabled(format!(".{}", name));
            }

            ui.child_window("JsonlTextPane")
                .size([width, text_height])
                .border(true)
                .build(ui, || {
                    self.text_viewer.render(ui, width, text_height);
                });
            return;
        }

        self.formatted_viewer.render(ui, width, content_height);
    }

    fn rebuild_cached_line(&mut self, line_index: usize, line_content: &str) {
        let (formatted, text_field) = match serde_json::from_str::<Value>(line_content) {
            Ok(value) => {
                let formatted = serde_json::to_string_pretty(&value).unwrap_or_else(|err| {
                    format!("(Error formatting JSON: {})\n\n{}", err, line_content)
                });
                (formatted, Self::extract_text_field(&value))
            }
            Err(err) => (format!("(Invalid JSON: {})\n\n{}", err, line_content), None),
        };

        self.cached_line_index = Some(line_index);
        self.cached_line_content = line_content.to_string();

        self.formatted_source = Self::build_text_source(&formatted);
        self.formatted_viewer.close();
        if let Some(source) = &self.formatted_source {
            self.formatted_viewer.open(source.clone());
        }

        self.text_viewer.close();
        self.text_source = None;
        self.text_field_name = None;

        if let Some((field_name, text)) = text_field {
            self.text_field_name = Some(field_name);
            self.text_source = Self::build_text_source(&text);
            if let Some(source) = &self.text_source {
                self.text_viewer.open(source.clone());
                self.text_viewer.set_word_wrap(true);
            }
        }
    }

    fn build_text_source(text: &str) -> Option<Arc<StreamingFilePreview>> {
        StreamingFilePreview::from_text(text).ok().map(Arc::new)
    }

    fn extract_text_field(value: &Value) -> Option<(String, String)> {
        let Value::Object(map) = value else {
            return None;
        };

        match map.get("text") {
            Some(Value::String(text)) => Some(("text".to_string(), text.clone())),
            _ => None,
        }
    }

    fn is_valid_json_line(line: &str) -> bool {
        let trimmed = line.trim_start();
        let Some(first) = trimmed.as_bytes().first() else {
            return false;
        };
        if *first != b'{' && *first != b'[' {
            return false;
        }

        serde_json::from_str::<Value>(trimmed).is_ok()
    }

    fn navigate(&mut self, delta: isize, line_count: usize) {
        if line_count == 0 {
            self.current_line = 0;
            return;
        }

        if delta < 0 {
            self.current_line = self.current_line.saturating_sub(delta.unsigned_abs());
        } else {
            self.current_line = (self.current_line + delta as usize).min(line_count - 1);
        }
    }

    fn full_key(bucket: &str, key: &str) -> String {
        format!("{}/{}", bucket, key)
    }

    fn source_id(source: &Arc<StreamingFilePreview>) -> usize {
        Arc::as_ptr(source) as usize
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
