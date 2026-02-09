use super::renderer::{PreviewContext, PreviewRenderer};
use std::collections::HashSet;

pub struct JsonlPreviewRenderer {
    current_key: String,
    current_line: usize,
    not_jsonl: HashSet<String>,
}

impl JsonlPreviewRenderer {
    pub fn new() -> Self {
        Self {
            current_key: String::new(),
            current_line: 0,
            not_jsonl: HashSet::new(),
        }
    }

    fn is_jsonl_extension(key: &str) -> bool {
        let lower = key.to_lowercase();
        lower.ends_with(".jsonl")
            || lower.ends_with(".ndjson")
            || lower.ends_with(".jsonl.gz")
            || lower.ends_with(".jsonl.zst")
            || lower.ends_with(".jsonl.zstd")
            || lower.ends_with(".ndjson.gz")
            || lower.ends_with(".ndjson.zst")
            || lower.ends_with(".ndjson.zstd")
    }
}

impl PreviewRenderer for JsonlPreviewRenderer {
    fn can_handle(&self, key: &str) -> bool {
        Self::is_jsonl_extension(key)
    }

    fn render(&mut self, ui: &dear_imgui_rs::Ui, ctx: &PreviewContext<'_>) {
        if self.current_key != ctx.key {
            self.current_key = ctx.key.to_string();
            self.current_line = 0;
        }

        let sp = match ctx.streaming_preview {
            Some(sp) => sp,
            None => {
                ui.text("Loading...");
                return;
            }
        };

        let line_count = sp.line_count();

        ui.text(format!("JSONL Preview: {}", ctx.filename));
        ui.same_line();
        ui.text(format!(
            "Line {} / {}",
            self.current_line + 1,
            line_count
        ));

        // Navigation buttons
        if ui.button("<") && self.current_line > 0 {
            self.current_line -= 1;
        }
        ui.same_line();
        if ui.button(">") && self.current_line + 1 < line_count {
            self.current_line += 1;
        }

        ui.separator();

        // Get current line and try to pretty-print
        if self.current_line < line_count {
            let raw_line = sp.get_line(self.current_line);

            // Validate it's actually JSON
            match serde_json::from_str::<serde_json::Value>(&raw_line) {
                Ok(val) => {
                    let pretty = serde_json::to_string_pretty(&val).unwrap_or(raw_line);
                    ui.text_wrapped(&pretty);
                }
                Err(_) => {
                    // Not valid JSON - mark this file as not-JSONL
                    let cache_key = format!("{}/{}", ctx.bucket, ctx.key);
                    self.not_jsonl.insert(cache_key);
                    ui.text_wrapped(&raw_line);
                }
            }
        }
    }

    fn reset(&mut self) {
        self.current_key.clear();
        self.current_line = 0;
    }

    fn wants_fallback(&self, bucket: &str, key: &str) -> bool {
        let cache_key = format!("{}/{}", bucket, key);
        self.not_jsonl.contains(&cache_key)
    }
}
