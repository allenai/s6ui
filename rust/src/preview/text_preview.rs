use super::renderer::{PreviewContext, PreviewRenderer};

pub struct TextPreviewRenderer {
    current_key: String,
}

impl TextPreviewRenderer {
    pub fn new() -> Self {
        Self {
            current_key: String::new(),
        }
    }
}

impl PreviewRenderer for TextPreviewRenderer {
    fn can_handle(&self, _key: &str) -> bool {
        true // Text is the fallback renderer
    }

    fn render(&mut self, ui: &dear_imgui_rs::Ui, ctx: &PreviewContext<'_>) {
        if self.current_key != ctx.key {
            self.current_key = ctx.key.to_string();
        }

        ui.text(format!("Preview: {}", ctx.filename));
        ui.separator();

        // Show download progress if streaming
        if let Some(sp) = ctx.streaming_preview {
            let downloaded = sp.bytes_downloaded();
            let total = sp.total_source_bytes();
            if total > 0 && downloaded < total {
                ui.text(format!(
                    "Downloading: {} / {} bytes ({:.0}%)",
                    downloaded,
                    total,
                    (downloaded as f64 / total as f64) * 100.0
                ));
            }

            // Show text content line by line
            let line_count = sp.line_count();
            let clipper = dear_imgui_rs::ListClipper::new(line_count as i32);
            let mut clipper_token = clipper.begin(ui);
            while clipper_token.step() {
                for i in clipper_token.display_start()..clipper_token.display_end() {
                    let line = sp.get_line(i as usize);
                    ui.text(&line);
                }
            }
        } else {
            // Simple text display from preview content
            let content = ctx.model.preview_content();
            let text = String::from_utf8_lossy(content);
            ui.text_wrapped(&*text);
        }
    }

    fn reset(&mut self) {
        self.current_key.clear();
    }
}
