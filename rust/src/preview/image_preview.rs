use super::renderer::{PreviewContext, PreviewRenderer};
use image::GenericImageView;

pub struct ImagePreviewRenderer {
    current_key: String,
    // TODO: wgpu texture ID for imgui rendering
}

impl ImagePreviewRenderer {
    pub fn new() -> Self {
        Self {
            current_key: String::new(),
        }
    }

    fn is_image_extension(key: &str) -> bool {
        let lower = key.to_lowercase();
        lower.ends_with(".png")
            || lower.ends_with(".jpg")
            || lower.ends_with(".jpeg")
            || lower.ends_with(".gif")
            || lower.ends_with(".bmp")
    }
}

impl PreviewRenderer for ImagePreviewRenderer {
    fn can_handle(&self, key: &str) -> bool {
        Self::is_image_extension(key)
    }

    fn render(&mut self, ui: &dear_imgui_rs::Ui, ctx: &PreviewContext<'_>) {
        if self.current_key != ctx.key {
            self.current_key = ctx.key.to_string();
            // TODO: decode image and create wgpu texture
        }

        ui.text(format!("Image: {}", ctx.filename));
        ui.separator();

        let content = ctx.model.preview_content();
        if content.is_empty() {
            ui.text("Loading image...");
            return;
        }

        // Try to decode image to get dimensions
        match image::load_from_memory(content) {
            Ok(img) => {
                let (w, h) = img.dimensions();
                ui.text(format!("{}x{} pixels", w, h));
                // TODO: render actual image texture
                ui.text("(Image rendering not yet implemented)");
            }
            Err(e) => {
                ui.text(format!("Failed to decode image: {}", e));
            }
        }
    }

    fn reset(&mut self) {
        self.current_key.clear();
    }
}
