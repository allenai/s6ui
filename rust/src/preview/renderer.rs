use crate::model::BrowserModel;
use crate::streaming_preview::StreamingFilePreview;
use std::sync::Arc;

/// Context passed to preview renderers.
pub struct PreviewContext<'a> {
    pub model: &'a BrowserModel,
    pub bucket: &'a str,
    pub key: &'a str,
    pub filename: &'a str,
    pub streaming_preview: Option<&'a Arc<StreamingFilePreview>>,
    pub available_width: f32,
    pub available_height: f32,
}

impl<'a> PreviewContext<'a> {
    /// Get a reference to the streaming preview if available.
    pub fn streaming(&self) -> Option<&StreamingFilePreview> {
        self.streaming_preview.map(|arc| arc.as_ref())
    }
}

/// Trait for preview renderers.
pub trait PreviewRenderer {
    /// Check if this renderer can handle the given key.
    fn can_handle(&self, key: &str) -> bool;

    /// Render the preview.
    fn render(&mut self, ui: &dear_imgui_rs::Ui, ctx: &PreviewContext<'_>);

    /// Reset the renderer state.
    fn reset(&mut self);

    /// Check if this renderer wants to fall back to another.
    fn wants_fallback(&self, _bucket: &str, _key: &str) -> bool {
        false
    }
}
