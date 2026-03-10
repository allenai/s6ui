use arboard::Clipboard;
use dear_imgui_rs::ClipboardBackend;

pub struct SystemClipboardBackend {
    clipboard: Option<Clipboard>,
}

impl SystemClipboardBackend {
    pub fn new() -> Self {
        Self {
            clipboard: Clipboard::new().ok(),
        }
    }

    fn ensure_clipboard(&mut self) -> Option<&mut Clipboard> {
        if self.clipboard.is_none() {
            self.clipboard = Clipboard::new().ok();
        }
        self.clipboard.as_mut()
    }
}

impl ClipboardBackend for SystemClipboardBackend {
    fn get(&mut self) -> Option<String> {
        self.ensure_clipboard()?.get_text().ok()
    }

    fn set(&mut self, value: &str) {
        let Some(clipboard) = self.ensure_clipboard() else {
            return;
        };
        let _ = clipboard.set_text(value.to_string());
    }
}

pub fn copy_text_to_clipboard(value: &str) {
    let mut clipboard = SystemClipboardBackend::new();
    clipboard.set(value);
}
