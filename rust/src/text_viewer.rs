//! Memory-mapped text viewer with virtual scrolling, word wrap, and selection.
//!
//! Port of C++ MmapTextViewer for efficient viewing of large text files.

use crate::preview::StreamingFilePreview;
use dear_imgui_rs::*;
use memmap2::Mmap;
use std::collections::HashMap;
use std::sync::Arc;

/// Position within text (line + byte offset within line)
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TextPosition {
    pub line: u64,
    pub byte_offset: u32,
}

impl TextPosition {
    fn new(line: u64, byte_offset: u32) -> Self {
        Self { line, byte_offset }
    }
}

impl PartialOrd for TextPosition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TextPosition {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.line.cmp(&other.line) {
            std::cmp::Ordering::Equal => self.byte_offset.cmp(&other.byte_offset),
            ord => ord,
        }
    }
}

/// Word wrap information for a single line
#[derive(Clone, Debug)]
struct WrapInfo {
    /// Number of visual rows this line occupies
    visual_row_count: u32,
    /// Byte offsets within the line where each visual row starts
    row_start_offsets: Vec<u32>,
}

impl Default for WrapInfo {
    fn default() -> Self {
        Self {
            visual_row_count: 1,
            row_start_offsets: vec![0],
        }
    }
}

/// Cache key for wrap info
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct WrapCacheKey {
    line: u64,
    /// Width in pixels (as integer for hashing)
    width_px: i32,
}

/// Memory-mapped text viewer for StreamingFilePreview
pub struct MmapTextViewer {
    /// Data source
    source: Option<Arc<StreamingFilePreview>>,

    /// Memory map of the temp file
    mmap: Option<Mmap>,

    /// Cached file size (for refresh detection)
    mapped_size: u64,

    /// Cached line offsets from source
    line_offsets: Vec<u64>,

    /// Scroll anchor: which logical line is at the top
    anchor_line: u64,
    /// Which visual sub-row within anchor_line (for word wrap)
    anchor_sub_row: u32,

    /// Word wrap enabled
    word_wrap: bool,
    /// Word wrap cache
    wrap_cache: HashMap<WrapCacheKey, WrapInfo>,
    /// Last wrap width used
    last_wrap_width: f32,

    /// Selection state
    selection_active: bool,
    selection_anchor: TextPosition,
    selection_end: TextPosition,
    mouse_down: bool,

    /// Scrollbar dragging
    scrollbar_dragging: bool,
    scrollbar_drag_start_y: f32,

    /// Average visual rows per line (for scrollbar estimation)
    avg_visual_rows: f32,
    avg_visual_rows_sample_line: u64,

    /// Cached monospace character width (all chars same width in fixed-width font)
    char_width: f32,
}

// Constants
const LINE_NUMBER_GUTTER_WIDTH: f32 = 60.0;
const SCROLLBAR_WIDTH: f32 = 14.0;
const WRAP_CACHE_MAX_SIZE: usize = 4096;
const TEXT_LEFT_PADDING: f32 = 4.0;

impl MmapTextViewer {
    pub fn new() -> Self {
        Self {
            source: None,
            mmap: None,
            mapped_size: 0,
            line_offsets: Vec::new(),
            anchor_line: 0,
            anchor_sub_row: 0,
            word_wrap: false,
            wrap_cache: HashMap::new(),
            last_wrap_width: 0.0,
            selection_active: false,
            selection_anchor: TextPosition::default(),
            selection_end: TextPosition::default(),
            mouse_down: false,
            scrollbar_dragging: false,
            scrollbar_drag_start_y: 0.0,
            avg_visual_rows: 1.0,
            avg_visual_rows_sample_line: 0,
            char_width: 0.0,
        }
    }

    /// Open from a StreamingFilePreview
    pub fn open(&mut self, source: Arc<StreamingFilePreview>) {
        self.close();

        let bytes_written = source.bytes_written();
        self.line_offsets = source.line_offsets();

        if bytes_written > 0 {
            match source.mmap() {
                Ok(mmap) => {
                    self.mmap = Some(mmap);
                    self.mapped_size = bytes_written;
                }
                Err(_) => return,
            }
        }

        self.source = Some(source);
    }

    /// Refresh mapping if source has new data
    pub fn refresh(&mut self) {
        let source = match &self.source {
            Some(s) => s,
            None => return,
        };

        let new_size = source.bytes_written();
        if new_size <= self.mapped_size {
            return;
        }

        // Re-map with new size
        match source.mmap() {
            Ok(mmap) => {
                // Invalidate wrap cache for last line (it may have grown)
                if !self.wrap_cache.is_empty() && !self.line_offsets.is_empty() {
                    let last_line = self.line_offsets.len() as u64 - 1;
                    self.wrap_cache.retain(|k, _| k.line != last_line);
                }

                self.mmap = Some(mmap);
                self.mapped_size = new_size;
                self.line_offsets = source.line_offsets();
            }
            Err(_) => {}
        }
    }

    /// Close the viewer
    pub fn close(&mut self) {
        self.mmap = None;
        self.source = None;
        self.mapped_size = 0;
        self.line_offsets.clear();
        self.anchor_line = 0;
        self.anchor_sub_row = 0;
        self.wrap_cache.clear();
        self.selection_active = false;
        self.mouse_down = false;
        self.scrollbar_dragging = false;
        self.avg_visual_rows = 1.0;
        self.avg_visual_rows_sample_line = 0;
    }

    pub fn is_open(&self) -> bool {
        self.mmap.is_some()
    }

    pub fn file_size(&self) -> u64 {
        self.mapped_size
    }

    pub fn line_count(&self) -> u64 {
        self.line_offsets.len() as u64
    }

    pub fn set_word_wrap(&mut self, enabled: bool) {
        if self.word_wrap != enabled {
            self.word_wrap = enabled;
            self.wrap_cache.clear();
            self.anchor_sub_row = 0;
        }
    }

    pub fn word_wrap(&self) -> bool {
        self.word_wrap
    }

    pub fn scroll_to_top(&mut self) {
        self.anchor_line = 0;
        self.anchor_sub_row = 0;
    }

    pub fn scroll_to_bottom(&mut self) {
        let lc = self.line_count();
        if lc == 0 {
            return;
        }
        self.anchor_line = lc.saturating_sub(100);
        self.anchor_sub_row = 0;
    }

    pub fn scroll_to_line(&mut self, line: u64) {
        let lc = self.line_count();
        self.anchor_line = if line >= lc && lc > 0 { lc - 1 } else { line };
        self.anchor_sub_row = 0;
    }

    /// Scroll to a fraction of the document [0.0, 1.0].
    /// In word wrap mode, resolves to the correct (anchor_line, anchor_sub_row).
    fn scroll_to_fraction(&mut self, fraction: f32) {
        let lc = self.line_count();
        if lc == 0 {
            return;
        }

        let fraction = fraction.clamp(0.0, 1.0);

        if !self.word_wrap {
            self.anchor_line = (fraction * (lc - 1) as f32) as u64;
            self.anchor_sub_row = 0;
            return;
        }

        // Map fraction to a logical line with fractional sub-row position
        let exact_line = fraction * (lc - 1) as f32;
        let target_line = (exact_line as u64).min(lc - 1);
        let line_frac = exact_line - target_line as f32;

        // Compute wrap info for the target line to resolve sub_row
        let wi = self.compute_wrap_info(target_line, self.last_wrap_width);
        let sub_row = (line_frac * wi.visual_row_count as f32) as u32;

        self.anchor_line = target_line;
        self.anchor_sub_row = sub_row.min(wi.visual_row_count.saturating_sub(1));
    }

    /// Get the current scroll position as a fraction [0.0, 1.0] of the document.
    fn scroll_fraction(&self) -> f32 {
        let lc = self.line_count();
        if lc <= 1 {
            return 0.0;
        }

        if !self.word_wrap || self.last_wrap_width <= 0.0 {
            return self.anchor_line as f32 / (lc - 1) as f32;
        }

        // Inverse of scroll_to_fraction: line + sub_row/visual_row_count → fraction
        let wi = self.compute_wrap_info(self.anchor_line, self.last_wrap_width);
        let line_frac = if wi.visual_row_count > 1 {
            self.anchor_sub_row as f32 / wi.visual_row_count as f32
        } else {
            0.0
        };
        let exact_line = self.anchor_line as f32 + line_frac;
        (exact_line / (lc - 1) as f32).clamp(0.0, 1.0)
    }

    /// Get line data (pointer into mmap and length)
    fn get_line_data(&self, line_index: u64) -> Option<&[u8]> {
        let mmap = self.mmap.as_ref()?;
        let lc = self.line_offsets.len();
        if line_index as usize >= lc {
            return None;
        }

        let start = self.line_offsets[line_index as usize] as usize;
        let end = if (line_index as usize) + 1 < lc {
            self.line_offsets[line_index as usize + 1] as usize
        } else {
            self.mapped_size as usize
        };

        if start >= mmap.len() || end > mmap.len() {
            return None;
        }

        let mut slice_end = end;
        // Strip trailing \n and \r
        while slice_end > start {
            let b = mmap[slice_end - 1];
            if b == b'\n' || b == b'\r' {
                slice_end -= 1;
            } else {
                break;
            }
        }

        Some(&mmap[start..slice_end])
    }

    /// Measure the monospace character width from the current font
    fn update_char_width(&mut self, ui: &Ui) {
        let font = ui.current_font();
        let size = ui.current_font_size();
        self.char_width = font.calc_text_size(size, f32::MAX, -1.0, "M")[0];
    }

    /// Compute wrap info for a line using the cached char width table
    fn compute_wrap_info(&self, line_index: u64, wrap_width: f32) -> WrapInfo {
        let mut info = WrapInfo::default();

        let line_data = match self.get_line_data(line_index) {
            Some(d) if !d.is_empty() => d,
            _ => return info,
        };

        let line_len = line_data.len() as u32;
        let mut x = 0.0f32;
        let mut last_break_offset = 0u32;
        let mut i = 0u32;

        while i < line_len {
            let ch = line_data[i as usize];

            if x + self.char_width > wrap_width && x > 0.0 {
                let break_at = if last_break_offset > *info.row_start_offsets.last().unwrap_or(&0) {
                    last_break_offset
                } else {
                    i
                };

                info.row_start_offsets.push(break_at);
                info.visual_row_count += 1;
                x = 0.0;

                if break_at < i {
                    i = break_at;
                    continue;
                }
            }

            // Track potential break points
            if ch == b' ' || ch == b'\t' || ch == b'-' || ch == b'/' || ch == b'\\' || ch == b',' || ch == b';' {
                last_break_offset = i + 1;
            }

            x += self.char_width;
            i += 1;
        }

        info
    }

    /// Scroll by visual rows (positive = down, negative = up)
    fn scroll_by_visual_rows(&mut self, rows: i64) {
        let lc = self.line_count();
        if lc == 0 {
            return;
        }

        if rows > 0 {
            for _ in 0..rows {
                if self.word_wrap {
                    let key = WrapCacheKey {
                        line: self.anchor_line,
                        width_px: self.last_wrap_width as i32,
                    };
                    let wi = self.wrap_cache.get(&key).cloned()
                        .unwrap_or_else(|| self.compute_wrap_info(self.anchor_line, self.last_wrap_width));

                    if self.anchor_sub_row + 1 < wi.visual_row_count {
                        self.anchor_sub_row += 1;
                    } else if self.anchor_line + 1 < lc {
                        self.anchor_line += 1;
                        self.anchor_sub_row = 0;
                    }
                } else if self.anchor_line + 1 < lc {
                    self.anchor_line += 1;
                }
            }
        } else {
            let remaining = (-rows) as u64;
            for _ in 0..remaining {
                if self.word_wrap {
                    if self.anchor_sub_row > 0 {
                        self.anchor_sub_row -= 1;
                    } else if self.anchor_line > 0 {
                        self.anchor_line -= 1;
                        let key = WrapCacheKey {
                            line: self.anchor_line,
                            width_px: self.last_wrap_width as i32,
                        };
                        let wi = self.wrap_cache.get(&key).cloned()
                            .unwrap_or_else(|| self.compute_wrap_info(self.anchor_line, self.last_wrap_width));
                        self.anchor_sub_row = wi.visual_row_count - 1;
                    }
                } else if self.anchor_line > 0 {
                    self.anchor_line -= 1;
                }
            }
        }
    }

    /// Estimate average visual rows per line (for scrollbar)
    fn estimate_avg_visual_rows(&mut self) -> f32 {
        if !self.word_wrap {
            return 1.0;
        }

        let lc = self.line_count();
        if lc == 0 {
            return 1.0;
        }

        let sample_count = std::cmp::min(lc, 1000);
        let mut total_rows = 0.0f32;

        for i in 0..sample_count {
            let idx = (i * lc) / sample_count;
            let wi = self.compute_wrap_info(idx, self.last_wrap_width);
            total_rows += wi.visual_row_count as f32;
        }

        self.avg_visual_rows = total_rows / sample_count as f32;
        self.avg_visual_rows_sample_line = lc;
        self.avg_visual_rows
    }

    /// Get selected text
    pub fn get_selected_text(&self) -> String {
        if !self.selection_active {
            return String::new();
        }

        let (start, end) = if self.selection_end < self.selection_anchor {
            (self.selection_end, self.selection_anchor)
        } else {
            (self.selection_anchor, self.selection_end)
        };

        let mut result = String::new();

        if start.line == end.line {
            if let Some(line_data) = self.get_line_data(start.line) {
                let s = start.byte_offset as usize;
                let e = std::cmp::min(end.byte_offset as usize, line_data.len());
                if e > s {
                    result.push_str(&String::from_utf8_lossy(&line_data[s..e]));
                }
            }
            return result;
        }

        // First line
        if let Some(line_data) = self.get_line_data(start.line) {
            let s = start.byte_offset as usize;
            if s < line_data.len() {
                result.push_str(&String::from_utf8_lossy(&line_data[s..]));
            }
            result.push('\n');
        }

        // Middle lines
        for line in (start.line + 1)..end.line {
            if let Some(line_data) = self.get_line_data(line) {
                result.push_str(&String::from_utf8_lossy(line_data));
            }
            result.push('\n');
        }

        // Last line
        if let Some(line_data) = self.get_line_data(end.line) {
            let e = std::cmp::min(end.byte_offset as usize, line_data.len());
            if e > 0 {
                result.push_str(&String::from_utf8_lossy(&line_data[..e]));
            }
        }

        result
    }

    /// Check if there's an active selection
    pub fn has_selection(&self) -> bool {
        self.selection_active
    }

    /// Hit test: convert mouse position to TextPosition
    fn hit_test(&self, _ui: &Ui, mouse_x: f32, mouse_y: f32, start_y: f32, text_x: f32, line_height: f32) -> TextPosition {
        let lc = self.line_count();
        if lc == 0 {
            return TextPosition::default();
        }

        let rel_y = (mouse_y - start_y).max(0.0);
        let visual_row = (rel_y / line_height) as u32;

        let mut cur_line = self.anchor_line;
        let mut cur_sub_row = self.anchor_sub_row;
        let mut rows_to_skip = visual_row;

        let text_area_width = self.last_wrap_width;

        while rows_to_skip > 0 && cur_line < lc {
            if self.word_wrap && text_area_width > 0.0 {
                let key = WrapCacheKey {
                    line: cur_line,
                    width_px: text_area_width as i32,
                };
                let total_rows = self.wrap_cache.get(&key)
                    .map(|wi| wi.visual_row_count)
                    .unwrap_or(1);

                let remaining_in_line = total_rows - cur_sub_row;
                if rows_to_skip < remaining_in_line {
                    cur_sub_row += rows_to_skip;
                    rows_to_skip = 0;
                } else {
                    rows_to_skip -= remaining_in_line;
                    cur_line += 1;
                    cur_sub_row = 0;
                }
            } else {
                cur_line += 1;
                rows_to_skip -= 1;
            }
        }

        if cur_line >= lc {
            cur_line = lc - 1;
            cur_sub_row = 0;
        }

        let line_data = match self.get_line_data(cur_line) {
            Some(d) => d,
            None => return TextPosition::new(cur_line, 0),
        };

        if line_data.is_empty() {
            return TextPosition::new(cur_line, 0);
        }

        let line_len = line_data.len() as u32;
        let (row_start, row_end) = if self.word_wrap && text_area_width > 0.0 {
            let key = WrapCacheKey {
                line: cur_line,
                width_px: text_area_width as i32,
            };
            if let Some(wi) = self.wrap_cache.get(&key) {
                if (cur_sub_row as usize) < wi.row_start_offsets.len() {
                    let rs = wi.row_start_offsets[cur_sub_row as usize];
                    let re = wi.row_start_offsets.get(cur_sub_row as usize + 1).copied().unwrap_or(line_len);
                    (rs, re)
                } else {
                    (0, line_len)
                }
            } else {
                (0, line_len)
            }
        } else {
            (0, line_len)
        };

        let target_x = mouse_x - text_x;
        if target_x < 0.0 {
            return TextPosition::new(cur_line, row_start);
        }

        // With monospace font, character index is simply target_x / char_width
        let char_offset = if self.char_width > 0.0 {
            (target_x / self.char_width + 0.5) as u32
        } else {
            0
        };
        let byte_pos = row_start + char_offset;
        if byte_pos >= row_end {
            return TextPosition::new(cur_line, row_end);
        }
        TextPosition::new(cur_line, byte_pos)
    }

    /// Render the text viewer
    pub fn render(&mut self, ui: &Ui, width: f32, height: f32) {
        let _id = ui.push_id("MmapTextViewer");

        if !self.is_open() {
            ui.text_colored([0.5, 0.5, 0.5, 1.0], "(no file open)");
            return;
        }

        if self.mapped_size == 0 {
            ui.text_colored([0.5, 0.5, 0.5, 1.0], "(empty file)");
            return;
        }

        let lc = self.line_count();
        if lc == 0 {
            return;
        }

        // Clamp anchor
        if self.anchor_line >= lc {
            self.anchor_line = lc - 1;
        }

        // Ensure char width is measured
        if self.char_width == 0.0 {
            self.update_char_width(ui);
        }

        let line_height = ui.text_line_height_with_spacing();
        let text_area_width = width - LINE_NUMBER_GUTTER_WIDTH - SCROLLBAR_WIDTH - TEXT_LEFT_PADDING;
        self.last_wrap_width = text_area_width;

        // Clear wrap cache if width changed significantly
        if !self.wrap_cache.is_empty() {
            let sample_key = *self.wrap_cache.keys().next().unwrap();
            if (sample_key.width_px as f32 - text_area_width).abs() > 1.0 {
                self.wrap_cache.clear();
            }
        }

        // Handle input
        let window_pos = ui.cursor_screen_pos();
        let mouse_pos = ui.io().mouse_pos();

        // Create invisible button to capture input
        if ui.invisible_button("##viewer_input", [width, height]) {
            // Click handled below
        }
        let viewer_focused = ui.is_item_focused();

        let mouse_in_area = mouse_pos[0] >= window_pos[0]
            && mouse_pos[0] < window_pos[0] + width
            && mouse_pos[1] >= window_pos[1]
            && mouse_pos[1] < window_pos[1] + height;

        let mouse_in_text_area = mouse_pos[0] >= window_pos[0]
            && mouse_pos[0] < window_pos[0] + width - SCROLLBAR_WIDTH
            && mouse_pos[1] >= window_pos[1]
            && mouse_pos[1] < window_pos[1] + height;

        // Mouse wheel scrolling
        if (mouse_in_area || self.scrollbar_dragging) && !ui.is_any_item_active() {
            let wheel = ui.io().mouse_wheel();
            if wheel != 0.0 {
                let rows = (-wheel * 3.0) as i64;
                self.scroll_by_visual_rows(rows);
            }
        }

        // Keyboard navigation
        if viewer_focused {
            if ui.is_key_pressed(Key::DownArrow) {
                self.scroll_by_visual_rows(1);
            }
            if ui.is_key_pressed(Key::UpArrow) {
                self.scroll_by_visual_rows(-1);
            }

            let visible_rows = (height / line_height) as i64;
            if ui.is_key_pressed(Key::PageDown) {
                self.scroll_by_visual_rows(visible_rows);
            }
            if ui.is_key_pressed(Key::PageUp) {
                self.scroll_by_visual_rows(-visible_rows);
            }

            if ui.is_key_pressed(Key::Home) {
                self.scroll_to_top();
            }
            if ui.is_key_pressed(Key::End) {
                self.scroll_to_bottom();
            }
        }

        // Context menu
        if mouse_in_text_area && ui.is_mouse_clicked(MouseButton::Right) {
            ui.open_popup("##textviewer_ctx");
        }
        if let Some(_popup) = ui.begin_popup("##textviewer_ctx") {
            // Note: clipboard copy would require platform-specific integration
            // dear-imgui-rs 0.8 doesn't expose set_clipboard_text
            if ui.menu_item("Copy (Cmd+C)") {
                // User needs to use keyboard shortcut for now
            }
        }

        // Rendering
        let draw_list = ui.get_window_draw_list();
        let start_x = window_pos[0];
        let start_y = window_pos[1];
        let text_x = start_x + LINE_NUMBER_GUTTER_WIDTH + TEXT_LEFT_PADDING;

        // Mouse selection handling
        if mouse_in_text_area && ui.is_mouse_clicked(MouseButton::Left) && !self.scrollbar_dragging {
            let pos = self.hit_test(ui, mouse_pos[0], mouse_pos[1], start_y, text_x, line_height);
            self.selection_anchor = pos;
            self.selection_end = pos;
            self.mouse_down = true;
            self.selection_active = false;
        }

        if self.mouse_down {
            if ui.is_mouse_down(MouseButton::Left) {
                let pos = self.hit_test(ui, mouse_pos[0], mouse_pos[1], start_y, text_x, line_height);
                self.selection_end = pos;
                if self.selection_anchor != self.selection_end {
                    self.selection_active = true;
                }
            } else {
                self.mouse_down = false;
            }
        }

        // Normalize selection for rendering
        let (sel_start, sel_end) = if self.selection_active {
            if self.selection_end < self.selection_anchor {
                (self.selection_end, self.selection_anchor)
            } else {
                (self.selection_anchor, self.selection_end)
            }
        } else {
            (TextPosition::default(), TextPosition::default())
        };

        // Background
        draw_list.add_rect(
            [start_x, start_y],
            [start_x + width, start_y + height],
            [0.08, 0.08, 0.08, 1.0],
        ).filled(true).build();

        // Gutter background
        draw_list.add_rect(
            [start_x, start_y],
            [start_x + LINE_NUMBER_GUTTER_WIDTH, start_y + height],
            [0.12, 0.12, 0.12, 1.0],
        ).filled(true).build();

        // Selection color
        let sel_color: [f32; 4] = [0.24, 0.40, 0.70, 0.5];
        // Text colors
        let text_color: [f32; 4] = [0.86, 0.86, 0.86, 1.0];
        let gutter_color: [f32; 4] = [0.47, 0.47, 0.47, 1.0];

        // Clip text to exclude scrollbar area
        draw_list.push_clip_rect(
            [start_x, start_y],
            [start_x + width - SCROLLBAR_WIDTH, start_y + height],
            true,
        );

        // Render lines
        let mut cursor_y = start_y;
        let mut current_line = self.anchor_line;
        let mut current_sub_row = self.anchor_sub_row;

        // Max bytes to copy/display in no-wrap mode (enough to fill visible width)
        let max_nowrap_bytes = if self.char_width > 0.0 {
            ((text_area_width / self.char_width) * 1.5) as usize + 16
        } else {
            1024
        };

        while cursor_y < start_y + height && current_line < lc {
            // Copy line data to avoid borrow conflicts with wrap_cache.
            // In no-wrap mode, truncate to visible width to avoid copying/rendering huge lines.
            let line_data_vec: Option<Vec<u8>> = if self.word_wrap {
                self.get_line_data(current_line).map(|d| d.to_vec())
            } else {
                self.get_line_data(current_line)
                    .map(|d| d[..std::cmp::min(d.len(), max_nowrap_bytes)].to_vec())
            };
            let line_data = line_data_vec.as_deref();

            if self.word_wrap && text_area_width > 0.0 {
                let key = WrapCacheKey {
                    line: current_line,
                    width_px: text_area_width as i32,
                };
                let wi = if let Some(cached) = self.wrap_cache.get(&key) {
                    cached.clone()
                } else {
                    let computed = self.compute_wrap_info(current_line, text_area_width);
                    if self.wrap_cache.len() >= WRAP_CACHE_MAX_SIZE {
                        self.wrap_cache.clear();
                    }
                    self.wrap_cache.insert(key, computed.clone());
                    computed
                };

                for row in current_sub_row..wi.visual_row_count {
                    if cursor_y >= start_y + height {
                        break;
                    }

                    // Line number (only on first row)
                    if row == 0 {
                        let line_num = format!("{}", current_line + 1);
                        let num_width = line_num.len() as f32 * self.char_width;
                        draw_list.add_text(
                            [start_x + LINE_NUMBER_GUTTER_WIDTH - num_width - 8.0, cursor_y],
                            gutter_color,
                            &line_num,
                        );
                    }

                    if let Some(data) = line_data {
                        let row_start = wi.row_start_offsets[row as usize] as usize;
                        let row_end = wi.row_start_offsets.get(row as usize + 1)
                            .map(|&v| v as usize)
                            .unwrap_or(data.len());

                        // Draw selection highlight
                        if self.selection_active {
                            let row_begin = TextPosition::new(current_line, row_start as u32);
                            let row_end_pos = TextPosition::new(current_line, row_end as u32);
                            if !(sel_end < row_begin || row_end_pos < sel_start) {
                                let hl_start = if sel_start.line == current_line && sel_start.byte_offset as usize > row_start {
                                    sel_start.byte_offset as usize
                                } else {
                                    row_start
                                };
                                let hl_end = if sel_end.line == current_line && (sel_end.byte_offset as usize) < row_end {
                                    sel_end.byte_offset as usize
                                } else {
                                    row_end
                                };
                                if hl_end > hl_start {
                                    let x0 = text_x + self.compute_text_width_slice(data, row_start, hl_start);
                                    let x1 = text_x + self.compute_text_width_slice(data, row_start, hl_end);
                                    draw_list.add_rect(
                                        [x0, cursor_y],
                                        [x1, cursor_y + line_height],
                                        sel_color,
                                    ).filled(true).build();
                                }
                            }
                        }

                        if row_end > row_start {
                            let text = String::from_utf8_lossy(&data[row_start..row_end]);
                            draw_list.add_text([text_x, cursor_y], text_color, text.as_ref());
                        }
                    }

                    cursor_y += line_height;
                }
                current_sub_row = 0;
            } else {
                // No word wrap
                let line_num = format!("{}", current_line + 1);
                let num_width = line_num.len() as f32 * self.char_width;
                draw_list.add_text(
                    [start_x + LINE_NUMBER_GUTTER_WIDTH - num_width - 8.0, cursor_y],
                    gutter_color,
                    &line_num,
                );

                if let Some(data) = line_data {
                    let display_len = data.len();

                    // Draw selection highlight
                    if self.selection_active {
                        let row_begin = TextPosition::new(current_line, 0);
                        let row_end_pos = TextPosition::new(current_line, display_len as u32);
                        if !(sel_end < row_begin || row_end_pos < sel_start) {
                            let hl_start = if sel_start.line == current_line {
                                sel_start.byte_offset as usize
                            } else {
                                0
                            };
                            let hl_end = if sel_end.line == current_line {
                                sel_end.byte_offset as usize
                            } else {
                                display_len
                            };
                            if hl_end > hl_start && hl_start < display_len {
                                let hl_end = std::cmp::min(hl_end, display_len);
                                let x0 = text_x + self.compute_text_width_slice(data, 0, hl_start);
                                let x1 = text_x + self.compute_text_width_slice(data, 0, hl_end);
                                draw_list.add_rect(
                                    [x0, cursor_y],
                                    [x1, cursor_y + line_height],
                                    sel_color,
                                ).filled(true).build();
                            }
                        }
                    }

                    let text = String::from_utf8_lossy(&data[..display_len]);
                    draw_list.add_text([text_x, cursor_y], text_color, text.as_ref());
                }

                cursor_y += line_height;
            }

            current_line += 1;
        }

        draw_list.pop_clip_rect();

        // Scrollbar
        if self.word_wrap && self.avg_visual_rows_sample_line != lc {
            self.estimate_avg_visual_rows();
        }
        self.render_scrollbar(ui, &draw_list, start_x + width - SCROLLBAR_WIDTH, start_y, height, lc as f32, line_height);
    }

    /// Compute text width for a range within line data
    fn compute_text_width_slice(&self, data: &[u8], from: usize, to: usize) -> f32 {
        if to <= from || from >= data.len() {
            return 0.0;
        }
        let len = std::cmp::min(to, data.len()) - from;
        len as f32 * self.char_width
    }

    /// Render the scrollbar
    fn render_scrollbar(&mut self, ui: &Ui, draw_list: &DrawListMut, x: f32, y: f32, height: f32, total_lines: f32, line_height: f32) {
        let avg = if self.word_wrap { self.avg_visual_rows } else { 1.0 };
        let total_visual_rows = total_lines * avg;
        let viewport_rows = height / line_height;

        // Background
        draw_list.add_rect(
            [x, y],
            [x + SCROLLBAR_WIDTH, y + height],
            [0.12, 0.12, 0.12, 1.0],
        ).filled(true).build();

        if total_visual_rows <= viewport_rows {
            return;
        }

        let thumb_ratio = viewport_rows / total_visual_rows;
        let thumb_h = (height * thumb_ratio).max(20.0);
        let cur_fraction = self.scroll_fraction();
        let thumb_y = y + cur_fraction * (height - thumb_h);

        let mouse_pos = ui.io().mouse_pos();
        let mouse_in_scrollbar = mouse_pos[0] >= x
            && mouse_pos[0] <= x + SCROLLBAR_WIDTH
            && mouse_pos[1] >= y
            && mouse_pos[1] <= y + height;

        if mouse_in_scrollbar && ui.is_mouse_clicked(MouseButton::Left) {
            if mouse_pos[1] >= thumb_y && mouse_pos[1] <= thumb_y + thumb_h {
                self.scrollbar_dragging = true;
                self.scrollbar_drag_start_y = mouse_pos[1] - thumb_y;
            } else {
                // Click above/below thumb — jump to that fraction
                let click_fraction = ((mouse_pos[1] - y - thumb_h * 0.5) / (height - thumb_h)).clamp(0.0, 1.0);
                self.scroll_to_fraction(click_fraction);
                self.scrollbar_dragging = true;
                self.scrollbar_drag_start_y = thumb_h * 0.5;
            }
        }

        if self.scrollbar_dragging {
            if ui.is_mouse_down(MouseButton::Left) {
                let new_thumb_y = mouse_pos[1] - self.scrollbar_drag_start_y;
                let new_fraction = ((new_thumb_y - y) / (height - thumb_h)).clamp(0.0, 1.0);
                self.scroll_to_fraction(new_fraction);
            } else {
                self.scrollbar_dragging = false;
            }
        }

        let thumb_color: [f32; 4] = if self.scrollbar_dragging {
            [0.70, 0.70, 0.70, 1.0]
        } else if mouse_in_scrollbar {
            [0.55, 0.55, 0.55, 1.0]
        } else {
            [0.40, 0.40, 0.40, 1.0]
        };

        draw_list.add_rect(
            [x + 2.0, thumb_y],
            [x + SCROLLBAR_WIDTH - 2.0, thumb_y + thumb_h],
            thumb_color,
        )
        .rounding(4.0)
        .filled(true)
        .build();
    }
}

impl Default for MmapTextViewer {
    fn default() -> Self {
        Self::new()
    }
}
