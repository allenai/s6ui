//! Image-WARC gallery viewer.
//!
//! Renders the image records inside a WARC object (`.warc` / `.warc.gz`) as a
//! scrollable thumbnail grid. WARC parsing + image decode + GPU texture upload
//! happen in `sync()` (called once per frame *before* the ImGui frame, where the
//! wgpu device/queue/renderer are reachable); `render()` only draws the already
//! uploaded textures inside the frame.

use crate::model::{BrowserModel, PreviewNode};
use crate::preview::StreamingStatus;
use dear_imgui_rs::Ui;
use dear_imgui_wgpu::WgpuRenderer;
use std::io::Read;

/// Cap on decoded images per WARC (keeps VRAM/CPU bounded for huge batches).
const MAX_IMAGES: usize = 300;
/// Thumbnails are downscaled so the longest edge is at most this many pixels.
const MAX_TEXTURE_EDGE: u32 = 1024;
/// Displayed thumbnail box (logical px).
const THUMB: f32 = 200.0;
/// Grid cell width including padding/caption (logical px).
const CELL: f32 = 224.0;

struct WarcImage {
    texture_id: u64,
    width: u32,
    height: u32,
    url: String,
    folder: String,
    content_type: String,
    bytes: usize,
}

struct Record {
    url: String,
    folder: String,
    content_type: String,
    payload: Vec<u8>,
}

pub struct WarcGallery {
    /// Preview cache key currently decoded (so we decode once per object).
    loaded_key: Option<String>,
    images: Vec<WarcImage>,
    texture_ids: Vec<u64>,
    total_records: usize,
    skipped: usize,
    status: String,
}

impl WarcGallery {
    pub fn new() -> Self {
        Self {
            loaded_key: None,
            images: Vec::new(),
            texture_ids: Vec::new(),
            total_records: 0,
            skipped: 0,
            status: String::new(),
        }
    }

    pub fn can_handle(key: &str) -> bool {
        let k = key.to_lowercase();
        k.ends_with(".warc") || k.ends_with(".warc.gz")
    }

    /// Whether this preview should be shown as an image gallery: either the name
    /// is a WARC, or the downloaded bytes sniff as one (catches extension-less
    /// Firehose batches like `pantry-…-image-1-…`).
    pub fn applies(node: &PreviewNode) -> bool {
        Self::can_handle(&node.key) || content_is_warc(node)
    }

    /// Free all GPU textures and reset state.
    fn clear(&mut self, renderer: &mut WgpuRenderer) {
        for id in self.texture_ids.drain(..) {
            renderer.unregister_texture(id);
        }
        self.images.clear();
        self.loaded_key = None;
        self.total_records = 0;
        self.skipped = 0;
        self.status.clear();
    }

    /// Decode + upload textures for the selected WARC if it changed. Must run
    /// outside the ImGui frame (needs the wgpu device/queue/renderer).
    pub fn sync(
        &mut self,
        model: &BrowserModel,
        device: &wgpu::Device,
        queue: &wgpu::Queue,
        renderer: &mut WgpuRenderer,
    ) {
        let node = model.selected_preview();
        let is_warc = node.map(Self::applies).unwrap_or(false);
        if !is_warc {
            if self.loaded_key.is_some() {
                self.clear(renderer);
            }
            return;
        }
        let node = node.unwrap();
        let key = model.selected_preview.clone().unwrap_or_default();

        // Already decoded this object.
        if self.loaded_key.as_deref() == Some(key.as_str()) {
            return;
        }

        // Wait for the full object before parsing (a partial gzip won't decode).
        if !matches!(node.streaming_status(), StreamingStatus::Complete) {
            if self.loaded_key.is_some() {
                self.clear(renderer);
            }
            self.status = format!("downloading… ({} bytes)", node.bytes_written());
            return;
        }

        // Fresh, complete object: rebuild the gallery.
        self.clear(renderer);
        self.loaded_key = Some(key);

        let raw = match node.preview.mmap() {
            Ok(m) => m,
            Err(e) => {
                self.status = format!("mmap error: {e}");
                return;
            }
        };
        let warc = maybe_gunzip(&raw);
        let records = parse_warc(&warc);
        self.total_records = records.len();

        for rec in records {
            if !rec.content_type.to_lowercase().starts_with("image/") {
                continue;
            }
            if self.images.len() >= MAX_IMAGES {
                self.skipped += 1;
                continue;
            }
            match decode_image(&rec.payload, &rec.content_type) {
                Some((w, h, rgba)) => {
                    if let Some(id) = upload_texture(device, queue, renderer, w, h, &rgba) {
                        self.texture_ids.push(id);
                        self.images.push(WarcImage {
                            texture_id: id,
                            width: w,
                            height: h,
                            url: rec.url,
                            folder: rec.folder,
                            content_type: rec.content_type,
                            bytes: rec.payload.len(),
                        });
                    } else {
                        self.skipped += 1;
                    }
                }
                None => self.skipped += 1,
            }
        }
        self.status = format!("{} image(s)", self.images.len());
    }

    /// Draw the gallery. Called inside the ImGui frame.
    pub fn render(&self, ui: &Ui, width: f32, height: f32) {
        ui.text(format!(
            "WARC image gallery — {} image(s) of {} record(s)",
            self.images.len(),
            self.total_records
        ));
        if self.skipped > 0 {
            ui.same_line();
            ui.text_colored(
                [0.65, 0.65, 0.65, 1.0],
                format!("· {} skipped (non-image / undecodable)", self.skipped),
            );
        }
        ui.separator();

        if self.images.is_empty() {
            let msg = if self.status.is_empty() {
                "(no images)"
            } else {
                self.status.as_str()
            };
            ui.text_colored([0.6, 0.6, 0.6, 1.0], msg);
            return;
        }

        let cols = ((width / CELL).floor() as usize).max(1);
        ui.child_window("WarcGalleryGrid")
            .size([width, (height - 60.0).max(60.0)])
            .build(ui, || {
                for (i, im) in self.images.iter().enumerate() {
                    if i % cols != 0 {
                        ui.same_line();
                    }
                    ui.group(|| {
                        ui.image(im.texture_id, fit(im.width, im.height, THUMB));
                        ui.text_colored(
                            [0.48, 0.86, 1.0, 1.0],
                            format!("{} · {}", im.content_type, human_bytes(im.bytes)),
                        );
                        if !im.folder.is_empty() {
                            ui.text_colored([0.6, 0.8, 0.6, 1.0], format!("📁 {}", im.folder));
                        }
                        ui.text_wrapped(short_url(&im.url));
                    });
                }
            });
    }
}

/// Fit (w,h) into a `box_px` square, preserving aspect ratio.
fn fit(w: u32, h: u32, box_px: f32) -> [f32; 2] {
    if w == 0 || h == 0 {
        return [box_px, box_px];
    }
    let (w, h) = (w as f32, h as f32);
    let s = (box_px / w).min(box_px / h);
    [w * s, h * s]
}

fn human_bytes(n: usize) -> String {
    let n = n as f64;
    if n >= 1_048_576.0 {
        format!("{:.1} MB", n / 1_048_576.0)
    } else if n >= 1024.0 {
        format!("{:.0} KB", n / 1024.0)
    } else {
        format!("{n} B")
    }
}

fn short_url(url: &str) -> String {
    // Show the tail of the path, which is the most identifying part.
    let trimmed = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url);
    if trimmed.len() <= 64 {
        trimmed.to_string()
    } else {
        format!("…{}", &trimmed[trimmed.len() - 63..])
    }
}

/// Sniff whether the (possibly partially) downloaded object is a WARC: it begins
/// with `WARC/`, or is gzip whose first decompressed bytes are `WARC/`.
fn content_is_warc(node: &PreviewNode) -> bool {
    let mmap = match node.preview.mmap() {
        Ok(m) => m,
        Err(_) => return false,
    };
    if mmap.is_empty() {
        return false;
    }
    let head = &mmap[..mmap.len().min(1024)];
    if head.starts_with(b"WARC/") {
        return true;
    }
    if head.len() >= 2 && head[0] == 0x1f && head[1] == 0x8b {
        let mut dec = flate2::read::MultiGzDecoder::new(head);
        let mut buf = [0u8; 8];
        if let Ok(n) = dec.read(&mut buf) {
            return buf[..n].starts_with(b"WARC/");
        }
    }
    false
}

/// gunzip if the bytes are gzip (handles concatenated members); else passthrough.
fn maybe_gunzip(raw: &[u8]) -> Vec<u8> {
    if raw.len() >= 2 && raw[0] == 0x1f && raw[1] == 0x8b {
        let mut out = Vec::new();
        let mut dec = flate2::read::MultiGzDecoder::new(raw);
        let _ = dec.read_to_end(&mut out); // keep whatever decoded on partial error
        out
    } else {
        raw.to_vec()
    }
}

fn find(hay: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    if from >= hay.len() || needle.is_empty() {
        return None;
    }
    hay[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|p| p + from)
}

/// Parse concatenated WARC `resource` records, returning each record's key
/// headers and payload. Sequential parse driven by Content-Length.
fn parse_warc(data: &[u8]) -> Vec<Record> {
    let mut recs = Vec::new();
    let mut pos = 0usize;
    while pos < data.len() {
        // Align to the next record.
        if pos + 5 > data.len() || &data[pos..pos + 5] != b"WARC/" {
            match find(data, pos, b"WARC/") {
                Some(p) => pos = p,
                None => break,
            }
        }
        let hdr_end = match find(data, pos, b"\r\n\r\n") {
            Some(p) => p,
            None => break,
        };
        let header_block = &data[pos..hdr_end];
        let mut url = String::new();
        let mut folder = String::new();
        let mut content_type = String::new();
        let mut content_length: Option<usize> = None;
        for line in header_block.split(|&b| b == b'\n') {
            let line = trim_cr(line);
            if let Some(idx) = line.iter().position(|&b| b == b':') {
                let name = String::from_utf8_lossy(&line[..idx]).trim().to_lowercase();
                let value = String::from_utf8_lossy(&line[idx + 1..]).trim().to_string();
                match name.as_str() {
                    "warc-target-uri" => url = value,
                    "folder" => folder = value,
                    "content-type" => content_type = value,
                    "content-length" => content_length = value.parse::<usize>().ok(),
                    _ => {}
                }
            }
        }
        let body_start = hdr_end + 4;
        let cl = match content_length {
            Some(n) => n,
            None => break,
        };
        let body_end = (body_start + cl).min(data.len());
        recs.push(Record {
            url,
            folder,
            content_type,
            payload: data[body_start..body_end].to_vec(),
        });
        pos = body_end;
    }
    recs
}

fn trim_cr(line: &[u8]) -> &[u8] {
    if line.last() == Some(&b'\r') {
        &line[..line.len() - 1]
    } else {
        line
    }
}

/// Decode an image payload to (width, height, RGBA8). Falls back to SVG
/// rasterization (via resvg) when the raster decoders can't handle it.
fn decode_image(payload: &[u8], content_type: &str) -> Option<(u32, u32, Vec<u8>)> {
    if let Ok(mut img) = image::load_from_memory(payload) {
        if img.width().max(img.height()) > MAX_TEXTURE_EDGE {
            img = img.thumbnail(MAX_TEXTURE_EDGE, MAX_TEXTURE_EDGE);
        }
        let rgba = img.to_rgba8();
        let (w, h) = rgba.dimensions();
        return Some((w, h, rgba.into_raw()));
    }
    if content_type.to_lowercase().contains("svg") || looks_like_svg(payload) {
        return svg_to_rgba(payload);
    }
    None
}

fn looks_like_svg(payload: &[u8]) -> bool {
    // svgz (gzipped svg) — usvg handles the decompression itself.
    if payload.len() >= 2 && payload[0] == 0x1f && payload[1] == 0x8b {
        return true;
    }
    let head = &payload[..payload.len().min(256)];
    let text = String::from_utf8_lossy(head).to_lowercase();
    text.contains("<svg") || text.trim_start().starts_with("<?xml")
}

/// Rasterize an SVG to RGBA8 with straight (un-premultiplied) alpha, scaled so
/// the longest edge is around 512px (clamped to MAX_TEXTURE_EDGE).
fn svg_to_rgba(data: &[u8]) -> Option<(u32, u32, Vec<u8>)> {
    let opt = resvg::usvg::Options::default();
    let tree = resvg::usvg::Tree::from_data(data, &opt).ok()?;
    let size = tree.size();
    let (w, h) = (size.width(), size.height());
    if !(w >= 1.0 && h >= 1.0) {
        return None;
    }
    let target = 512.0_f32;
    let s = (target / w.max(h)).clamp(0.05, 16.0);
    let pw = ((w * s).round() as u32).clamp(1, MAX_TEXTURE_EDGE);
    let ph = ((h * s).round() as u32).clamp(1, MAX_TEXTURE_EDGE);

    let mut pixmap = resvg::tiny_skia::Pixmap::new(pw, ph)?;
    let transform = resvg::tiny_skia::Transform::from_scale(pw as f32 / w, ph as f32 / h);
    resvg::render(&tree, transform, &mut pixmap.as_mut());

    let mut rgba = pixmap.take(); // tiny_skia stores premultiplied alpha
    for px in rgba.chunks_exact_mut(4) {
        let a = px[3] as u32;
        if a > 0 && a < 255 {
            px[0] = ((px[0] as u32 * 255 + a / 2) / a).min(255) as u8;
            px[1] = ((px[1] as u32 * 255 + a / 2) / a).min(255) as u8;
            px[2] = ((px[2] as u32 * 255 + a / 2) / a).min(255) as u8;
        }
    }
    Some((pw, ph, rgba))
}

/// Upload RGBA pixels as a wgpu texture and register it with the ImGui renderer.
fn upload_texture(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    renderer: &mut WgpuRenderer,
    w: u32,
    h: u32,
    rgba: &[u8],
) -> Option<u64> {
    if w == 0 || h == 0 || rgba.len() < (w * h * 4) as usize {
        return None;
    }
    let size = wgpu::Extent3d {
        width: w,
        height: h,
        depth_or_array_layers: 1,
    };
    let texture = device.create_texture(&wgpu::TextureDescriptor {
        label: Some("warc-image"),
        size,
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format: wgpu::TextureFormat::Rgba8Unorm,
        usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
        view_formats: &[],
    });
    queue.write_texture(
        wgpu::TexelCopyTextureInfo {
            texture: &texture,
            mip_level: 0,
            origin: wgpu::Origin3d::ZERO,
            aspect: wgpu::TextureAspect::All,
        },
        rgba,
        wgpu::TexelCopyBufferLayout {
            offset: 0,
            bytes_per_row: Some(w * 4),
            rows_per_image: Some(h),
        },
        size,
    );
    let view = texture.create_view(&wgpu::TextureViewDescriptor::default());
    Some(renderer.register_external_texture(&texture, &view))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Encode a tiny PNG so the test exercises the real image decoder.
    fn tiny_png() -> Vec<u8> {
        let img = image::RgbaImage::from_pixel(2, 3, image::Rgba([10, 20, 30, 255]));
        let mut buf = std::io::Cursor::new(Vec::new());
        image::DynamicImage::ImageRgba8(img)
            .write_to(&mut buf, image::ImageFormat::Png)
            .unwrap();
        buf.into_inner()
    }

    /// Build a single gzipped WARC `resource` record (mirrors Pantry's per-record gzip).
    fn gzipped_warc_record(url: &str, folder: &str, ct: &str, payload: &[u8]) -> Vec<u8> {
        let header = format!(
            "WARC/1.0\r\nWARC-Type: resource\r\nWARC-Target-URI: {url}\r\nContent-Type: {ct}\r\nfolder: {folder}\r\nContent-Length: {}\r\n\r\n",
            payload.len()
        );
        let mut record = Vec::new();
        record.extend_from_slice(header.as_bytes());
        record.extend_from_slice(payload);
        record.extend_from_slice(b"\r\n\r\n");

        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(&record).unwrap();
        enc.finish().unwrap()
    }

    #[test]
    fn parses_single_gzipped_record() {
        let png = tiny_png();
        let obj = gzipped_warc_record("https://e.com/a.png", "site_path", "image/png", &png);

        let warc = maybe_gunzip(&obj);
        let recs = parse_warc(&warc);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].url, "https://e.com/a.png");
        assert_eq!(recs[0].folder, "site_path");
        assert_eq!(recs[0].content_type, "image/png");
        assert_eq!(recs[0].payload, png);

        // And the payload decodes to the expected dimensions.
        let img = image::load_from_memory(&recs[0].payload).unwrap();
        assert_eq!((img.width(), img.height()), (2, 3));
    }

    #[test]
    fn parses_concatenated_records() {
        // Firehose-style: multiple individually-gzipped records concatenated.
        let png = tiny_png();
        let mut obj = gzipped_warc_record("https://e.com/1.png", "f1", "image/png", &png);
        obj.extend(gzipped_warc_record(
            "https://e.com/2.jpg",
            "f2",
            "image/jpeg",
            b"not-a-real-jpeg",
        ));

        let warc = maybe_gunzip(&obj);
        let recs = parse_warc(&warc);
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].url, "https://e.com/1.png");
        assert_eq!(recs[0].payload, png);
        assert_eq!(recs[1].url, "https://e.com/2.jpg");
        assert_eq!(recs[1].content_type, "image/jpeg");
    }

    #[test]
    fn rasterizes_svg() {
        let svg = br##"<svg xmlns="http://www.w3.org/2000/svg" width="8" height="6"><rect width="8" height="6" fill="#3366cc"/></svg>"##;
        assert!(looks_like_svg(svg));
        let (w, h, rgba) = decode_image(svg, "image/svg+xml").expect("svg should rasterize");
        assert!(w >= 8 && h >= 6); // scaled up toward ~512 longest edge
        assert_eq!(rgba.len(), (w * h * 4) as usize);
        // The fill is opaque, so alpha should be 255 somewhere.
        assert!(rgba.chunks_exact(4).any(|p| p[3] == 255));
    }

    #[test]
    fn can_handle_extensions() {
        assert!(WarcGallery::can_handle("x/y.warc"));
        assert!(WarcGallery::can_handle("x/y.warc.gz"));
        assert!(WarcGallery::can_handle("X/Y.WARC.GZ"));
        assert!(!WarcGallery::can_handle("x/y.jsonl.gz"));
        assert!(!WarcGallery::can_handle("x/y.txt"));
    }
}
