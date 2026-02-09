use std::fs;
use std::io::{Read, Write};
use std::sync::Mutex;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

/// Abstract interface for data transformation (decompression, etc.)
pub trait StreamTransform: Send {
    fn transform(&mut self, data: &[u8]) -> Vec<u8>;
    fn flush(&mut self) -> Vec<u8>;
}

/// Shared transform/flush logic for buffered decompression transforms.
/// Buffers all compressed data and re-decompresses from scratch each time,
/// emitting only the newly decompressed bytes.
macro_rules! impl_buffered_transform {
    ($ty:ty) => {
        impl StreamTransform for $ty {
            fn transform(&mut self, data: &[u8]) -> Vec<u8> {
                if self.error || data.is_empty() {
                    return vec![];
                }
                self.buffer.extend_from_slice(data);
                let all = self.decompress_all();
                if all.len() > self.emitted {
                    let new_data = all[self.emitted..].to_vec();
                    self.emitted = all.len();
                    new_data
                } else {
                    vec![]
                }
            }

            fn flush(&mut self) -> Vec<u8> {
                if self.error {
                    return vec![];
                }
                let all = self.decompress_all();
                if all.len() > self.emitted {
                    let new_data = all[self.emitted..].to_vec();
                    self.emitted = all.len();
                    new_data
                } else {
                    vec![]
                }
            }
        }
    };
}

/// Pass-through transform (no transformation)
pub struct PassThroughTransform;

impl StreamTransform for PassThroughTransform {
    fn transform(&mut self, data: &[u8]) -> Vec<u8> {
        data.to_vec()
    }
    fn flush(&mut self) -> Vec<u8> {
        vec![]
    }
}

/// Gzip decompression transform.
/// Buffers all compressed data and re-decompresses from scratch each time,
/// tracking how many decompressed bytes were already emitted.
pub struct GzipTransform {
    buffer: Vec<u8>,
    emitted: usize,
    error: bool,
}

impl GzipTransform {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            emitted: 0,
            error: false,
        }
    }

    pub fn has_error(&self) -> bool {
        self.error
    }

    fn decompress_all(&self) -> Vec<u8> {
        use flate2::read::GzDecoder;
        let cursor = std::io::Cursor::new(&self.buffer);
        let mut decoder = GzDecoder::new(cursor);
        let mut output = Vec::new();
        let _ = decoder.read_to_end(&mut output);
        output
    }
}

impl_buffered_transform!(GzipTransform);

/// Zstd decompression transform.
/// Same approach: buffer all compressed data, decompress from scratch.
pub struct ZstdTransform {
    buffer: Vec<u8>,
    emitted: usize,
    error: bool,
}

impl ZstdTransform {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            emitted: 0,
            error: false,
        }
    }

    pub fn has_error(&self) -> bool {
        self.error
    }

    fn decompress_all(&self) -> Vec<u8> {
        let cursor = std::io::Cursor::new(&self.buffer);
        let mut decoder = match zstd::stream::Decoder::new(cursor) {
            Ok(d) => d,
            Err(_) => return vec![],
        };
        let mut output = Vec::new();
        let _ = decoder.read_to_end(&mut output);
        output
    }
}

impl_buffered_transform!(ZstdTransform);

/// Manages streaming download of a file to a temp file with newline indexing.
pub struct StreamingFilePreview {
    bucket: String,
    key: String,
    temp_file_path: String,
    file: Option<fs::File>,
    total_source_size: usize,
    inner: Mutex<StreamingInner>,
}

struct StreamingInner {
    bytes_downloaded: usize,
    bytes_written: usize,
    complete: bool,
    line_offsets: Vec<usize>,
    transform: Box<dyn StreamTransform>,
}

impl StreamingFilePreview {
    pub fn new(
        bucket: String,
        key: String,
        initial_data: &[u8],
        total_file_size: usize,
        transform: Option<Box<dyn StreamTransform>>,
    ) -> Self {
        let tmpdir = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_string());
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let temp_file_path = format!(
            "{}/s6ui_preview_{}_{}",
            tmpdir,
            std::process::id(),
            COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
        );

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_file_path)
            .ok();

        let transform = transform.unwrap_or_else(|| Box::new(PassThroughTransform));

        let mut preview = StreamingFilePreview {
            bucket,
            key,
            temp_file_path,
            file,
            total_source_size: total_file_size,
            inner: Mutex::new(StreamingInner {
                bytes_downloaded: 0,
                bytes_written: 0,
                complete: false,
                line_offsets: vec![0],
                transform,
            }),
        };

        // Write initial data
        if !initial_data.is_empty() {
            let mut inner = preview.inner.lock().unwrap();
            let transformed = inner.transform.transform(initial_data);
            if !transformed.is_empty() {
                if let Some(ref mut f) = preview.file {
                    let base_offset = inner.bytes_written;
                    let _ = f.write_all(&transformed);
                    Self::index_newlines(&transformed, base_offset, &mut inner.line_offsets);
                    inner.bytes_written += transformed.len();
                }
            }
            inner.bytes_downloaded = initial_data.len();

            if inner.bytes_downloaded >= total_file_size {
                Self::finish_stream_inner(&mut inner, &mut preview.file);
            }
        }

        preview
    }

    fn finish_stream_inner(inner: &mut StreamingInner, file: &mut Option<fs::File>) {
        if inner.complete {
            return;
        }

        let remaining = inner.transform.flush();
        if !remaining.is_empty() {
            if let Some(f) = file {
                let base_offset = inner.bytes_written;
                let _ = f.write_all(&remaining);
                Self::index_newlines(&remaining, base_offset, &mut inner.line_offsets);
                inner.bytes_written += remaining.len();
            }
        }

        inner.complete = true;
    }

    fn index_newlines(data: &[u8], base_offset: usize, line_offsets: &mut Vec<usize>) {
        for (i, &b) in data.iter().enumerate() {
            if b == b'\n' {
                line_offsets.push(base_offset + i + 1);
            }
        }
    }

    /// Append a new chunk from streaming download.
    pub fn append_chunk(&self, data: &[u8], offset: usize) {
        let mut inner = self.inner.lock().unwrap();

        if self.file.is_none() {
            return;
        }

        if offset != inner.bytes_downloaded {
            eprintln!(
                "StreamingFilePreview: chunk offset mismatch, expected {} got {}",
                inner.bytes_downloaded, offset
            );
            return;
        }

        let transformed = inner.transform.transform(data);
        if !transformed.is_empty() {
            // We need to write to the file but self.file is behind &self.
            // Use the file handle via unsafe raw fd for pwrite, or just accept the limitation.
            // Actually, File::write_all needs &mut File, but we can use write on &File on unix.

            if let Some(ref f) = self.file {
                let base_offset = inner.bytes_written;
                let _ = f.write_all_at(&transformed, base_offset as u64);
                Self::index_newlines(&transformed, base_offset, &mut inner.line_offsets);
                inner.bytes_written += transformed.len();
            }
        }
        inner.bytes_downloaded += data.len();

        if inner.bytes_downloaded >= self.total_source_size {
            // For finish_stream, we can't pass &mut self.file since self is &self.
            // Instead, do an inline finish.
            if !inner.complete {
                let remaining = inner.transform.flush();
                if !remaining.is_empty() {
        
                    if let Some(ref f) = self.file {
                        let base_offset = inner.bytes_written;
                        let _ = f.write_all_at(&remaining, base_offset as u64);
                        Self::index_newlines(&remaining, base_offset, &mut inner.line_offsets);
                        inner.bytes_written += remaining.len();
                    }
                }
                inner.complete = true;
            }
        }
    }

    pub fn line_count(&self) -> usize {
        self.inner.lock().unwrap().line_offsets.len()
    }

    pub fn bytes_downloaded(&self) -> usize {
        self.inner.lock().unwrap().bytes_downloaded
    }

    pub fn bytes_written(&self) -> usize {
        self.inner.lock().unwrap().bytes_written
    }

    pub fn total_source_bytes(&self) -> usize {
        self.total_source_size
    }

    pub fn is_complete(&self) -> bool {
        self.inner.lock().unwrap().complete
    }

    pub fn next_byte_needed(&self) -> usize {
        self.inner.lock().unwrap().bytes_downloaded
    }

    /// Get a specific line (0-indexed).
    pub fn get_line(&self, line_index: usize) -> String {
        let inner = self.inner.lock().unwrap();

        if line_index >= inner.line_offsets.len() {
            return String::new();
        }

        let start_offset = inner.line_offsets[line_index];
        let end_offset = if line_index + 1 < inner.line_offsets.len() {
            inner.line_offsets[line_index + 1].saturating_sub(1)
        } else {
            inner.bytes_written
        };

        let end_offset = end_offset.min(inner.bytes_written);
        if start_offset >= end_offset {
            return String::new();
        }

        let line_len = (end_offset - start_offset).min(10 * 1024 * 1024);
        drop(inner);


        if let Some(ref f) = self.file {
            let mut buf = vec![0u8; line_len];
            match f.read_at(&mut buf, start_offset as u64) {
                Ok(n) => {
                    buf.truncate(n);
                    while buf.last() == Some(&b'\n') || buf.last() == Some(&b'\r') {
                        buf.pop();
                    }
                    String::from_utf8_lossy(&buf).into_owned()
                }
                Err(_) => String::new(),
            }
        } else {
            String::new()
        }
    }

    /// Get all content written so far.
    pub fn get_all_content(&self) -> Vec<u8> {
        let inner = self.inner.lock().unwrap();
        let bytes_written = inner.bytes_written;
        drop(inner);

        if bytes_written == 0 {
            return vec![];
        }


        if let Some(ref f) = self.file {
            let mut buf = vec![0u8; bytes_written];
            match f.read_at(&mut buf, 0) {
                Ok(n) => {
                    buf.truncate(n);
                    buf
                }
                Err(_) => vec![],
            }
        } else {
            vec![]
        }
    }

    pub fn is_line_complete(&self, line_index: usize) -> bool {
        let inner = self.inner.lock().unwrap();
        if line_index >= inner.line_offsets.len() {
            return false;
        }
        if line_index + 1 < inner.line_offsets.len() {
            return true;
        }
        inner.complete
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn temp_file_path(&self) -> &str {
        &self.temp_file_path
    }
}

impl Drop for StreamingFilePreview {
    fn drop(&mut self) {
        // Drop the file handle before removing
        drop(self.file.take());
        if !self.temp_file_path.is_empty() {
            let _ = fs::remove_file(&self.temp_file_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_preview_basic() {
        let data = b"line1\nline2\nline3\n";
        let preview = StreamingFilePreview::new(
            "bucket".to_string(),
            "key".to_string(),
            data,
            data.len(),
            None,
        );

        assert_eq!(preview.line_count(), 4); // 3 lines + empty after last \n
        assert_eq!(preview.get_line(0), "line1");
        assert_eq!(preview.get_line(1), "line2");
        assert_eq!(preview.get_line(2), "line3");
        assert!(preview.is_complete());
    }

    #[test]
    fn test_streaming_preview_chunked() {
        let total = b"hello\nworld\n";
        let preview = StreamingFilePreview::new(
            "bucket".to_string(),
            "key".to_string(),
            &total[..6], // "hello\n"
            total.len(),
            None,
        );

        assert_eq!(preview.line_count(), 2);
        assert_eq!(preview.get_line(0), "hello");
        assert!(!preview.is_complete());

        preview.append_chunk(&total[6..], 6);
        assert_eq!(preview.line_count(), 3);
        assert_eq!(preview.get_line(1), "world");
        assert!(preview.is_complete());
    }
}
