//! Streaming file preview with on-the-fly decompression and line indexing.
//!
//! Downloads S3 objects in chunks, decompresses if needed (gzip/zstd),
//! writes decompressed content to a temp file, and indexes newline positions
//! for efficient line-based rendering.

use memmap2::Mmap;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use zstd::stream::raw::Operation;

/// Atomic counter for unique temp file names
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Prefetch size: first 64KB downloaded before user can request more
pub const PREFETCH_BYTES: u64 = 64 * 1024;

/// Status of a streaming preview
#[derive(Clone, PartialEq, Debug)]
pub enum StreamingStatus {
    /// First 64KB download in progress
    Prefetching,
    /// First 64KB done, user can request full download
    PrefetchReady,
    /// Full download in progress
    Downloading,
    /// All data received
    Complete,
    /// Error occurred
    Error(String),
}

/// Compression type detected from file extension
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Compression {
    None,
    Gzip,
    Zstd,
}

impl Compression {
    /// Detect compression from filename extension
    pub fn from_filename(key: &str) -> Self {
        let lower = key.to_lowercase();
        if lower.ends_with(".gz") || lower.ends_with(".gzip") {
            Compression::Gzip
        } else if lower.ends_with(".zst") || lower.ends_with(".zstd") {
            Compression::Zstd
        } else {
            Compression::None
        }
    }
}

/// Trait for decompression transforms
pub trait Transform: Send {
    /// Process input chunk, append decompressed output
    fn process(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<(), String>;

    /// Finalize stream (flush remaining data)
    fn finish(&mut self, output: &mut Vec<u8>) -> Result<(), String>;
}

/// Identity transform for uncompressed files
pub struct IdentityTransform;

impl Transform for IdentityTransform {
    fn process(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<(), String> {
        output.extend_from_slice(input);
        Ok(())
    }

    fn finish(&mut self, _output: &mut Vec<u8>) -> Result<(), String> {
        Ok(())
    }
}

/// Gzip decompression transform using accumulated buffer + GzDecoder
pub struct GzipTransform {
    buffer: Vec<u8>,
    finished: bool,
}

impl GzipTransform {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            finished: false,
        }
    }

    fn try_decompress(&mut self, output: &mut Vec<u8>) -> Result<(), String> {
        use flate2::bufread::GzDecoder;
        use std::io::Read;

        if self.buffer.is_empty() || self.finished {
            return Ok(());
        }

        let mut decoder = GzDecoder::new(&self.buffer[..]);
        let mut decompressed = Vec::new();

        loop {
            let mut chunk = [0u8; 8192];
            match decoder.read(&mut chunk) {
                Ok(0) => {
                    // EOF - we've decompressed everything
                    self.finished = true;
                    break;
                }
                Ok(n) => {
                    decompressed.extend_from_slice(&chunk[..n]);
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Need more input data - this is normal for streaming
                    break;
                }
                Err(e) => {
                    return Err(format!("gzip decompress error: {}", e));
                }
            }
        }

        output.extend_from_slice(&decompressed);
        Ok(())
    }
}

impl Transform for GzipTransform {
    fn process(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<(), String> {
        // Accumulate input
        self.buffer.extend_from_slice(input);
        // Try to decompress what we have
        self.try_decompress(output)
    }

    fn finish(&mut self, output: &mut Vec<u8>) -> Result<(), String> {
        // Final decompression attempt
        self.try_decompress(output)
    }
}

/// Zstd decompression transform
pub struct ZstdTransform {
    decoder: zstd::stream::raw::Decoder<'static>,
}

impl ZstdTransform {
    pub fn new() -> Result<Self, String> {
        let decoder =
            zstd::stream::raw::Decoder::new().map_err(|e| format!("zstd init error: {}", e))?;
        Ok(Self { decoder })
    }
}

impl Transform for ZstdTransform {
    fn process(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<(), String> {
        let start_out = output.len();
        // Reserve space for decompressed output
        output.resize(start_out + input.len() * 4 + 1024, 0);

        let mut in_buf = zstd::stream::raw::InBuffer::around(input);
        let mut total_out = start_out;

        while in_buf.pos() < in_buf.src.len() {
            if total_out >= output.len() {
                output.resize(output.len() * 2, 0);
            }

            let mut out_buf = zstd::stream::raw::OutBuffer::around(&mut output[total_out..]);
            self.decoder
                .run(&mut in_buf, &mut out_buf)
                .map_err(|e| format!("zstd decompress error: {}", e))?;

            total_out += out_buf.pos();
        }

        output.truncate(total_out);
        Ok(())
    }

    fn finish(&mut self, output: &mut Vec<u8>) -> Result<(), String> {
        loop {
            let start_out = output.len();
            output.resize(start_out + 1024, 0);

            let (remaining, written) = {
                let mut out_buf = zstd::stream::raw::OutBuffer::around(&mut output[start_out..]);
                let remaining = self
                    .decoder
                    .flush(&mut out_buf)
                    .map_err(|e| format!("zstd finish error: {}", e))?;
                (remaining, out_buf.pos())
            };

            output.truncate(start_out + written);

            if remaining == 0 {
                break;
            }
        }

        Ok(())
    }
}

/// Create appropriate transform for compression type
pub fn create_transform(compression: Compression) -> Result<Box<dyn Transform>, String> {
    match compression {
        Compression::None => Ok(Box::new(IdentityTransform)),
        Compression::Gzip => Ok(Box::new(GzipTransform::new())),
        Compression::Zstd => Ok(Box::new(ZstdTransform::new()?)),
    }
}

/// Mutable state protected by mutex
struct StreamingState {
    /// Decompressed bytes written to temp file
    bytes_written: u64,
    /// Compressed/raw bytes downloaded from source
    source_bytes: u64,
    /// Line byte offsets (start of each line)
    line_offsets: Vec<u64>,
    /// Current status
    status: StreamingStatus,
    /// Partial line buffer (bytes after last newline)
    partial_line: Vec<u8>,
    /// Decompression transform (owned, processes incoming chunks)
    transform: Box<dyn Transform>,
    /// Reusable buffer for decompression output
    decompress_buf: Vec<u8>,
}

/// Streaming file preview with on-the-fly decompression
pub struct StreamingFilePreview {
    /// Backing temp file (opened for read+write)
    temp_file: File,
    /// Path to temp file (for cleanup)
    temp_path: PathBuf,
    /// Total size of source if known (Content-Length)
    total_source_size: AtomicU64,
    /// Compression type
    compression: Compression,
    /// Mutable state protected by mutex
    state: Mutex<StreamingState>,
}

impl StreamingFilePreview {
    /// Create a new streaming preview with temp file
    pub fn new(compression: Compression) -> Result<Self, String> {
        let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let temp_path = std::env::temp_dir().join(format!("s6ui-preview-{}-{}", std::process::id(), counter));

        let temp_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(|e| format!("Failed to create temp file: {}", e))?;

        let transform = create_transform(compression)?;

        Ok(Self {
            temp_file,
            temp_path,
            total_source_size: AtomicU64::new(0),
            compression,
            state: Mutex::new(StreamingState {
                bytes_written: 0,
                source_bytes: 0,
                line_offsets: vec![0], // Line 0 starts at byte 0
                status: StreamingStatus::Prefetching,
                partial_line: Vec::new(),
                transform,
                decompress_buf: Vec::with_capacity(64 * 1024),
            }),
        })
    }

    /// Get compression type
    pub fn compression(&self) -> Compression {
        self.compression
    }

    /// Set total source size (from Content-Length header)
    pub fn set_total_source_size(&self, size: u64) {
        self.total_source_size.store(size, Ordering::SeqCst);
    }

    /// Get total source size if known
    pub fn total_source_size(&self) -> Option<u64> {
        let size = self.total_source_size.load(Ordering::SeqCst);
        if size > 0 {
            Some(size)
        } else {
            None
        }
    }

    /// Get bytes written (decompressed)
    pub fn bytes_written(&self) -> u64 {
        self.state.lock().unwrap().bytes_written
    }

    /// Get source bytes downloaded
    pub fn source_bytes(&self) -> u64 {
        self.state.lock().unwrap().source_bytes
    }

    /// Get line count
    pub fn line_count(&self) -> usize {
        self.state.lock().unwrap().line_offsets.len()
    }

    /// Get current status
    pub fn status(&self) -> StreamingStatus {
        self.state.lock().unwrap().status.clone()
    }

    /// Set status
    pub fn set_status(&self, status: StreamingStatus) {
        self.state.lock().unwrap().status = status;
    }

    /// Append a raw chunk from the network (possibly compressed).
    /// Handles decompression internally and writes to temp file.
    pub fn append_chunk(&self, raw_data: &[u8]) -> Result<(), String> {
        if raw_data.is_empty() {
            return Ok(());
        }

        let mut state = self.state.lock().unwrap();

        // Track source bytes
        state.source_bytes += raw_data.len() as u64;

        // Take the buffer out to avoid borrow conflict
        let mut buf = std::mem::take(&mut state.decompress_buf);
        buf.clear();

        // Decompress
        let result = state.transform.process(raw_data, &mut buf);

        // Put the buffer back
        state.decompress_buf = buf;

        result?;

        // Write decompressed data to temp file
        if !state.decompress_buf.is_empty() {
            self.write_decompressed(&mut state)?;
        }

        Ok(())
    }

    /// Finalize the stream (flush any remaining decompression state)
    pub fn finish_stream(&self) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();

        // Take the buffer out to avoid borrow conflict
        let mut buf = std::mem::take(&mut state.decompress_buf);
        buf.clear();

        // Flush remaining decompression data
        let result = state.transform.finish(&mut buf);

        // Put the buffer back
        state.decompress_buf = buf;

        result?;

        // Write any remaining decompressed data
        if !state.decompress_buf.is_empty() {
            self.write_decompressed(&mut state)?;
        }

        Ok(())
    }

    /// Internal: write decompressed data from buffer to temp file and index newlines
    fn write_decompressed(&self, state: &mut StreamingState) -> Result<(), String> {
        let data = &state.decompress_buf;
        if data.is_empty() {
            return Ok(());
        }

        let write_offset = state.bytes_written;

        // Write to temp file using pwrite (positional write)
        self.temp_file
            .write_all_at(data, write_offset)
            .map_err(|e| format!("Failed to write to temp file: {}", e))?;

        // Update line index
        let search_start = if state.partial_line.is_empty() {
            0
        } else {
            state.partial_line.len()
        };

        state.partial_line.extend_from_slice(data);

        // Find newlines and collect line starts
        let partial_len = state.partial_line.len();
        let mut new_line_offsets = Vec::new();
        let mut last_newline_pos = 0;

        for (i, &byte) in state.partial_line[search_start..].iter().enumerate() {
            if byte == b'\n' {
                let abs_pos = search_start + i;
                let line_start = write_offset as i64
                    - (partial_len as i64 - data.len() as i64)
                    + abs_pos as i64 + 1;
                if line_start >= 0 {
                    new_line_offsets.push(line_start as u64);
                }
                last_newline_pos = abs_pos + 1;
            }
        }

        // Add collected line offsets
        state.line_offsets.extend(new_line_offsets);

        // Keep only the part after the last newline
        if last_newline_pos > 0 {
            state.partial_line = state.partial_line[last_newline_pos..].to_vec();
        }

        state.bytes_written += data.len() as u64;
        Ok(())
    }

    /// Mark prefetch as complete
    pub fn set_prefetch_ready(&self) {
        let mut state = self.state.lock().unwrap();
        if state.status == StreamingStatus::Prefetching {
            state.status = StreamingStatus::PrefetchReady;
        }
    }

    /// Mark download as complete
    pub fn set_complete(&self) {
        let mut state = self.state.lock().unwrap();
        state.status = StreamingStatus::Complete;
    }

    /// Mark as downloading (full file)
    pub fn set_downloading(&self) {
        let mut state = self.state.lock().unwrap();
        if matches!(state.status, StreamingStatus::PrefetchReady | StreamingStatus::Prefetching) {
            state.status = StreamingStatus::Downloading;
        }
    }

    /// Read a range of lines for display
    pub fn read_lines(&self, start_line: usize, count: usize) -> Vec<String> {
        // Get line offsets and bytes_written under lock, then release
        let (offsets, next_offset, bytes_written) = {
            let state = self.state.lock().unwrap();
            let end_line = std::cmp::min(start_line + count, state.line_offsets.len());
            let offsets: Vec<u64> = state.line_offsets[start_line..end_line].to_vec();
            let next_offset = state.line_offsets.get(end_line).copied();
            let bytes_written = state.bytes_written;
            (offsets, next_offset, bytes_written)
        };

        let mut result = Vec::with_capacity(count);

        for (i, &start) in offsets.iter().enumerate() {
            // For end offset, try the next item in our slice, then the next_offset, then bytes_written
            let end = offsets
                .get(i + 1)
                .copied()
                .or(next_offset)
                .unwrap_or(bytes_written);

            // Read line from file
            if start < end {
                let len = (end - start) as usize;
                let mut buf = vec![0u8; len];
                if self.temp_file.read_at(&mut buf, start).is_ok() {
                    // Strip trailing newline if present
                    if buf.last() == Some(&b'\n') {
                        buf.pop();
                    }
                    if buf.last() == Some(&b'\r') {
                        buf.pop();
                    }
                    result.push(String::from_utf8_lossy(&buf).into_owned());
                } else {
                    result.push(String::new());
                }
            } else {
                result.push(String::new());
            }
        }

        result
    }

    /// Memory map the temp file for efficient access
    pub fn mmap(&self) -> Result<Mmap, std::io::Error> {
        unsafe { Mmap::map(&self.temp_file) }
    }

    /// Get line offsets for external iteration
    pub fn line_offsets(&self) -> Vec<u64> {
        self.state.lock().unwrap().line_offsets.clone()
    }
}

impl Drop for StreamingFilePreview {
    fn drop(&mut self) {
        // Delete temp file
        let _ = std::fs::remove_file(&self.temp_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_transform() {
        let mut transform = IdentityTransform;
        let input = b"Hello, World!";
        let mut output = Vec::new();
        transform.process(input, &mut output).unwrap();
        assert_eq!(output, input);
    }

    #[test]
    fn test_streaming_preview_basic() {
        let preview = StreamingFilePreview::new(Compression::None).unwrap();

        preview.append_chunk(b"line1\nline2\nline3\n").unwrap();

        assert_eq!(preview.line_count(), 4); // 3 newlines = 4 line starts
        assert_eq!(preview.bytes_written(), 18);

        let lines = preview.read_lines(0, 3);
        assert_eq!(lines, vec!["line1", "line2", "line3"]);
    }

    #[test]
    fn test_streaming_preview_chunked() {
        let preview = StreamingFilePreview::new(Compression::None).unwrap();

        // Simulate chunked arrival
        preview.append_chunk(b"hel").unwrap();
        preview.append_chunk(b"lo\nwor").unwrap();
        preview.append_chunk(b"ld\n").unwrap();

        assert_eq!(preview.line_count(), 3);

        let lines = preview.read_lines(0, 2);
        assert_eq!(lines, vec!["hello", "world"]);
    }

    #[test]
    fn test_compression_detection() {
        assert_eq!(Compression::from_filename("file.txt"), Compression::None);
        assert_eq!(Compression::from_filename("file.json.gz"), Compression::Gzip);
        assert_eq!(Compression::from_filename("file.log.gzip"), Compression::Gzip);
        assert_eq!(Compression::from_filename("file.csv.zst"), Compression::Zstd);
        assert_eq!(Compression::from_filename("file.data.zstd"), Compression::Zstd);
    }

    #[test]
    fn test_gzip_transform() {
        // Create a simple gzip-compressed payload
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(b"Hello, compressed world!").unwrap();
        let compressed = encoder.finish().unwrap();

        let mut transform = GzipTransform::new();
        let mut output = Vec::new();
        transform.process(&compressed, &mut output).unwrap();
        transform.finish(&mut output).unwrap();

        assert_eq!(String::from_utf8_lossy(&output), "Hello, compressed world!");
    }
}
