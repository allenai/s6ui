use crate::events::StateEvent;
use crate::preview::StreamingFilePreview;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestPriority {
    High,
    Low,
}

#[derive(Clone, Debug, Default)]
pub struct BackendDebugSnapshot {
    pub high_queue_len: usize,
    pub low_queue_len: usize,
    pub active_high_requests: usize,
    pub active_low_requests: usize,
    pub active_list_buckets: usize,
    pub active_list_objects: usize,
    pub active_get_object: usize,
    pub active_streaming_get_object: usize,
}

/// Abstract backend interface for async S3 operations.
/// Implementations handle execution on background threads/tasks
/// and queue StateEvents for the model to poll each frame.
pub trait Backend: Send {
    /// Take all pending events (called by model each frame).
    /// Returns events and clears the internal queue.
    fn take_events(&self) -> Vec<StateEvent>;

    /// Request bucket list.
    fn list_buckets(&self);

    /// Request objects in a bucket/prefix.
    /// continuation_token is empty for first request.
    fn list_objects_with_priority(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: &str,
        priority: RequestPriority,
    );

    /// Request objects in a bucket/prefix at high priority.
    fn list_objects(&self, bucket: &str, prefix: &str, continuation_token: &str) {
        self.list_objects_with_priority(bucket, prefix, continuation_token, RequestPriority::High);
    }

    /// Request object content (for preview).
    /// max_bytes limits download size (0 = no limit).
    fn get_object(&self, bucket: &str, key: &str, max_bytes: usize);

    /// Request streaming object download with on-the-fly decompression.
    /// Writes decompressed content to the StreamingFilePreview's temp file.
    /// range_start: byte offset to start from (for continuation)
    /// max_bytes: None = full file, Some(n) = limit to n bytes
    fn streaming_get_object_with_priority(
        &self,
        bucket: &str,
        key: &str,
        preview: Arc<StreamingFilePreview>,
        range_start: u64,
        max_bytes: Option<u64>,
        priority: RequestPriority,
    );

    /// Request streaming object download at high priority.
    fn streaming_get_object(
        &self,
        bucket: &str,
        key: &str,
        preview: Arc<StreamingFilePreview>,
        range_start: u64,
        max_bytes: Option<u64>,
    ) {
        self.streaming_get_object_with_priority(
            bucket,
            key,
            preview,
            range_start,
            max_bytes,
            RequestPriority::High,
        );
    }

    /// Cancel all pending requests.
    fn cancel_all(&self);

    /// Cancel queued streaming object requests except for the provided object key.
    fn cancel_streaming_requests_except(&self, bucket: &str, key: &str);

    /// Snapshot backend runtime state for debug overlay.
    fn debug_snapshot(&self) -> BackendDebugSnapshot;
}
