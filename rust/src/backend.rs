use crate::events::StateEvent;
use crate::preview::StreamingFilePreview;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestPriority {
    High,
    Low,
}

/// Abstract backend interface for async S3 operations.
/// Implementations handle execution on background threads/tasks
/// and queue StateEvents for the model to poll each frame.
pub trait Backend: Send {
    /// Take all pending events (called by model each frame).
    /// Returns events and clears the internal queue.
    fn take_events(&self) -> Vec<StateEvent>;

    /// Request bucket list.
    fn list_buckets(&self, request_id: u64);

    /// Request objects in a bucket/prefix.
    /// continuation_token is empty for first request.
    fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: &str,
        priority: RequestPriority,
        request_id: u64,
    );

    /// Request streaming object download with on-the-fly decompression.
    /// Writes decompressed content to the StreamingFilePreview's temp file.
    /// range_start: byte offset to start from (for continuation)
    /// max_bytes: None = full file, Some(n) = limit to n bytes
    fn streaming_get_object(
        &self,
        bucket: &str,
        key: &str,
        preview: Arc<StreamingFilePreview>,
        range_start: u64,
        max_bytes: Option<u64>,
        priority: RequestPriority,
        request_id: u64,
    );

    /// Cancel all pending requests.
    fn cancel_all(&self);

    /// Cancel queued requests for a specific priority class.
    fn cancel_pending(&self, priority: RequestPriority);
}
