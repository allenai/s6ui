use crate::events::StateEvent;

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
    fn list_objects(&self, bucket: &str, prefix: &str, continuation_token: &str);

    /// Request object content (for preview).
    /// max_bytes limits download size (0 = no limit).
    fn get_object(&self, bucket: &str, key: &str, max_bytes: usize);

    /// Cancel all pending requests.
    fn cancel_all(&self);
}
