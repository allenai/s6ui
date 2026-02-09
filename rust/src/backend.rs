use crate::events::StateEvent;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// Shared cancel flag for in-flight requests
pub type CancelFlag = Arc<AtomicBool>;

/// Abstract backend interface.
/// Implementations handle async execution and queue events for the model to poll.
pub trait Backend: Send {
    /// Take all pending events (called by model each frame).
    /// Returns events and clears the internal queue.
    fn take_events(&self) -> Vec<StateEvent>;

    /// Request bucket list.
    fn list_buckets(&self);

    /// Request objects in a bucket/prefix.
    /// `continuation_token` is empty for first request.
    fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: &str,
        cancel_flag: Option<CancelFlag>,
    );

    /// Request object content (for preview).
    /// `max_bytes` limits download size (0 = no limit).
    /// `low_priority` = true for background prefetch.
    /// `cancellable` = true means this request can be cancelled by newer hover prefetches.
    fn get_object(
        &self,
        bucket: &str,
        key: &str,
        max_bytes: usize,
        low_priority: bool,
        cancellable: bool,
    );

    /// Request a specific byte range of an object.
    fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        start_byte: usize,
        end_byte: usize,
        cancel_flag: Option<CancelFlag>,
    );

    /// Stream an object from a starting byte offset.
    /// Emits ObjectRangeLoaded events as chunks arrive.
    fn get_object_streaming(
        &self,
        bucket: &str,
        key: &str,
        start_byte: usize,
        total_size: usize,
        cancel_flag: Option<CancelFlag>,
    );

    /// Cancel all pending requests.
    fn cancel_all(&self);

    /// Queue a low-priority background request for prefetch.
    fn list_objects_prefetch(&self, bucket: &str, prefix: &str, cancellable: bool);

    /// Boost a pending request to high priority. Returns true if found and boosted.
    fn prioritize_request(&self, bucket: &str, prefix: &str) -> bool;

    /// Check if there's already a pending request for this bucket/prefix.
    fn has_pending_request(&self, bucket: &str, prefix: &str) -> bool;

    /// Check if there's already a pending object fetch request.
    fn has_pending_object_request(&self, bucket: &str, key: &str) -> bool;

    /// Boost a pending object request to high priority.
    fn prioritize_object_request(&self, bucket: &str, key: &str) -> bool;
}
