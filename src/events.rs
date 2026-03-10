/// S3 bucket metadata
#[derive(Debug, Clone)]
pub struct S3Bucket {
    pub name: String,
}

/// S3 object metadata (file or folder)
#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub display_name: String,
    pub size: i64,
    pub is_folder: bool,
}

use crate::preview::StreamingStatus;

/// Events emitted by the backend for the model to process
pub enum StateEvent {
    BucketsLoaded {
        request_id: u64,
        buckets: Vec<S3Bucket>,
    },
    BucketsError {
        request_id: u64,
        error: String,
    },
    ObjectsLoaded {
        bucket: String,
        prefix: String,
        request_id: u64,
        continuation_token: String,
        objects: Vec<S3Object>,
        next_continuation_token: String,
        is_truncated: bool,
    },
    ObjectsError {
        bucket: String,
        prefix: String,
        request_id: u64,
        error: String,
    },
    /// Streaming preview progress update
    PreviewProgress {
        bucket: String,
        key: String,
        request_id: u64,
        status: StreamingStatus,
    },
    /// Streaming preview error
    PreviewError {
        bucket: String,
        key: String,
        request_id: u64,
    },
}
