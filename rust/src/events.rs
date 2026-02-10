/// S3 bucket metadata
#[derive(Debug, Clone)]
pub struct S3Bucket {
    pub name: String,
    pub creation_date: String,
}

/// S3 object metadata (file or folder)
#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub display_name: String,
    pub size: i64,
    pub last_modified: String,
    pub is_folder: bool,
}

use crate::preview::StreamingStatus;

/// Events emitted by the backend for the model to process
pub enum StateEvent {
    BucketsLoaded {
        buckets: Vec<S3Bucket>,
    },
    BucketsError {
        error: String,
    },
    ObjectsLoaded {
        bucket: String,
        prefix: String,
        continuation_token: String,
        objects: Vec<S3Object>,
        next_continuation_token: String,
        is_truncated: bool,
    },
    ObjectsError {
        bucket: String,
        prefix: String,
        error: String,
    },
    ObjectContentLoaded {
        bucket: String,
        key: String,
        content: String,
    },
    ObjectContentError {
        bucket: String,
        key: String,
        error: String,
    },
    /// Streaming preview progress update
    PreviewProgress {
        bucket: String,
        key: String,
        decompressed_bytes: u64,
        source_bytes: u64,
        line_count: usize,
        status: StreamingStatus,
    },
    /// Streaming preview error
    PreviewError {
        bucket: String,
        key: String,
        error: String,
    },
}
