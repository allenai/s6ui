use std::fmt;

#[derive(Debug, Clone)]
pub struct S3Bucket {
    pub name: String,
    pub creation_date: String,
}

#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub display_name: String,
    pub size: i64,
    pub last_modified: String,
    pub is_folder: bool,
}

#[derive(Debug)]
pub enum StateEvent {
    BucketsLoaded {
        buckets: Vec<S3Bucket>,
    },
    BucketsLoadError {
        error_message: String,
    },
    ObjectsLoaded {
        bucket: String,
        prefix: String,
        continuation_token: String,
        objects: Vec<S3Object>,
        next_continuation_token: String,
        is_truncated: bool,
    },
    ObjectsLoadError {
        bucket: String,
        prefix: String,
        error_message: String,
    },
    ObjectContentLoaded {
        bucket: String,
        key: String,
        content: Vec<u8>,
    },
    ObjectContentLoadError {
        bucket: String,
        key: String,
        error_message: String,
    },
    ObjectRangeLoaded {
        bucket: String,
        key: String,
        start_byte: usize,
        total_size: usize,
        data: Vec<u8>,
    },
    ObjectRangeLoadError {
        bucket: String,
        key: String,
        start_byte: usize,
        error_message: String,
    },
}

impl fmt::Display for StateEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StateEvent::BucketsLoaded { buckets } => {
                write!(f, "BucketsLoaded(count={})", buckets.len())
            }
            StateEvent::BucketsLoadError { error_message } => {
                write!(f, "BucketsLoadError({})", error_message)
            }
            StateEvent::ObjectsLoaded {
                bucket,
                prefix,
                objects,
                is_truncated,
                ..
            } => write!(
                f,
                "ObjectsLoaded(bucket={}, prefix={}, count={}, truncated={})",
                bucket,
                prefix,
                objects.len(),
                is_truncated
            ),
            StateEvent::ObjectsLoadError {
                bucket,
                prefix,
                error_message,
            } => write!(
                f,
                "ObjectsLoadError(bucket={}, prefix={}, {})",
                bucket, prefix, error_message
            ),
            StateEvent::ObjectContentLoaded {
                bucket,
                key,
                content,
            } => write!(
                f,
                "ObjectContentLoaded(bucket={}, key={}, size={})",
                bucket,
                key,
                content.len()
            ),
            StateEvent::ObjectContentLoadError {
                bucket,
                key,
                error_message,
            } => write!(
                f,
                "ObjectContentLoadError(bucket={}, key={}, {})",
                bucket, key, error_message
            ),
            StateEvent::ObjectRangeLoaded {
                bucket,
                key,
                start_byte,
                total_size,
                data,
            } => write!(
                f,
                "ObjectRangeLoaded(bucket={}, key={}, offset={}, size={}, total={})",
                bucket,
                key,
                start_byte,
                data.len(),
                total_size
            ),
            StateEvent::ObjectRangeLoadError {
                bucket,
                key,
                start_byte,
                error_message,
            } => write!(
                f,
                "ObjectRangeLoadError(bucket={}, key={}, offset={}, {})",
                bucket, key, start_byte, error_message
            ),
        }
    }
}
