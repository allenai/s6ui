/// An object in an S3 listing
#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub size: i64,
    pub last_modified: String,
}

/// Requests from the model to the S3 backend
#[derive(Debug)]
pub enum S3Request {
    ListBuckets {
        profile: String,
    },
    ListObjects {
        profile: String,
        bucket: String,
        prefix: String,
        continuation_token: Option<String>,
    },
}

/// Events from the S3 backend back to the model
#[derive(Debug)]
pub enum StateEvent {
    BucketsLoaded {
        profile: String,
        buckets: Vec<String>,
    },
    ObjectsLoaded {
        bucket: String,
        prefix: String,
        objects: Vec<S3Object>,
        common_prefixes: Vec<String>,
        next_continuation_token: Option<String>,
        is_continuation: bool,
    },
    Error(String),
}
