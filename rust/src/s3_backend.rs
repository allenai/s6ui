use crate::events::{S3Object, S3Request, StateEvent};
use std::sync::mpsc;
use tokio::sync::mpsc as tokio_mpsc;

pub struct S3Backend {
    request_tx: tokio_mpsc::UnboundedSender<S3Request>,
    event_rx: mpsc::Receiver<StateEvent>,
}

impl S3Backend {
    pub fn new() -> Self {
        let (request_tx, request_rx) = tokio_mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::channel();

        std::thread::Builder::new()
            .name("s3-backend".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .enable_all()
                    .build()
                    .expect("Failed to create tokio runtime");
                rt.block_on(worker_loop(request_rx, event_tx));
            })
            .expect("Failed to spawn backend thread");

        Self {
            request_tx,
            event_rx,
        }
    }

    pub fn send_request(&self, request: S3Request) {
        let _ = self.request_tx.send(request);
    }

    pub fn try_recv_event(&self) -> Option<StateEvent> {
        self.event_rx.try_recv().ok()
    }
}

async fn worker_loop(
    mut rx: tokio_mpsc::UnboundedReceiver<S3Request>,
    tx: mpsc::Sender<StateEvent>,
) {
    while let Some(request) = rx.recv().await {
        let tx = tx.clone();
        tokio::spawn(async move {
            let events = handle_request(request).await;
            for event in events {
                let _ = tx.send(event);
            }
        });
    }
}

async fn make_s3_client(profile: &str) -> aws_sdk_s3::Client {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if !profile.is_empty() && profile != "default" {
        loader = loader.profile_name(profile);
    }
    let config = loader.load().await;
    aws_sdk_s3::Client::new(&config)
}

async fn handle_request(request: S3Request) -> Vec<StateEvent> {
    match request {
        S3Request::ListBuckets { profile } => match list_buckets(&profile).await {
            Ok(evt) => vec![evt],
            Err(e) => vec![StateEvent::Error(format!("List buckets failed: {e}"))],
        },
        S3Request::ListObjects {
            profile,
            bucket,
            prefix,
            continuation_token,
        } => {
            let is_cont = continuation_token.is_some();
            match list_objects(&profile, &bucket, &prefix, continuation_token, is_cont).await {
                Ok(evt) => vec![evt],
                Err(e) => vec![StateEvent::Error(format!("List objects failed: {e}"))],
            }
        }
    }
}

async fn list_buckets(
    profile: &str,
) -> Result<StateEvent, Box<dyn std::error::Error + Send + Sync>> {
    let client = make_s3_client(profile).await;
    let resp = client.list_buckets().send().await?;
    let buckets: Vec<String> = resp
        .buckets()
        .iter()
        .filter_map(|b| b.name().map(|s| s.to_string()))
        .collect();
    Ok(StateEvent::BucketsLoaded {
        profile: profile.to_string(),
        buckets,
    })
}

async fn list_objects(
    profile: &str,
    bucket: &str,
    prefix: &str,
    continuation_token: Option<String>,
    is_continuation: bool,
) -> Result<StateEvent, Box<dyn std::error::Error + Send + Sync>> {
    let client = make_s3_client(profile).await;

    let mut req = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .delimiter("/")
        .max_keys(1000);

    if let Some(token) = &continuation_token {
        req = req.continuation_token(token);
    }

    let resp = req.send().await?;

    let objects: Vec<S3Object> = resp
        .contents()
        .iter()
        .map(|obj| S3Object {
            key: obj.key().unwrap_or("").to_string(),
            size: obj.size().unwrap_or(0),
            last_modified: obj
                .last_modified()
                .map(|dt| {
                    dt.fmt(aws_smithy_types::date_time::Format::DateTime)
                        .unwrap_or_else(|_| "unknown".into())
                })
                .unwrap_or_default(),
        })
        .collect();

    let common_prefixes: Vec<String> = resp
        .common_prefixes()
        .iter()
        .filter_map(|cp| cp.prefix().map(|s| s.to_string()))
        .collect();

    let is_truncated = resp.is_truncated().unwrap_or(false);
    let next_token = if is_truncated {
        resp.next_continuation_token().map(|s| s.to_string())
    } else {
        None
    };

    Ok(StateEvent::ObjectsLoaded {
        bucket: bucket.to_string(),
        prefix: prefix.to_string(),
        objects,
        common_prefixes,
        next_continuation_token: next_token,
        is_continuation,
    })
}
