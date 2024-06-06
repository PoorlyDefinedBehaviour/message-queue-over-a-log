use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use axum::{
    extract::{Query, State},
    http::HeaderMap,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use std::{
    collections::HashMap,
    io::ErrorKind,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{Mutex, Notify},
};

const CLIENT_ID_HEADER: &str = "X-Client-ID";

type ClientID = String;

#[tokio::main]
async fn main() {
    let app = router("./data.log").await.expect("creating router");

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await.unwrap();

    axum::serve(listener, app)
        .await
        .expect("running http server");
}

async fn router(log_file_path: &str) -> Result<Router> {
    Ok(Router::new()
        .route("/enqueue", post(enqueue_handler))
        .route("/receive", get(receive_handler))
        .with_state(Arc::new(Deps {
            storage: Box::new(
                FileStorage::new(log_file_path)
                    .await
                    .context("instantiating FileLogWriter")?,
            ),
        })))
}

#[async_trait]
trait Storage: Send + Sync {
    async fn write(&self, items: &[Message]) -> Result<()>;
    async fn read_batch(&self, client_id: &str, batch_size: u16) -> Result<Vec<Message>>;
}

struct FileStorage {
    log_file_path: String,
    log_file_len: AtomicU64,
    readers: Mutex<HashMap<ClientID, Arc<Mutex<tokio::io::BufReader<tokio::fs::File>>>>>,
    notify: Arc<Notify>,
    writer: Mutex<tokio::io::BufWriter<tokio::fs::File>>,
}

impl FileStorage {
    async fn new(log_dir_path: &str) -> Result<Self> {
        tokio::fs::create_dir_all(log_dir_path)
            .await
            .context("creating log file path directory if not exists")?;

        let mut dir_file = File::open(log_dir_path).await?;
        dir_file.flush().await?;

        let log_file_path = format!("{log_dir_path}/data.log");

        let writer = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file_path)
            .await
            .with_context(|| {
                format!("creating/opening log file for writing: file_path={log_file_path}")
            })?;

        let notify = Arc::new(Notify::new());
        tokio::spawn(notify_on_interval(
            Arc::clone(&notify),
            Duration::from_secs(1),
        ));

        Ok(Self {
            log_file_path,
            log_file_len: AtomicU64::new(
                writer
                    .metadata()
                    .await
                    .context("reading log file length")?
                    .len(),
            ),
            notify,
            readers: Mutex::new(HashMap::new()),
            writer: Mutex::new(tokio::io::BufWriter::new(writer)),
        })
    }
}

async fn notify_on_interval(notify: Arc<Notify>, interval: Duration) {
    let mut interval = tokio::time::interval(interval);

    loop {
        interval.tick().await;
        notify.notify_waiters();
    }
}

#[async_trait]
impl Storage for FileStorage {
    async fn write(&self, items: &[Message]) -> Result<()> {
        let mut writer = self.writer.lock().await;

        let mut bytes_written = 0;

        for item in items {
            assert!(item.payload.len() <= u32::MAX as usize);
            writer
                .write_u32(item.payload.len() as u32)
                .await
                .context("writing payload length")?;

            bytes_written += std::mem::size_of::<u32>();

            writer
                .write_all(item.payload.as_bytes())
                .await
                .context("writing payload")?;

            bytes_written += item.payload.as_bytes().len();
        }

        writer
            .flush()
            .await
            .context("flushing log items to storage")?;

        let _ = self
            .log_file_len
            .fetch_add(bytes_written as u64, Ordering::SeqCst);

        self.notify.notify_waiters();

        Ok(())
    }

    async fn read_batch(&self, client_id: &str, batch_size: u16) -> Result<Vec<Message>> {
        let mut messages = Vec::with_capacity(batch_size as usize);

        let mut readers = self.readers.lock().await;

        let reader_arc = if let Some(reader) = readers.get(client_id) {
            reader.to_owned()
        } else {
            let reader = Arc::new(Mutex::new(tokio::io::BufReader::new(
                tokio::fs::OpenOptions::new()
                    .read(true)
                    .open(&self.log_file_path)
                    .await
                    .with_context(|| {
                        format!(
                            "opening log file for reading: file_path={}",
                            self.log_file_path
                        )
                    })?,
            )));
            readers.insert(client_id.to_owned(), Arc::clone(&reader));
            reader
        };

        drop(readers);

        let mut reader = reader_arc.lock().await;

        let position = reader
            .stream_position()
            .await
            .context("reading reader position")?;

        if self.log_file_len.load(Ordering::SeqCst) >= position {
            // Wait for new entries to be written to the log.
            self.notify.notified().await;
        }

        let log_file_len_at_start = self.log_file_len.load(Ordering::SeqCst);

        for _ in 0..batch_size {
            let position = reader
                .stream_position()
                .await
                .context("reading reader position")?;
            if position >= log_file_len_at_start {
                return Ok(messages);
            }

            let payload_len = match reader.read_u32().await {
                Err(err) => {
                    // Reached the end of the file.
                    if err.kind() == ErrorKind::UnexpectedEof {
                        return Ok(messages);
                    }
                    return Err(anyhow!("reading payload length: {err:?}"));
                }
                Ok(v) => v,
            };

            let mut payload = vec![0_u8; payload_len as usize];

            reader
                .read_exact(&mut payload)
                .await
                .context("reading payload")?;

            messages.push(Message {
                payload: String::from_utf8(payload)?,
            });
        }

        return Ok(messages);
    }
}

struct Deps {
    storage: Box<dyn Storage>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
struct Message {
    payload: String,
}

#[axum_macros::debug_handler]
async fn enqueue_handler(
    State(deps): State<Arc<Deps>>,
    Json(items): Json<Vec<Message>>,
) -> impl IntoResponse {
    match deps
        .storage
        .write(&items)
        .await
        .context("writing log entries")
    {
        Err(err) => {
            eprintln!("{}", err);

            axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
        Ok(()) => "OK".into_response(),
    }
}

#[derive(Debug, serde::Deserialize)]
struct ReceiveQueryParams {
    batch_size: u16,
}

#[axum_macros::debug_handler]
async fn receive_handler(
    State(deps): State<Arc<Deps>>,
    headers: HeaderMap,
    Query(params): Query<ReceiveQueryParams>,
) -> impl IntoResponse {
    let client_id = match headers.get(CLIENT_ID_HEADER) {
        None => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                format!("header {CLIENT_ID_HEADER} is required"),
            )
                .into_response()
        }
        Some(v) => v.to_str().unwrap(),
    };
    loop {
        match deps.storage.read_batch(client_id, params.batch_size).await {
            Err(err) => return err.to_string().into_response(),
            Ok(messages) => {
                if !messages.is_empty() {
                    return Json(messages).into_response();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use axum::{body::Body, extract::Request};
    use http_body_util::BodyExt as _; // for `collect`
    use tower::{Service, ServiceExt};

    fn temp_log_file_dir() -> String {
        format!("./dev/temp/{}", uuid::Uuid::new_v4())
    }

    trait RequestBuilderExt {
        fn json<T: serde::Serialize>(self, value: T) -> Result<Request<Body>>;
    }

    impl RequestBuilderExt for axum::http::request::Builder {
        fn json<T: serde::Serialize>(self, value: T) -> Result<Request<Body>> {
            self.header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                .method(http::Method::POST)
                .body(Body::from(
                    serde_json::to_string(&value).context("marshalling value to json")?,
                ))
                .context("setting request body")
        }
    }

    #[async_trait]
    trait ResponseExt {
        async fn json<T: serde::de::DeserializeOwned>(self) -> Result<T>;
        async fn text(self) -> Result<String>;
    }

    #[async_trait]
    impl ResponseExt for axum::response::Response<Body> {
        async fn json<T: serde::de::DeserializeOwned>(self) -> Result<T> {
            let bytes = self
                .into_body()
                .collect()
                .await
                .context("collecting responsed body bytes")?
                .to_bytes();
            serde_json::from_slice(bytes.as_ref()).context("unmarshalling request body")
        }

        async fn text(self) -> Result<String> {
            let bytes = self
                .into_body()
                .collect()
                .await
                .context("collecting responsed body bytes")?
                .to_bytes();

            let s = String::from_utf8_lossy(&bytes);
            Ok(s.to_string())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn enqueue_adds_items_to_queue() -> Result<()> {
        let app = router(&temp_log_file_dir()).await?;

        let response = app
            .oneshot(Request::builder().uri("/enqueue").json(&[Message {
                payload: "message".to_owned(),
            }])?)
            .await?;

        assert_eq!(http::StatusCode::OK, response.status());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn receive_uses_long_polling_to_consume_items_from_the_queue() -> Result<()> {
        let mut app = router(&temp_log_file_dir()).await?;

        // Try to receive messages. Should block because queue is empty.
        let handle = tokio::spawn(
            app.call(
                Request::builder()
                    .uri("/receive?batch_size=3")
                    .header("X-Client-ID", "1")
                    .method(http::Method::GET)
                    .body(Body::empty())?,
            ),
        );

        // Wait a little before adding messages to the queue.
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Add messages to the queue.
        let response = app
            .oneshot(Request::builder().uri("/enqueue").json(&[Message {
                payload: "payload 1".to_owned(),
            }])?)
            .await?;

        assert_eq!(http::StatusCode::OK, response.status());

        // The new messages should be received.
        let receive_response = handle.await??;

        assert_eq!(http::StatusCode::OK, receive_response.status());

        let messages: Vec<Message> = receive_response.json().await?;

        assert_eq!(
            vec![Message {
                payload: "payload 1".to_owned()
            }],
            messages
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn receive_different_consumers_can_consume_from_the_same_queue() -> Result<()> {
        let mut app = router(&temp_log_file_dir()).await?;

        // Add messages to the queue.
        let response = app
            .call(Request::builder().uri("/enqueue").json(&[
                Message {
                    payload: "payload 1".to_owned(),
                },
                Message {
                    payload: "payload 2".to_owned(),
                },
                Message {
                    payload: "payload 3".to_owned(),
                },
            ])?)
            .await?;
        assert_eq!(http::StatusCode::OK, response.status());

        // Read messages 1 and 2.
        let client_1_response = app
            .call(
                Request::builder()
                    .uri("/receive?batch_size=2")
                    .header("X-Client-ID", "1")
                    .method(http::Method::GET)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(http::StatusCode::OK, client_1_response.status());
        assert_eq!(
            vec![
                Message {
                    payload: "payload 1".to_owned()
                },
                Message {
                    payload: "payload 2".to_owned()
                }
            ],
            client_1_response.json::<Vec<Message>>().await?
        );

        // Read message 1.
        let client_2_response = app
            .call(
                Request::builder()
                    .uri("/receive?batch_size=1")
                    .header("X-Client-ID", "2")
                    .method(http::Method::GET)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(http::StatusCode::OK, client_2_response.status());
        assert_eq!(
            vec![Message {
                payload: "payload 1".to_owned()
            }],
            client_2_response.json::<Vec<Message>>().await?
        );

        // Read message 3.
        let client_1_response = app
            .call(
                Request::builder()
                    .uri("/receive?batch_size=2")
                    .header("X-Client-ID", "1")
                    .method(http::Method::GET)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(http::StatusCode::OK, client_1_response.status());
        assert_eq!(
            vec![Message {
                payload: "payload 3".to_owned()
            }],
            client_1_response.json::<Vec<Message>>().await?
        );

        // Read messages 2 and 3.
        let client_2_response = app
            .call(
                Request::builder()
                    .uri("/receive?batch_size=4")
                    .header("X-Client-ID", "2")
                    .method(http::Method::GET)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(http::StatusCode::OK, client_2_response.status());
        assert_eq!(
            vec![
                Message {
                    payload: "payload 2".to_owned()
                },
                Message {
                    payload: "payload 3".to_owned()
                }
            ],
            client_2_response.json::<Vec<Message>>().await?
        );

        Ok(())
    }
}
