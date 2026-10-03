//! Test harness for pgvis integration tests.
//!
//! Starts a pgvis server in-process on a random port, sets up the test database,
//! and provides an HTTP client for making requests.

#![allow(dead_code)]

use std::net::TcpListener;
use std::time::Duration;

use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Client, Response, StatusCode};
use serde_json::Value;

/// The DSN used by tests. Defaults to local socket connection.
pub fn test_dsn() -> String {
    std::env::var("PGVIS_TEST_DSN").unwrap_or_else(|_| {
        let user = std::env::var("USER").unwrap_or_else(|_| "postgres".to_string());
        format!("postgres://{user}@localhost/postgres")
    })
}

/// Find a free port on localhost.
pub fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind random port");
    listener.local_addr().unwrap().port()
}

/// A running pgvis server instance for testing (in-process).
pub struct PgvisServer {
    pub port: u16,
    pub client: Client,
    pub base_url: String,
    _server_handle: tokio::task::JoinHandle<()>,
}

impl PgvisServer {
    /// Start a pgvis server in-process on a random port with the given schema.
    ///
    /// The caller must ensure the test schema is already loaded in the DB.
    pub async fn start(dsn: &str, schema: &str) -> Self {
        let config = pgvis_core::Config {
            schemas: vec![schema.to_string()],
            ..Default::default()
        };
        Self::start_with_config(dsn, config).await
    }

    /// Start a pgvis server in-process on a random port with the given config.
    #[allow(dead_code)] // not every test binary uses it
    pub async fn start_with_config(dsn: &str, config: pgvis_core::Config) -> Self {
        let port = free_port();
        let bind_addr = format!("127.0.0.1:{port}");

        let router = pgvis_lib::Builder::new(dsn)
            .config(config)
            .build()
            .await
            .expect("failed to build pgvis router");

        let listener = tokio::net::TcpListener::bind(&bind_addr)
            .await
            .expect("failed to bind");

        let server_handle = tokio::spawn(async move {
            axum::serve(listener, router).await.ok();
        });

        let base_url = format!("http://127.0.0.1:{port}");
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        let server = Self {
            port,
            client,
            base_url,
            _server_handle: server_handle,
        };

        // Wait for the server to become ready
        server.wait_ready().await;
        server
    }

    /// Wait until the server accepts connections (max 5 seconds).
    async fn wait_ready(&self) {
        let url = format!("{}/", self.base_url);
        for _ in 0..50 {
            match self.client.get(&url).send().await {
                Ok(_) => return,
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
        panic!(
            "pgvis server did not become ready within 5s on port {}",
            self.port
        );
    }

    /// GET request to `/{path}`.
    pub async fn get(&self, path: &str) -> Response {
        self.client
            .get(format!("{}{path}", self.base_url))
            .send()
            .await
            .expect("GET request failed")
    }

    /// GET request with custom headers.
    pub async fn get_with_headers(&self, path: &str, headers: HeaderMap) -> Response {
        self.client
            .get(format!("{}{path}", self.base_url))
            .headers(headers)
            .send()
            .await
            .expect("GET request failed")
    }

    /// POST request with JSON body.
    pub async fn post(&self, path: &str, body: &Value) -> Response {
        self.client
            .post(format!("{}{path}", self.base_url))
            .header(CONTENT_TYPE, "application/json")
            .json(body)
            .send()
            .await
            .expect("POST request failed")
    }

    /// POST request with JSON body and custom headers.
    pub async fn post_with_headers(
        &self,
        path: &str,
        body: &Value,
        headers: HeaderMap,
    ) -> Response {
        self.client
            .post(format!("{}{path}", self.base_url))
            .header(CONTENT_TYPE, "application/json")
            .headers(headers)
            .json(body)
            .send()
            .await
            .expect("POST request failed")
    }

    /// PATCH request with JSON body.
    pub async fn patch(&self, path: &str, body: &Value) -> Response {
        self.client
            .patch(format!("{}{path}", self.base_url))
            .header(CONTENT_TYPE, "application/json")
            .json(body)
            .send()
            .await
            .expect("PATCH request failed")
    }

    /// PATCH request with custom headers.
    pub async fn patch_with_headers(
        &self,
        path: &str,
        body: &Value,
        headers: HeaderMap,
    ) -> Response {
        self.client
            .patch(format!("{}{path}", self.base_url))
            .header(CONTENT_TYPE, "application/json")
            .headers(headers)
            .json(body)
            .send()
            .await
            .expect("PATCH request failed")
    }

    /// DELETE request.
    pub async fn delete(&self, path: &str) -> Response {
        self.client
            .delete(format!("{}{path}", self.base_url))
            .send()
            .await
            .expect("DELETE request failed")
    }

    /// DELETE request with custom headers.
    pub async fn delete_with_headers(&self, path: &str, headers: HeaderMap) -> Response {
        self.client
            .delete(format!("{}{path}", self.base_url))
            .headers(headers)
            .send()
            .await
            .expect("DELETE request failed")
    }
}

/// Run schema.sql and seed.sql against the test database, once per test binary.
///
/// A binary may start several shared fixtures concurrently (rpc.rs runs an HTTP
/// server and an in-process state); schema.sql drops and recreates the schema,
/// so a second run would pull it out from under tests already using the first.
pub async fn setup_test_db(dsn: &str) {
    static DONE: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    DONE.get_or_init(|| load_fixtures(dsn)).await;
}

async fn load_fixtures(dsn: &str) {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("failed to connect to test database");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("test db connection error: {e}");
        }
    });

    let schema_sql = include_str!("../fixtures/schema.sql");
    let seed_sql = include_str!("../fixtures/seed.sql");

    client
        .batch_execute(schema_sql)
        .await
        .expect("failed to execute schema.sql");

    client
        .batch_execute(seed_sql)
        .await
        .expect("failed to execute seed.sql");
}

/// Helper to parse a JSON response body.
pub async fn json_body(response: Response) -> Value {
    response
        .json::<Value>()
        .await
        .expect("failed to parse JSON body")
}

/// Helper to assert response status and return parsed JSON.
pub async fn assert_json(response: Response, expected_status: StatusCode) -> Value {
    let status = response.status();
    let body = json_body(response).await;
    assert_eq!(
        status, expected_status,
        "expected {expected_status}, got {status}. Body: {body}"
    );
    body
}

/// Prefer header builder.
pub fn prefer(value: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("Prefer", HeaderValue::from_str(value).unwrap());
    headers
}

/// Accept header builder.
pub fn accept(value: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_str(value).unwrap());
    headers
}
