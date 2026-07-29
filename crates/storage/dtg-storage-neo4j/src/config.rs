use std::fmt;
use std::io::Read;
use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use dtg_storage::StorageError;
use serde_json::{Value, json};

pub(crate) type QueryRows = Vec<Vec<Value>>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryApiContract;

impl QueryApiContract {
    #[must_use]
    pub const fn v2() -> Self {
        Self
    }

    #[must_use]
    pub fn database_path(self, database: &str) -> String {
        format!("/db/{database}/query/v2")
    }

    #[must_use]
    pub const fn begin_suffix(self) -> &'static str {
        "/tx"
    }

    #[must_use]
    pub fn continue_path(self, transaction_id: &str) -> String {
        format!("/tx/{transaction_id}")
    }

    #[must_use]
    pub fn commit_path(self, transaction_id: &str) -> String {
        format!("/tx/{transaction_id}/commit")
    }

    #[must_use]
    pub fn rollback_path(self, transaction_id: &str) -> String {
        self.continue_path(transaction_id)
    }

    #[must_use]
    pub const fn affinity_header(self) -> &'static str {
        "neo4j-cluster-affinity"
    }

    #[must_use]
    pub const fn transaction_id_pointer(self) -> &'static str {
        "/transaction/id"
    }

    #[must_use]
    pub const fn rows_pointer(self) -> &'static str {
        "/data/values"
    }

    #[must_use]
    pub const fn max_response_bytes(self) -> usize {
        64 * 1024 * 1024
    }

    #[must_use]
    pub fn request_body(self, statement: &str, parameters: Value) -> Value {
        json!({"statement": statement, "parameters": parameters})
    }
}

#[derive(Clone)]
pub struct Neo4jConfig {
    endpoint: String,
    database: String,
    username: String,
    password: String,
    timeout: Duration,
}

impl fmt::Debug for Neo4jConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Neo4jConfig")
            .field("endpoint", &self.endpoint)
            .field("database", &self.database)
            .field("username", &self.username)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl Neo4jConfig {
    pub fn new(
        endpoint: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let endpoint = endpoint.into().trim_end_matches('/').to_owned();
        let username = username.into();
        let password = password.into();
        if !(endpoint.starts_with("http://") || endpoint.starts_with("https://"))
            || username.is_empty()
            || password.is_empty()
        {
            return Err(StorageError::InvalidBinding(
                "Neo4j endpoint and credentials are incomplete".into(),
            ));
        }
        Ok(Self {
            endpoint,
            database: "neo4j".into(),
            username,
            password,
            timeout: Duration::from_secs(30),
        })
    }

    #[must_use]
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }

    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub(crate) fn client(&self) -> Result<QueryApiClient, StorageError> {
        if self.database.is_empty()
            || !self
                .database
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(StorageError::InvalidBinding(
                "Neo4j database name is not canonical".into(),
            ));
        }
        let credentials = STANDARD.encode(format!("{}:{}", self.username, self.password));
        Ok(QueryApiClient {
            agent: ureq::AgentBuilder::new().timeout(self.timeout).build(),
            url: format!(
                "{}{}",
                self.endpoint,
                QueryApiContract::v2().database_path(&self.database)
            ),
            authorization: format!("Basic {credentials}"),
        })
    }
}

#[derive(Clone)]
pub(crate) struct QueryApiClient {
    agent: ureq::Agent,
    url: String,
    authorization: String,
}

impl QueryApiClient {
    pub(crate) async fn execute(
        &self,
        statement: impl Into<String>,
        parameters: Value,
    ) -> Result<QueryRows, StorageError> {
        let client = self.clone();
        let statement = statement.into();
        tokio::task::spawn_blocking(move || {
            client.execute_at(&client.url, None, &statement, parameters)
        })
        .await
        .map_err(join_error)?
    }

    pub(crate) async fn begin_transaction(
        &self,
        statement: impl Into<String>,
        parameters: Value,
    ) -> Result<(QueryApiTransaction, QueryRows), StorageError> {
        let client = self.clone();
        let statement = statement.into();
        tokio::task::spawn_blocking(move || client.begin_transaction_sync(&statement, parameters))
            .await
            .map_err(join_error)?
    }

    fn begin_transaction_sync(
        &self,
        statement: &str,
        parameters: Value,
    ) -> Result<(QueryApiTransaction, QueryRows), StorageError> {
        let url = format!("{}{}", self.url, QueryApiContract::v2().begin_suffix());
        let response = self.post(&url, None, statement, parameters)?;
        let affinity = response
            .header(QueryApiContract::v2().affinity_header())
            .map(str::to_owned);
        let body = response_json(response)?;
        reject_query_errors(&body)?;
        let transaction_id = body
            .pointer(QueryApiContract::v2().transaction_id_pointer())
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                StorageError::Internal(
                    "Neo4j Query API transaction response omitted transaction ID".into(),
                )
            })?;
        let rows = query_rows(&body)?;
        Ok((
            QueryApiTransaction {
                client: self.clone(),
                url: format!("{url}/{transaction_id}"),
                affinity,
                open: true,
            },
            rows,
        ))
    }

    fn execute_at(
        &self,
        url: &str,
        affinity: Option<&str>,
        statement: &str,
        parameters: Value,
    ) -> Result<QueryRows, StorageError> {
        let body = response_json(self.post(url, affinity, statement, parameters)?)?;
        reject_query_errors(&body)?;
        query_rows(&body)
    }

    fn post(
        &self,
        url: &str,
        affinity: Option<&str>,
        statement: &str,
        parameters: Value,
    ) -> Result<ureq::Response, StorageError> {
        let mut request = self
            .agent
            .post(url)
            .set("Accept", "application/json")
            .set("Authorization", &self.authorization);
        if let Some(affinity) = affinity {
            request = request.set(QueryApiContract::v2().affinity_header(), affinity);
        }
        request
            .send_json(QueryApiContract::v2().request_body(statement, parameters))
            .map_err(query_api_error)
    }
}

pub(crate) struct QueryApiTransaction {
    client: QueryApiClient,
    url: String,
    affinity: Option<String>,
    open: bool,
}

impl QueryApiTransaction {
    pub(crate) async fn execute(
        &self,
        statement: impl Into<String>,
        parameters: Value,
    ) -> Result<QueryRows, StorageError> {
        let client = self.client.clone();
        let url = self.url.clone();
        let affinity = self.affinity.clone();
        let statement = statement.into();
        tokio::task::spawn_blocking(move || {
            client.execute_at(&url, affinity.as_deref(), &statement, parameters)
        })
        .await
        .map_err(join_error)?
    }

    pub(crate) async fn commit(mut self) -> Result<(), StorageError> {
        let client = self.client.clone();
        let url = format!("{}/commit", self.url);
        let affinity = self.affinity.clone();
        tokio::task::spawn_blocking(move || {
            client.execute_at(&url, affinity.as_deref(), "RETURN 1", json!({}))
        })
        .await
        .map_err(join_error)??;
        self.open = false;
        Ok(())
    }

    pub(crate) async fn rollback(mut self) -> Result<(), StorageError> {
        let client = self.client.clone();
        let url = self.url.clone();
        let affinity = self.affinity.clone();
        tokio::task::spawn_blocking(move || rollback_sync(&client, &url, affinity.as_deref()))
            .await
            .map_err(join_error)??;
        self.open = false;
        Ok(())
    }
}

impl Drop for QueryApiTransaction {
    fn drop(&mut self) {
        if self.open {
            self.open = false;
            let client = self.client.clone();
            let url = self.url.clone();
            let affinity = self.affinity.clone();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn_blocking(move || {
                    let _ = rollback_sync(&client, &url, affinity.as_deref());
                });
            } else {
                let _ = rollback_sync(&client, &url, affinity.as_deref());
            }
        }
    }
}

fn rollback_sync(
    client: &QueryApiClient,
    url: &str,
    affinity: Option<&str>,
) -> Result<(), StorageError> {
    let mut request = client
        .agent
        .delete(url)
        .set("Accept", "application/json")
        .set("Authorization", &client.authorization);
    if let Some(affinity) = affinity {
        request = request.set(QueryApiContract::v2().affinity_header(), affinity);
    }
    request.call().map(|_| ()).map_err(query_api_error)
}

fn response_json(response: ureq::Response) -> Result<Value, StorageError> {
    let bytes = bounded_response_bytes(response)?;
    serde_json::from_slice(&bytes)
        .map_err(|error| StorageError::Internal(format!("invalid Neo4j Query API JSON: {error}")))
}

fn reject_query_errors(body: &Value) -> Result<(), StorageError> {
    if let Some(errors) = body.get("errors").and_then(Value::as_array)
        && !errors.is_empty()
    {
        return Err(StorageError::Internal(format!(
            "Neo4j Query API error: {}",
            errors[0]
        )));
    }
    Ok(())
}

fn query_rows(body: &Value) -> Result<QueryRows, StorageError> {
    body.pointer(QueryApiContract::v2().rows_pointer())
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    row.as_array().cloned().ok_or_else(|| {
                        StorageError::Internal("Neo4j result row is not an array".into())
                    })
                })
                .collect()
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

fn query_api_error(error: ureq::Error) -> StorageError {
    match error {
        ureq::Error::Status(status, response) => {
            let body = bounded_response_bytes(response)
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                .unwrap_or_else(|error| error.to_string());
            StorageError::Internal(format!("Neo4j Query API returned HTTP {status}: {body}"))
        }
        ureq::Error::Transport(error) => {
            StorageError::Internal(format!("Neo4j Query API transport failed: {error}"))
        }
    }
}

fn bounded_response_bytes(response: ureq::Response) -> Result<Vec<u8>, StorageError> {
    let limit = QueryApiContract::v2().max_response_bytes();
    if response
        .header("Content-Length")
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > limit)
    {
        return Err(StorageError::Internal(
            "Neo4j Query API response exceeds the provider bound".into(),
        ));
    }
    let mut bytes = Vec::new();
    response
        .into_reader()
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            StorageError::Internal(format!("failed to read Neo4j Query API response: {error}"))
        })?;
    if bytes.len() > limit {
        return Err(StorageError::Internal(
            "Neo4j Query API response exceeds the provider bound".into(),
        ));
    }
    Ok(bytes)
}

fn join_error(error: tokio::task::JoinError) -> StorageError {
    StorageError::Internal(format!("Neo4j blocking Query API task failed: {error}"))
}
