use std::{fmt, str::FromStr};

use dtg_storage::{NamespaceId, StorageError};
use tokio_postgres::{Client, Config, NoTls};

#[derive(Clone)]
pub struct PostgresConfig {
    database: Config,
}

impl fmt::Debug for PostgresConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresConfig")
            .finish_non_exhaustive()
    }
}

impl PostgresConfig {
    pub fn new(connection_string: impl AsRef<str>) -> Result<Self, StorageError> {
        let database = Config::from_str(connection_string.as_ref()).map_err(|error| {
            StorageError::InvalidBinding(format!("invalid PostgreSQL connection profile: {error}"))
        })?;
        Ok(Self { database })
    }

    pub(crate) async fn connect(&self, schema_name: &str) -> Result<Client, StorageError> {
        let client = self.connect_unscoped().await?;
        client
            .batch_execute(&format!("SET search_path TO {schema_name}, pg_catalog"))
            .await
            .map_err(postgres_error)?;
        Ok(client)
    }

    pub(crate) async fn connect_unscoped(&self) -> Result<Client, StorageError> {
        let (client, connection) = self.database.connect(NoTls).await.map_err(postgres_error)?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }
}

pub(crate) fn schema_name(namespace: &NamespaceId) -> String {
    let mut high = 0x6c62_272e_07bb_0142_u64;
    let mut low = 0x62b8_2175_6295_c58d_u64;
    for byte in b"dtg-postgres-schema-v1"
        .iter()
        .chain(namespace.as_str().as_bytes())
    {
        high ^= u64::from(*byte);
        high = high.wrapping_mul(0x0000_0100_0000_01b3);
        low ^= u64::from(*byte);
        low = low.rotate_left(5).wrapping_mul(0x9e37_79b1_85eb_ca87);
    }
    let mut name = String::from("dtg_");
    use std::fmt::Write;
    write!(&mut name, "{high:016x}{low:016x}").expect("writing to a String cannot fail");
    name
}

pub(crate) fn postgres_error(error: tokio_postgres::Error) -> StorageError {
    StorageError::Internal(format!("PostgreSQL provider error: {error}"))
}
