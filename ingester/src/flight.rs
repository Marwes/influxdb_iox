//! Flight gRPC API for interaction with the query service

use crate::connection::Connection;
use arrow_flight::{flight_service_client::FlightServiceClient, HandshakeRequest};
use futures::{stream, StreamExt};
use rand::Rng;
use thiserror::Error;

/// Error responses when querying an IOx ingester using the Arrow Flight gRPC API.
#[derive(Debug, Error)]
pub enum Error {
    /// An unknown server error occurred. Contains the `tonic::Status` returned
    /// from the server.
    #[error(transparent)]
    GrpcError(#[from] tonic::Status),

    /// Arrow Flight handshake failed.
    #[error("Handshake failed")]
    HandshakeFailed,
}

/// An ingester Arrow Flight gRPC API client for the query service to use.
///
/// ```rust,no_run
/// #[tokio::main]
/// # async fn main() {
/// use ingester::{connection::Builder, flight::Client};
///
/// let connection = Builder::default()
///     .build("http://127.0.0.1:8082")
///     .await
///     .expect("client should be valid");
///
/// let mut client = Client::new(connection);
///
/// let mut query_results = client
///     .perform_query()
///     .await
///     .expect("query request should work");
///
/// let mut batches = vec![];
///
/// while let Some(data) = query_results.next().await.expect("valid batches") {
///     batches.push(data);
/// }
/// # }
/// ```
#[derive(Debug)]
pub struct Client {
    inner: FlightServiceClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: FlightServiceClient::new(channel),
        }
    }

    /// Perform a handshake with the server, as defined by the Arrow Flight API.
    pub async fn handshake(&mut self) -> Result<(), Error> {
        let request = HandshakeRequest {
            protocol_version: 0,
            payload: rand::thread_rng().gen::<[u8; 16]>().to_vec(),
        };
        let mut response = self
            .inner
            .handshake(stream::iter(vec![request.clone()]))
            .await?
            .into_inner();
        if request.payload.eq(&response
            .next()
            .await
            .ok_or(Error::HandshakeFailed)??
            .payload)
        {
            Result::Ok(())
        } else {
            Result::Err(Error::HandshakeFailed)
        }
    }
}
