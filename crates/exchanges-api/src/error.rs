use warp::reject::Reject;

impl Reject for Error {}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("DbError: {0}")]
    DbError(#[from] diesel::result::Error),
    #[error("ConnectionPoolError: {0}")]
    ConnectionPoolError(#[from] r2d2::Error),
    #[error("ValidationError: {0}")]
    ValidationError(String, Option<std::collections::HashMap<String, String>>),
    #[error("UpstreamAPIRequestError: {0}")]
    UpstreamAPIRequestError(#[from] wavesexchange_apis::Error),
    #[error("UpstreamAPIBadResponse: {0}")]
    UpstreamAPIBadResponse(String),
    #[error("CursorSerializationError: {0}")]
    CursorSerializationError(#[from] serde_json::Error),
    #[error("CursorDecodeError: {0}")]
    CursorDecodeError(#[from] base64::DecodeError),
}
