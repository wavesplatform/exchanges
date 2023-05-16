#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("InvalidMessage: {0}")]
    InvalidMessage(String),
    #[error("DbError: {0}")]
    DbError(#[from] diesel::result::Error),
    #[error("StreamClosed: {0}")]
    StreamClosed(String),
    #[error("UpstreamAPIRequestError: {0}")]
    UpstreamAPIRequestError(#[from] wavesexchange_apis::Error),
    #[error("UpstreamAPIBadResponse: {0}")]
    UpstreamAPIBadResponse(String),
}
