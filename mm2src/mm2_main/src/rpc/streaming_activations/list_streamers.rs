//! RPC to list all active streamers.
use common::HttpStatusCode;
use http::StatusCode;
use mm2_core::mm_ctx::MmArc;
use mm2_err_handle::{map_to_mm::MapToMmResult, mm_error::MmResult};
use mm2_event_stream::StreamerId;

#[derive(Deserialize)]
pub struct ListStreamersRequest;

/// The success/ok response for listing the currently active streamers_ids request.
#[derive(Serialize)]
pub struct ListStreamersResponse {
    active_streamers_ids: Vec<StreamerId>,
}

impl ListStreamersResponse {
    pub fn new(streamers_ids: Vec<StreamerId>) -> Self {
        Self {
            active_streamers_ids: streamers_ids,
        }
    }
}

#[derive(Display, Serialize, SerializeErrorType)]
#[serde(tag = "error_type", content = "error_data")]
/// The error response for listing the currently active streamers_ids request.
pub enum ListStreamersRequestError {
    ListStreamersError(String),
}

impl HttpStatusCode for ListStreamersRequestError {
    fn status_code(&self) -> StatusCode { StatusCode::BAD_REQUEST }
}

pub async fn list_active_streamers_ids(
    ctx: MmArc,
    _req: ListStreamersRequest,
) -> MmResult<ListStreamersResponse, ListStreamersRequestError> {
    let active_ids = ctx
        .event_stream_manager
        .get_active_streamers_ids()
        .map_to_mm(|e| ListStreamersRequestError::ListStreamersError(format!("{e:?}")))?;

    Ok(ListStreamersResponse::new(active_ids))
}
