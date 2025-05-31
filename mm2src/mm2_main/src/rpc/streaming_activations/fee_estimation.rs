//! RPC activation and deactivation for different fee estimation streamers.
use super::{EnableStreamingRequest, EnableStreamingResponse};

use coins::eth::fee_estimation::eth_fee_events::{EthFeeEventStreamer, EthFeeStreamingConfig};
use coins::{lp_coinfind, MmCoin, MmCoinEnum};
use common::HttpStatusCode;
use http::StatusCode;
use mm2_core::mm_ctx::MmArc;
use mm2_err_handle::{map_to_mm::MapToMmResult, mm_error::MmResult};
use std::fmt;

const FEE_STREAMING_SUPPORTED_COINS: &[&str] = &["EthCoin"];

#[derive(Deserialize)]
pub struct EnableFeeStreamingRequest {
    pub coin: String,
    pub config: EthFeeStreamingConfig,
}

#[derive(Serialize, SerializeErrorType)]
#[serde(tag = "error_type", content = "error_data")]
pub enum FeeStreamingRequestError {
    EnableError(String),
    CoinNotFound,
    CoinNotSupported { supported_coins: &'static [&'static str] },
    Internal(String),
}

impl fmt::Display for FeeStreamingRequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FeeStreamingRequestError::EnableError(msg) => write!(f, "EnableError: {}", msg),
            FeeStreamingRequestError::CoinNotFound => write!(f, "CoinNotFound"),
            FeeStreamingRequestError::CoinNotSupported { supported_coins } => {
                write!(f, "CoinNotSupported: {:?}", supported_coins)
            },
            FeeStreamingRequestError::Internal(msg) => write!(f, "Internal: {}", msg),
        }
    }
}

impl HttpStatusCode for FeeStreamingRequestError {
    fn status_code(&self) -> StatusCode {
        match self {
            FeeStreamingRequestError::EnableError(_) => StatusCode::BAD_REQUEST,
            FeeStreamingRequestError::CoinNotFound => StatusCode::NOT_FOUND,
            FeeStreamingRequestError::CoinNotSupported { .. } => StatusCode::NOT_IMPLEMENTED,
            FeeStreamingRequestError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

pub async fn enable_fee_estimation(
    ctx: MmArc,
    req: EnableStreamingRequest<EnableFeeStreamingRequest>,
) -> MmResult<EnableStreamingResponse, FeeStreamingRequestError> {
    let (client_id, req) = (req.client_id, req.inner);
    let coin = lp_coinfind(&ctx, &req.coin)
        .await
        .map_err(FeeStreamingRequestError::Internal)?
        .ok_or(FeeStreamingRequestError::CoinNotFound)?;

    match coin {
        MmCoinEnum::EthCoin(coin) => {
            let eth_fee_estimator_streamer = EthFeeEventStreamer::new(req.config, coin.clone());
            ctx.event_stream_manager
                .add(client_id, eth_fee_estimator_streamer, coin.spawner())
                .await
                .map(EnableStreamingResponse::new)
                .map_to_mm(|e| FeeStreamingRequestError::EnableError(format!("{e:?}")))
        },
        _ => Err(FeeStreamingRequestError::CoinNotSupported {
            supported_coins: FEE_STREAMING_SUPPORTED_COINS,
        })?,
    }
}
