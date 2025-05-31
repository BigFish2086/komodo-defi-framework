//! RPC activation and deactivation for different balance event streamers.
use super::{EnableStreamingRequest, EnableStreamingResponse};

use coins::eth::eth_balance_events::EthBalanceEventStreamer;
use coins::tendermint::tendermint_balance_events::TendermintBalanceEventStreamer;
use coins::utxo::utxo_balance_events::UtxoBalanceEventStreamer;
use coins::z_coin::z_balance_streaming::ZCoinBalanceEventStreamer;
use coins::{lp_coinfind, MmCoin, MmCoinEnum};
use common::HttpStatusCode;
use http::StatusCode;
use mm2_core::mm_ctx::MmArc;
use mm2_err_handle::{map_to_mm::MapToMmResult, mm_error::MmResult};
use std::fmt;

use serde_json::Value as Json;

const BALANCE_STREAMING_SUPPORTED_COINS: &[&str] = &["UtxoCoin", "Bch", "QtumCoin", "EthCoin", "ZCoin", "Tendermint"];

#[derive(Deserialize)]
pub struct EnableBalanceStreamingRequest {
    pub coin: String,
    pub config: Option<Json>,
}

#[derive(Serialize, SerializeErrorType)]
#[serde(tag = "error_type", content = "error_data")]
pub enum BalanceStreamingRequestError {
    EnableError(String),
    CoinNotFound,
    CoinNotSupported { supported_coins: &'static [&'static str] },
    Internal(String),
}

impl fmt::Display for BalanceStreamingRequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BalanceStreamingRequestError::EnableError(msg) => write!(f, "EnableError: {}", msg),
            BalanceStreamingRequestError::CoinNotFound => write!(f, "CoinNotFound"),
            BalanceStreamingRequestError::CoinNotSupported { supported_coins } => {
                write!(f, "CoinNotSupported: {:?}", supported_coins)
            },
            BalanceStreamingRequestError::Internal(msg) => write!(f, "Internal: {}", msg),
        }
    }
}

impl HttpStatusCode for BalanceStreamingRequestError {
    fn status_code(&self) -> StatusCode {
        match self {
            BalanceStreamingRequestError::EnableError(_) => StatusCode::BAD_REQUEST,
            BalanceStreamingRequestError::CoinNotFound => StatusCode::NOT_FOUND,
            BalanceStreamingRequestError::CoinNotSupported { .. } => StatusCode::NOT_IMPLEMENTED,
            BalanceStreamingRequestError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

pub async fn enable_balance(
    ctx: MmArc,
    req: EnableStreamingRequest<EnableBalanceStreamingRequest>,
) -> MmResult<EnableStreamingResponse, BalanceStreamingRequestError> {
    let (client_id, req) = (req.client_id, req.inner);
    let coin = lp_coinfind(&ctx, &req.coin)
        .await
        .map_err(BalanceStreamingRequestError::Internal)?
        .ok_or(BalanceStreamingRequestError::CoinNotFound)?;

    match coin {
        MmCoinEnum::EthCoin(_) => (),
        MmCoinEnum::ZCoin(_)
        | MmCoinEnum::UtxoCoin(_)
        | MmCoinEnum::Bch(_)
        | MmCoinEnum::QtumCoin(_)
        | MmCoinEnum::Tendermint(_) => {
            if req.config.is_some() {
                Err(BalanceStreamingRequestError::EnableError(
                    "Invalid config provided. No config needed".to_string(),
                ))?
            }
        },
        _ => Err(BalanceStreamingRequestError::CoinNotSupported {
            supported_coins: BALANCE_STREAMING_SUPPORTED_COINS,
        })?,
    }

    let enable_result = match coin {
        MmCoinEnum::UtxoCoin(coin) => {
            let streamer = UtxoBalanceEventStreamer::new(coin.clone().into());
            ctx.event_stream_manager.add(client_id, streamer, coin.spawner()).await
        },
        MmCoinEnum::Bch(coin) => {
            let streamer = UtxoBalanceEventStreamer::new(coin.clone().into());
            ctx.event_stream_manager.add(client_id, streamer, coin.spawner()).await
        },
        MmCoinEnum::QtumCoin(coin) => {
            let streamer = UtxoBalanceEventStreamer::new(coin.clone().into());
            ctx.event_stream_manager.add(client_id, streamer, coin.spawner()).await
        },
        MmCoinEnum::EthCoin(coin) => {
            let streamer = EthBalanceEventStreamer::try_new(req.config, coin.clone())
                .map_to_mm(|e| BalanceStreamingRequestError::EnableError(format!("{e:?}")))?;
            ctx.event_stream_manager.add(client_id, streamer, coin.spawner()).await
        },
        MmCoinEnum::ZCoin(coin) => {
            let streamer = ZCoinBalanceEventStreamer::new(coin.clone());
            ctx.event_stream_manager.add(client_id, streamer, coin.spawner()).await
        },
        MmCoinEnum::Tendermint(coin) => {
            let streamer = TendermintBalanceEventStreamer::new(coin.clone());
            ctx.event_stream_manager.add(client_id, streamer, coin.spawner()).await
        },
        _ => Err(BalanceStreamingRequestError::CoinNotSupported {
            supported_coins: BALANCE_STREAMING_SUPPORTED_COINS,
        })?,
    };

    enable_result
        .map(EnableStreamingResponse::new)
        .map_to_mm(|e| BalanceStreamingRequestError::EnableError(format!("{e:?}")))
}
