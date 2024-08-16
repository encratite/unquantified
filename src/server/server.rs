use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::{response::IntoResponse, extract::{Json, State}, routing::post, Router};
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::services::ServeDir;
use anyhow::{Context, Result, anyhow};

use common::*;
use crate::backtest::BacktestConfiguration;
use crate::correlation::*;
use crate::datetime::*;
use crate::manager::AssetManager;

struct ServerState {
	asset_manager: AssetManager,
	backtest_configuration: BacktestConfiguration
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetHistoryRequest {
	symbols: Vec<String>,
	from: RelativeDateTime,
	to: RelativeDateTime,
	// Minutes, 1440 for daily data
	time_frame: u16
}

#[derive(Debug, Serialize)]
struct GetHistoryResponse {
	tickers: Option<HashMap<String, Vec<OhlcRecordWeb>>>,
	error: Option<String>
}

#[derive(Debug, Deserialize)]
struct GetCorrelationRequest {
	symbols: Vec<String>,
	from: RelativeDateTime,
	to: RelativeDateTime
}

#[derive(Debug, Serialize)]
struct GetCorrelationResponse {
	correlation: Option<CorrelationData>,
	error: Option<String>
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OhlcRecordWeb {
	pub symbol: String,
	pub time: String,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
	pub volume: u32,
	pub open_interest: Option<u32>
}

impl OhlcRecordWeb {
	pub fn new(record: &OhlcRecord) -> OhlcRecordWeb {
		OhlcRecordWeb {
			symbol: record.symbol.clone(),
			time: record.time.to_rfc3339(),
			open: record.open,
			high: record.high,
			low: record.low,
			close: record.close,
			volume: record.volume,
			open_interest: record.open_interest
		}
	}
}

pub async fn run(address: SocketAddr, ticker_directory: String, assets_path: String, backtest_configuration: BacktestConfiguration) {
	println!("Running server on {}", address);
	let asset_manager = AssetManager::new(ticker_directory, assets_path);
	let server_state = ServerState {
		asset_manager,
		backtest_configuration
	};
	let state_arc = Arc::new(server_state);
	let serve_dir = ServeDir::new("web");
	let app = Router::new()
		.route("/history", post(get_history))
		.route("/correlation", post(get_correlation))
		.with_state(state_arc)
		.fallback_service(serve_dir);
	let listener = TcpListener::bind(address).await.unwrap();
	axum::serve(listener, app).await.unwrap();
}

async fn get_history(
	State(state): State<Arc<ServerState>>,
	Json(request): Json<GetHistoryRequest>
) -> impl IntoResponse {
	let response = match get_history_data(request, &state.asset_manager) {
		Ok(data) => GetHistoryResponse {
			tickers: Some(data),
			error: None
		},
		Err(error) => GetHistoryResponse {
			tickers: None,
			error: Some(error.to_string())
		}
	};
	Json(response)
}

async fn get_correlation(
	State(state): State<Arc<ServerState>>,
	Json(request): Json<GetCorrelationRequest>
) -> impl IntoResponse {
	let response = match get_correlation_data(request, &state.asset_manager) {
		Ok(data) => GetCorrelationResponse {
			correlation: Some(data),
			error: None
		},
		Err(error) => GetCorrelationResponse {
			correlation: None,
			error: Some(error.to_string())
		}
	};
	Json(response)
}

fn get_history_data(request: GetHistoryRequest, asset_manager: &AssetManager) -> Result<HashMap<String, Vec<OhlcRecordWeb>>> {
	let resolved_symbols = asset_manager.resolve_symbols(&request.symbols)?;
	let archives = get_ticker_archives(&resolved_symbols, asset_manager)?;
	let from_resolved = request.from.resolve(&request.to, &archives)?;
	let to_resolved = request.to.resolve(&request.from, &archives)?;
	let result: Result<Vec<Vec<OhlcRecordWeb>>> = archives
		.iter()
		.map(|archive| get_ohlc_records(&from_resolved, &to_resolved, request.time_frame, archive))
		.collect();
	match result {
		Ok(ticker_records) => {
			let tuples = resolved_symbols.into_iter().zip(ticker_records.into_iter()).collect();
			Ok(tuples)
		},
		Err(error) => Err(error)
	}
}

fn get_ticker_archives(symbols: &Vec<String>, asset_manager: &AssetManager) -> Result<Vec<Arc<OhlcArchive>>> {
	symbols
		.iter()
		.map(|x| asset_manager.get_archive(&x))
		.collect()
}

fn get_correlation_data(request: GetCorrelationRequest, asset_manager: &AssetManager) -> Result<CorrelationData> {
	let resolved_symbols = asset_manager.resolve_symbols(&request.symbols)?;
	let archives = get_ticker_archives(&resolved_symbols, asset_manager)?;
	let from = request.from.resolve(&request.to, &archives)?;
	let to = request.to.resolve(&request.from, &archives)?;
	get_correlation_matrix(resolved_symbols, from, to, archives)
}

fn get_ohlc_records(from: &DateTime<FixedOffset>, to: &DateTime<FixedOffset>, time_frame: u16, archive: &Arc<OhlcArchive>) -> Result<Vec<OhlcRecordWeb>> {
	if time_frame >= 1440 {
		return Ok(get_raw_records_from_archive(from, to, &archive.daily.unadjusted));
	} else if time_frame == archive.intraday_time_frame {
		return Ok(get_raw_records_from_archive(from, to, &archive.intraday.unadjusted));
	} else if time_frame < archive.intraday_time_frame {
		return Err(anyhow!("Requested time frame too small for intraday data in archive"));
	} else if time_frame % archive.intraday_time_frame != 0 {
		let message = format!("Requested time frame must be a multiple of {}", archive.intraday_time_frame);
		return Err(anyhow!(message));
	}
	let chunk_size = (time_frame / archive.intraday_time_frame) as usize;
	archive.intraday.unadjusted
		.iter()
		.filter(|x| matches_from_to(from, to, x))
		.collect::<Vec<_>>()
		.chunks(chunk_size)
		.filter(|x| x.len() == chunk_size)
		.map(|x| -> Result<OhlcRecordWeb> {
			let first = x.first().unwrap();
			let last = x.last().unwrap();
			let symbol = first.symbol.clone();
			let time = first.time.to_rfc3339();
			let open = first.open;
			let high = x
				.iter()
				.max_by(|x, y| x.high.partial_cmp(&y.high).unwrap())
				.unwrap()
				.high;
			let low = x
				.iter()
				.min_by(|x, y| x.low.partial_cmp(&y.low).unwrap())
				.unwrap()
				.low;
			let close = last.close;
			let volume = x.iter().map(|x| x.volume).sum();
			let open_interest = x.iter().map(|x| x.open_interest).sum();
			Ok(OhlcRecordWeb {
				symbol,
				time,
				open,
				high,
				low,
				close,
				volume,
				open_interest
			})
		})
		.collect()
}

fn matches_from_to(from: &DateTime<FixedOffset>, to: &DateTime<FixedOffset>, record: &OhlcRecord) -> bool {
	record.time >= *from && record.time < *to
}

fn get_raw_records_from_archive<'a, T>(from: &DateTime<FixedOffset>, to: &DateTime<FixedOffset>, records: T) -> Vec<OhlcRecordWeb>
where
	T: IntoIterator<Item = &'a OhlcArc>,
{
	records
		.into_iter()
		.filter(|x| matches_from_to(from, to, x))
		.map(|x| OhlcRecordWeb::new(&**x))  // Dereference twice to get the OhlcRecord
		.collect()
}