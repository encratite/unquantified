use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use axum::response::IntoResponse;
use axum::extract::{Json, State};
use axum::routing::post;
use axum::Router;
use chrono::{DateTime, FixedOffset};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::services::ServeDir;

use common::*;
use crate::correlation::*;
use crate::datetime::*;
use crate::manager::AssetManager;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetHistoryRequest {
	symbols: Vec<String>,
	from: RelativeDateTime,
	to: RelativeDateTime,
	// Minutes, 1440 for daily data
	time_frame: u16
}

#[derive(Serialize)]
struct GetHistoryResponse {
	tickers: Option<HashMap<String, Vec<OhlcRecordWeb>>>,
	error: Option<String>
}

#[derive(Deserialize)]
struct GetCorrelationRequest {
	symbols: Vec<String>,
	from: RelativeDateTime,
	to: RelativeDateTime
}

#[derive(Serialize)]
struct GetCorrelationResponse {
	correlation: Option<CorrelationData>,
	error: Option<String>
}

#[derive(Serialize)]
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
	pub fn new(record: &OhlcRecord, archive: &OhlcArchive) -> Result<OhlcRecordWeb, Box<dyn Error>> {
		let tz = Tz::from_str(&archive.time_zone)?;
		let time = get_date_time_string(record.time, &tz)?;
		Ok(OhlcRecordWeb {
			symbol: record.symbol.clone(),
			time: time,
			open: record.open,
			high: record.high,
			low: record.low,
			close: record.close,
			volume: record.volume,
			open_interest: record.open_interest
		})
	}
}

pub async fn run(address: SocketAddr, ticker_directory: String, assets_path: String) {
	println!("Running server on {}", address);
	let mut manager = AssetManager::new(ticker_directory, assets_path);
	let state_arc = Arc::new(manager);
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
	State(manager): State<Arc<AssetManager>>,
	Json(request): Json<GetHistoryRequest>
) -> impl IntoResponse {
	let response = match get_history_data(request, manager) {
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
	State(manager): State<Arc<AssetManager>>,
	Json(request): Json<GetCorrelationRequest>
) -> impl IntoResponse {
	let response = match get_correlation_data(request, manager) {
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

fn get_history_data(request: GetHistoryRequest, manager: Arc<AssetManager>) -> Result<HashMap<String, Vec<OhlcRecordWeb>>, Box<dyn Error>> {
	let resolved_symbols = manager.resolve_symbols(&request.symbols)?;
	let archives = get_ticker_archives(&resolved_symbols, &manager)?;
	let from_resolved = request.from.resolve(&request.to, &archives)?;
	let to_resolved = request.to.resolve(&request.from, &archives)?;
	let result: Result<Vec<Vec<OhlcRecordWeb>>, Box<dyn Error>> = archives
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

fn get_ticker_archives(symbols: &Vec<String>, manager: &Arc<AssetManager>) -> Result<Vec<Arc<OhlcArchive>>, Box<dyn Error>> {
	symbols
		.iter()
		.map(|x| manager.get_archive(&x))
		.collect()
}

fn get_correlation_data(request: GetCorrelationRequest, manager: Arc<AssetManager>) -> Result<CorrelationData, Box<dyn Error>> {
	let resolved_symbols = manager.resolve_symbols(&request.symbols)?;
	let archives = get_ticker_archives(&resolved_symbols, &manager)?;
	let from = request.from.resolve(&request.to, &archives)?;
	let to = request.to.resolve(&request.from, &archives)?;
	get_correlation_matrix(resolved_symbols, from, to, archives)
}

fn get_ohlc_records(from: &DateTime<FixedOffset>, to: &DateTime<FixedOffset>, time_frame: u16, archive: &Arc<OhlcArchive>) -> Result<Vec<OhlcRecordWeb>, Box<dyn Error>> {
	let tz = Tz::from_str(archive.time_zone.as_str())?;
	if time_frame >= 1440 {
		return get_raw_records_from_archive(from, to, &tz, &archive.daily, &archive)
	}
	else if time_frame == archive.intraday_time_frame {
		return get_raw_records_from_archive(from, to, &tz, &archive.intraday, &archive);
	}
	else if time_frame < archive.intraday_time_frame {
		return Err("Requested time frame too small for intraday data in archive".into());
	}
	else if time_frame % archive.intraday_time_frame != 0 {
		let message = format!("Requested time frame must be a multiple of {}", archive.intraday_time_frame);
		return Err(message.into());
	}
	let chunk_size = (time_frame / archive.intraday_time_frame) as usize;
	// This doesn't merge continuous contracts correctly
	archive.intraday
		.iter()
		.filter(|x| matches_from_to(from, to, &tz, x))
		.collect::<Vec<_>>()
		.chunks(chunk_size)
		.filter(|x| x.len() == chunk_size)
		.map(|x| -> Result<OhlcRecordWeb, Box<dyn Error>> {
			let first = x.first().unwrap();
			let last = x.last().unwrap();
			let symbol = first.symbol.clone();
			let time = get_date_time_string(first.time, &tz)?;
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

fn matches_from_to(from: &DateTime<FixedOffset>, to: &DateTime<FixedOffset>, tz: &Tz, record: &OhlcRecord) -> bool {
	let time = get_date_time_tz(record.time, tz);
	time >= *from && time < *to
}

fn get_raw_records_from_archive(from: &DateTime<FixedOffset>, to: &DateTime<FixedOffset>, tz: &Tz, records: &Vec<OhlcRecord>, archive: &Arc<OhlcArchive>) -> Result<Vec<OhlcRecordWeb>, Box<dyn Error>> {
	records
		.iter()
		.filter(|x| matches_from_to(from, to, tz, x))
		.map(|x| OhlcRecordWeb::new(&x, archive))
		.collect()
}