use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use axum::response::IntoResponse;
use axum::extract::{Json, State};
use axum::routing::post;
use axum::Router;
use chrono::{DateTime, FixedOffset};
use chrono_tz::Tz;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::services::ServeDir;
use dashmap::DashMap;
use common::*;
use crate::datetime::*;

struct ServerState {
	data_directory: Arc<String>,
	ticker_cache: Arc<DashMap<String, Arc<OhlcArchive>>>
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetHistoryRequest {
	tickers: Vec<String>,
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
	tickers: Vec<String>,
	from: RelativeDateTime,
	to: RelativeDateTime
}

#[derive(Serialize)]
struct GetCorrelationResponse {
	correlation: Option<Vec<Vec<f64>>>,
	error: Option<String>
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OhlcRecordWeb {
	pub ticker: Option<String>,
	pub time: String,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
	pub volume: i32,
	pub open_interest: Option<i32>
}

impl ServerState {
	pub fn new(data_directory: String) -> ServerState {
		ServerState {
			data_directory: Arc::new(data_directory),
			ticker_cache: Arc::new(DashMap::new())
		}
	}
}

impl OhlcRecordWeb {
	pub fn new(record: &OhlcRecord, archive: &OhlcArchive) -> Result<OhlcRecordWeb, Box<dyn Error>> {
		let tz = Tz::from_str(&archive.time_zone)?;
		let time = get_date_time_string(record.time, &tz)?;
		Ok(OhlcRecordWeb {
			ticker: record.ticker.clone(),
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

pub async fn run(address: SocketAddr, data_directory: String) {
	println!("Running server on {}", address);
	let state = ServerState::new(data_directory);
	let state_arc = Arc::new(state);
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
	let response = match get_history_data(request, state) {
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
	let response = match get_correlation_data(request, state) {
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

fn get_history_data(request: GetHistoryRequest, state: Arc<ServerState>) -> Result<HashMap<String, Vec<OhlcRecordWeb>>, Box<dyn Error>> {
	let ticker_archives: Result<Vec<Arc<OhlcArchive>>, Box<dyn Error>> = request.tickers
		.iter()
		.map(|x| get_archive(&x, &state))
		.collect();
	let archives = ticker_archives?;
	let from_resolved = request.from.resolve(&request.to, &archives)?;
	let to_resolved = request.to.resolve(&request.from, &archives)?;
	let result: Result<Vec<Vec<OhlcRecordWeb>>, Box<dyn Error>> = archives
		.iter()
		.map(|archive| get_ohlc_records(&from_resolved, &to_resolved, request.time_frame, archive))
		.collect();
	match result {
		Ok(ticker_records) => {
			let tuples = request.tickers.into_iter().zip(ticker_records.into_iter()).collect();
			Ok(tuples)
		},
		Err(error) => Err(error)
	}
}

fn get_correlation_data(request: GetCorrelationRequest, state: Arc<ServerState>) -> Result<Vec<Vec<f64>>, Box<dyn Error>> {
	panic!("Not implemented");
}

fn get_archive(ticker: &String, state: &Arc<ServerState>) -> Result<Arc<OhlcArchive>, Box<dyn Error>> {
	// Simple directory traversal check
	let pattern = Regex::new("^[A-Z0-9]+$")?;
	if !pattern.is_match(ticker) {
		return Err("Invalid ticker".into());
	}
	if let Some(archive_ref) = state.ticker_cache.get(ticker) {
		Ok(Arc::clone(archive_ref.value()))
	}
	else {
		let file_name = get_archive_file_name(ticker);
		let data_directory = &*state.data_directory;
		let archive_path = Path::new(data_directory).join(file_name);
		let archive = read_archive(&archive_path)?;
		let archive_arc = Arc::new(archive);
		state.ticker_cache.insert(ticker.to_string(), Arc::clone(&archive_arc));
		Ok(archive_arc)
	}
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
			let ticker = first.ticker.clone();
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
				ticker,
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