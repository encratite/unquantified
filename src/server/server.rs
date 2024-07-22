use std::collections::HashMap;
use std::error::Error;
use std::fs;
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
use crate::backtest::Asset;
use crate::correlation::*;
use crate::datetime::*;

struct ServerState {
	ticker_directory: String,
	ticker_cache: DashMap<String, Arc<OhlcArchive>>,
	assets: HashMap<String, Asset>
}

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

impl ServerState {
	pub fn new(data_directory: String, assets: HashMap<String, Asset>) -> ServerState {
		ServerState {
			ticker_directory: data_directory,
			ticker_cache: DashMap::new(),
			assets: assets
		}
	}
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
	let assets = load_assets(assets_path);
	let mut state = ServerState::new(ticker_directory, assets);
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

fn load_assets(csv_path: String) -> HashMap<String, Asset> {
	let mut assets = HashMap::new();
	read_csv::<Asset>(csv_path.into(), |record| {
		assets.insert(record.symbol.clone(), record);
	});
	return assets;
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
	let resolved_symbols = resolve_symbols(&request.symbols, &state)?;
	let archives = get_ticker_archives(&resolved_symbols, &state)?;
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

fn get_ticker_archives(symbols: &Vec<String>, state: &Arc<ServerState>) -> Result<Vec<Arc<OhlcArchive>>, Box<dyn Error>> {
	symbols
		.iter()
		.map(|x| get_archive(&x, &state))
		.collect()
}

fn get_correlation_data(request: GetCorrelationRequest, state: Arc<ServerState>) -> Result<CorrelationData, Box<dyn Error>> {
	let resolved_symbols = resolve_symbols(&request.symbols, &state)?;
	let archives = get_ticker_archives(&resolved_symbols, &state)?;
	let from = request.from.resolve(&request.to, &archives)?;
	let to = request.to.resolve(&request.from, &archives)?;
	get_correlation_matrix(resolved_symbols, from, to, archives)
}

fn get_archive(symbol: &String, state: &Arc<ServerState>) -> Result<Arc<OhlcArchive>, Box<dyn Error>> {
	// Simple directory traversal check
	let pattern = Regex::new("^[A-Z0-9]+$")?;
	if !pattern.is_match(symbol) {
		return Err("Invalid ticker".into());
	}
	if let Some(archive_ref) = state.ticker_cache.get(symbol) {
		Ok(Arc::clone(archive_ref.value()))
	}
	else {
		let file_name = get_archive_file_name(symbol);
		let data_directory = &*state.ticker_directory;
		let archive_path = Path::new(data_directory).join(file_name);
		let archive = read_archive(&archive_path)?;
		let archive_arc = Arc::new(archive);
		state.ticker_cache.insert(symbol.to_string(), Arc::clone(&archive_arc));
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

fn resolve_symbols(symbols: &Vec<String>, state: &Arc<ServerState>) -> Result<Vec<String>, Box<dyn Error>> {
	let all_keyword = "all";
	if symbols.iter().any(|x| x == all_keyword) {
		let data_directory = &*state.ticker_directory;
		let entries = fs::read_dir(data_directory)
			.expect("Unable to get list of archives");
		let result = entries
			.filter_map(|x| x.ok())
			.map(|x| x.path())
			.filter(|x| x.is_file())
			.filter(|x| x.extension()
				.and_then(|x| x.to_str()) == Some("zrk"))
			.filter_map(|x| x.file_stem()
				.and_then(|x| x.to_str())
				.map(|x| x.to_string()))
			.collect();
		Ok(result)
	}
	else {
		Ok(symbols.clone())
	}
}