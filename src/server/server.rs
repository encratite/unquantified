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
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
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
	correlation: Option<CorrelationData>,
	error: Option<String>
}

#[derive(Serialize)]
struct CorrelationData {
	correlation: Vec<Vec<f64>>,
	from: DateTime<FixedOffset>,
	to: DateTime<FixedOffset>
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
	let resolved_tickers = resolve_tickers(&request.tickers, &state)?;
	let archives = get_ticker_archives(&resolved_tickers, &state)?;
	let from_resolved = request.from.resolve(&request.to, &archives)?;
	let to_resolved = request.to.resolve(&request.from, &archives)?;
	let result: Result<Vec<Vec<OhlcRecordWeb>>, Box<dyn Error>> = archives
		.iter()
		.map(|archive| get_ohlc_records(&from_resolved, &to_resolved, request.time_frame, archive))
		.collect();
	match result {
		Ok(ticker_records) => {
			let tuples = resolved_tickers.into_iter().zip(ticker_records.into_iter()).collect();
			Ok(tuples)
		},
		Err(error) => Err(error)
	}
}

fn get_ticker_archives(tickers: &Vec<String>, state: &Arc<ServerState>) -> Result<Vec<Arc<OhlcArchive>>, Box<dyn Error>> {
	tickers
		.iter()
		.map(|x| get_archive(&x, &state))
		.collect()
}

fn get_correlation_data(request: GetCorrelationRequest, state: Arc<ServerState>) -> Result<CorrelationData, Box<dyn Error>> {
	let resolved_tickers = resolve_tickers(&request.tickers, &state)?;
	let archives = get_ticker_archives(&resolved_tickers, &state)?;
	let mut from = request.from.resolve(&request.to, &archives)?;
	let mut to = request.to.resolve(&request.from, &archives)?;
	let get_fixed_time = |x: &OhlcRecord, archive: &OhlcArchive| archive.add_tz(x.time).fixed_offset();
	// Determine common time range
	for archive in &archives {
		let add_tz = |x: &OhlcRecord| Some(get_fixed_time(x, &archive));
		let records = &archive.daily;
		let first = records
			.iter()
			.next()
			.and_then(add_tz);
		let last = records
			.iter()
			.last()
			.and_then(add_tz);
		match (first, last) {
			(Some(first_time), Some(last_time)) => {
				from = from.max(first_time);
				to = to.min(last_time);
			}
			_ => return Err("Missing records in archive".into())
		}
	}
	// Create an index map to make sure that each cell in the matrix corresponds to the same point in time
	let in_range = |fixed_time| fixed_time >= from && fixed_time <= to;
	let mut indexes = HashMap::new();
	let first_archive = &archives.iter().next()
		.ok_or_else(|| "No archives specified")?;
	let mut i: usize = 0;
	for x in &first_archive.daily {
		let fixed_time = get_fixed_time(&x, &first_archive);
		if in_range(fixed_time) {
			indexes.insert(fixed_time, i);
			i += 1;
		}
	}
	let count = indexes.len();
	let delta_samples: Vec<(Vec<f64>, f64)> = archives.par_iter().map(|archive| {
		let mut sum = 0f64;
		let initial_value = 0f64;
		let mut samples = vec![initial_value; count];
		// Get close samples for the dynamic time range
		for record in &archive.daily {
			let fixed_time = get_fixed_time(&record, &archive);
			if in_range(fixed_time) {
				if let Some(index) = indexes.get(&fixed_time) {
					let sample = record.close;
					samples[*index] = sample;
					sum += sample;
				}
			}
		}
		let mean = sum / (count as f64);
		let mut square_sum = 0f64;
		for x in &mut samples {
			if *x != initial_value {
				// Store pre-calculated x_i - x_mean values
				*x -= mean;
			}
			else {
				// Fill out gaps in the data with the mean value
				*x = mean;
			}
			square_sum += *x * *x;
		}
		let sqrt = square_sum.sqrt();
		(samples, sqrt)
	}).collect();
	// Create a square a matrix, default to 1.0 for diagonal elements
	let mut matrix = vec![vec![1f64; count]; count];
	// Generate a list of pairs (i, j) of indices for one half of the matrix, excluding the diagonal, for parallel processing
	let mut pairs = Vec::new();
	for i in 0..count {
		for j in 0..count {
			if i < j {
				pairs.push((i, j));
			}
		}
	}
	// Calculate Pearson correlation coefficients
	let coefficients: Vec<(usize, usize, f64)> = pairs.par_iter().map(|(i, j)| {
		let (x_samples, x_sqrt) = &delta_samples[*i];
		let (y_samples, y_sqrt) = &delta_samples[*j];
		let mut sum = 0f64;
		for k in 0..count {
			let delta_x = x_samples[k];
			let delta_y = y_samples[k];
			sum += delta_x * delta_y;
		}
		let coefficient = sum / (x_sqrt * y_sqrt);
		(*i, *j, coefficient)
	}).collect();
	// Store correlation coefficients symmetrically
	for (i, j, coefficient) in coefficients {
		matrix[i][j] = coefficient;
		matrix[j][i] = coefficient;
	}
	let output = CorrelationData {
		correlation: matrix,
		from: from,
		to: to
	};
	Ok(output)
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

fn resolve_tickers(tickers: &Vec<String>, state: &Arc<ServerState>) -> Result<Vec<String>, Box<dyn Error>> {
	let all_keyword = "all";
	if tickers.iter().any(|x| x == all_keyword) {
		let data_directory = &*state.data_directory;
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
		Ok(tickers.clone())
	}
}