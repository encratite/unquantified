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
use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use chrono_tz::Tz;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::services::{ServeDir, ServeFile};
use dashmap::DashMap;
use common::*;

struct ServerState {
	data_directory: Arc<String>,
	ticker_cache: Arc<DashMap<String, Arc<OhlcArchive>>>
}

#[derive(Deserialize)]
struct GetHistoryRequest {
	tickers: Vec<String>,
	from: DateTime<FixedOffset>,
	to: DateTime<FixedOffset>,
	// Minutes, 1440 for daily data
	time_frame: u16
}

#[derive(Serialize)]
struct GetHistoryResponse {
	tickers: HashMap<String, Vec<OhlcRecordWeb>>
}

#[derive(Serialize)]
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
		let time = get_date_time_string(record.time, &archive.time_zone)?;
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
	let serve_dir = ServeDir::new("web")
		.not_found_service(ServeFile::new("web/index.html"));
	let app = Router::new()
		.route("/history", post(get_history))
		.with_state(state_arc)
		.fallback_service(serve_dir);
	let listener = TcpListener::bind(address).await.unwrap();
	axum::serve(listener, app).await.unwrap();
}

async fn get_history(
	State(state): State<Arc<ServerState>>,
	Json(request): Json<GetHistoryRequest>
) -> impl IntoResponse {
	let archives = request.tickers
		.into_iter()
		.map(|x| get_archive(&x, &state));
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

fn get_date_time_string(time: NaiveDateTime, time_zone: &String) -> Result<String, Box<dyn Error>> {
	let time_utc = DateTime::<Utc>::from_naive_utc_and_offset(time, Utc);
	let tz = Tz::from_str(time_zone)?;
	let time_tz = time_utc.with_timezone(&tz);
	Ok(time_tz.to_rfc3339())
}