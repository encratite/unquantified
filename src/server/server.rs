use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use axum::response::IntoResponse;
use axum::extract::{Json, State};
use axum::routing::post;
use axum::Router;
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use tower_http::services::{ServeDir, ServeFile};
use dashmap::DashMap;
use common::*;

struct ServerState {
	data_directory: Arc<String>,
	ticker_cache: Arc<DashMap<String, OhlcArchive>>
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
	tickers: HashMap<String, Vec<OhlcRecord>>
}

#[derive(Serialize)]
struct OhlcRecord {
	pub symbol: Option<String>,
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
	let listener = tokio::net::TcpListener::bind(address).await.unwrap();
	axum::serve(listener, app).await.unwrap();
}

async fn get_history(
	State(state): State<Arc<ServerState>>,
	Json(request): Json<GetHistoryRequest>
) -> impl IntoResponse {
}

fn get_archive(ticker: &String, state: &mut Arc<ServerState>) -> Result<OhlcArchive, Box<dyn Error>> {
	if let Some(archive_ref) = state.ticker_cache.get(ticker) {
		Ok(archive_ref.value().clone())
	}
	else {
		let file_name = get_archive_file_name(ticker);
		let data_directory = &*state.data_directory;
		let archive_path = Path::new(data_directory).join(file_name);
		let archive = read_archive(&archive_path)?;
		state.ticker_cache.insert(ticker.to_string(), archive.clone());
		Ok(archive)
	}
}