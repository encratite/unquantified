use std::error::Error;
use std::net::SocketAddr;
use axum::response::IntoResponse;
use axum::extract::Json;
use axum::routing::post;
use axum::Router;
use chrono::{DateTime, FixedOffset};
use serde::Deserialize;
use tower_http::services::{ServeDir, ServeFile};
use common::*;

#[derive(Deserialize)]
struct GetHistoryRequest {
	symbol: String,
	from: DateTime<FixedOffset>,
	to: DateTime<FixedOffset>,
	// Minutes, 1440 for daily data
	time_frame: u16
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
	let config = get_config("server.ini")?;
	let get_key = |key| {
		config.get("server", key)
			.expect(&*format!("Failed to find key \"{}\" in configuration file", key))
	};
	let _data_directory = get_key("data_directory");
	let address_string = get_key("address");
	let address: SocketAddr = address_string.parse()
		.expect("Unable to parse server address");
	run_server(address).await;
	Ok(())
}

async fn run_server(address: SocketAddr) {
	println!("Running server on {}", address);
	let serve_dir = ServeDir::new("web")
		.not_found_service(ServeFile::new("web/index.html"));
	let app = Router::new()
		.route("/history", post(get_history))
		.fallback_service(serve_dir);
	let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn get_history(Json(request): Json<GetHistoryRequest>) -> impl IntoResponse {
    format!("Received symbol: {}", request.symbol)
}