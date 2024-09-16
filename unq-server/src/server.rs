use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use std::cell::RefCell;
use axum::{response::IntoResponse, extract::{Json, State}, routing::post, Router};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::services::ServeDir;
use anyhow::{Result, anyhow, Error, Context, bail};
use stopwatch::Stopwatch;
use tokio::task;
use tokio::task::JoinError;
use unq_common::backtest::{Backtest, BacktestConfiguration, BacktestResult};
use unq_common::manager::AssetManager;
use unq_common::ohlc::{OhlcArchive, OhlcMap, OhlcRecord, TimeFrame};
use unq_common::strategy::{StrategyParameter, StrategyParameters};
use unq_common::web::WebF64;
use unq_strategy::get_strategy;
use crate::correlation::{get_correlation_matrix, CorrelationData};
use crate::datetime::RelativeDateTime;

const MINUTES_PER_DAY: u16 = 1440;

struct ServerState {
	asset_manager: AssetManager,
	backtest_configuration: BacktestConfiguration
}

#[derive(Serialize)]
struct Response<T> {
	result: Option<T>,
	error: Option<String>
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

#[derive(Deserialize)]
struct GetCorrelationRequest {
	symbols: Vec<String>,
	from: RelativeDateTime,
	to: RelativeDateTime
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunBacktestRequest {
	strategy: String,
	symbols: Vec<String>,
	from: RelativeDateTime,
	to: RelativeDateTime,
	parameters: Vec<StrategyParameter>,
	time_frame: TimeFrame
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OhlcRecordWeb {
	pub symbol: String,
	pub time: NaiveDateTime,
	pub open: WebF64,
	pub high: WebF64,
	pub low: WebF64,
	pub close: WebF64,
	pub volume: u32,
	pub open_interest: Option<u32>
}

impl OhlcRecordWeb {
	pub fn new(record: &OhlcRecord) -> OhlcRecordWeb {
		OhlcRecordWeb {
			symbol: record.symbol.clone(),
			time: record.time,
			open: WebF64::new(record.open),
			high: WebF64::new(record.high),
			low: WebF64::new(record.low),
			close: WebF64::new(record.close),
			volume: record.volume,
			open_interest: record.open_interest
		}
	}
}

pub async fn run(address: SocketAddr, ticker_directory: String, csv_directory: String, assets_path: String, backtest_configuration: BacktestConfiguration) -> Result<()> {
	println!("Loading assets");
	let stopwatch = Stopwatch::start_new();
	let asset_manager = AssetManager::new(ticker_directory, csv_directory, assets_path)?;
	println!("Loaded assets in {} ms", stopwatch.elapsed_ms());
	println!("Running server on {}", address);
	let server_state = ServerState {
		asset_manager,
		backtest_configuration
	};
	let state_arc = Arc::new(server_state);
	let serve_dir = ServeDir::new("web");
	let app = Router::new()
		.route("/history", post(get_history))
		.route("/correlation", post(get_correlation))
		.route("/backtest", post(run_backtest))
		.with_state(state_arc)
		.fallback_service(serve_dir);
	let listener = TcpListener::bind(address).await
		.with_context(|| "Failed to bind address")?;
	axum::serve(listener, app).await
		.with_context(|| "Failed to launch axum server")?;
	Ok(())
}

async fn get_response<A, B>(state: Arc<ServerState>, request: A, get_data: Box<dyn FnOnce(A, Arc<ServerState>) -> Result<B> + Send>) -> impl IntoResponse
where
	A: Send + 'static,
	B: Send + Serialize + 'static
{
	let get_response = |data: B| {
		Response {
			result: Some(data),
			error: None
		}
	};
	let get_error = |error: Error| {
		Response {
			result: None,
			error: Some(error.to_string())
		}
	};
	let response = task::spawn_blocking(|| get_data(request, state))
		.await
		.map(|task_result| task_result.map_or_else(get_error, get_response))
		.unwrap_or_else(|error: JoinError| get_error(anyhow!(error)));
	Json(response)
}

async fn get_history(
	State(state): State<Arc<ServerState>>,
	Json(request): Json<GetHistoryRequest>
) -> impl IntoResponse {
	get_response(state, request, Box::new(|request, state| {
		get_history_data(request, &state.asset_manager)
	})).await
}

async fn get_correlation(
	State(state): State<Arc<ServerState>>,
	Json(request): Json<GetCorrelationRequest>
) -> impl IntoResponse {
	get_response(state, request, Box::new(|request, state| {
		get_correlation_data(request, &state.asset_manager)
	})).await
}

async fn run_backtest(
	State(state): State<Arc<ServerState>>,
	Json(request): Json<RunBacktestRequest>
) -> impl IntoResponse {
	get_response(state, request, Box::new(|request, state| {
		get_backtest_result(request, &state.asset_manager, &state.backtest_configuration)
	})).await
}

fn get_history_data(request: GetHistoryRequest, asset_manager: &AssetManager) -> Result<HashMap<String, Vec<OhlcRecordWeb>>> {
	let time_frame = if request.time_frame >= MINUTES_PER_DAY {
		TimeFrame::Daily
	} else {
		TimeFrame::Intraday
	};
	let resolved_symbols = asset_manager.resolve_symbols(&request.symbols)?;
	let archives = get_ticker_archives(&resolved_symbols, asset_manager)?;
	let from_resolved = request.from.resolve(&request.to, &time_frame, &archives)?;
	let to_resolved = request.to.resolve(&request.from, &time_frame, &archives)?;
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

fn get_ticker_archives<'a>(symbols: &Vec<String>, asset_manager: &'a AssetManager) -> Result<Vec<&'a OhlcArchive>> {
	symbols
		.iter()
		.map(|x| asset_manager.get_archive(&x))
		.collect()
}

fn get_correlation_data(request: GetCorrelationRequest, asset_manager: &AssetManager) -> Result<CorrelationData> {
	let resolved_symbols = asset_manager.resolve_symbols(&request.symbols)?;
	let archives = get_ticker_archives(&resolved_symbols, asset_manager)?;
	let time_frame = TimeFrame::Daily;
	let from = request.from.resolve(&request.to, &time_frame, &archives)?;
	let to = request.to.resolve(&request.from, &time_frame, &archives)?;
	get_correlation_matrix(resolved_symbols, from, to, &archives)
}

fn get_ohlc_records(from: &NaiveDateTime, to: &NaiveDateTime, time_frame: u16, archive: &OhlcArchive) -> Result<Vec<OhlcRecordWeb>> {
	if time_frame >= MINUTES_PER_DAY {
		return Ok(get_unprocessed_records(from, to, archive.daily.get_adjusted_fallback()));
	} else if time_frame == archive.intraday_time_frame {
		return Ok(get_unprocessed_records(from, to, archive.intraday.get_adjusted_fallback()));
	} else if time_frame < archive.intraday_time_frame {
		bail!("Requested time frame too small for intraday data in archive");
	} else if time_frame % archive.intraday_time_frame != 0 {
		let message = format!("Requested time frame must be a multiple of {}", archive.intraday_time_frame);
		bail!(message);
	}
	let chunk_size = (time_frame / archive.intraday_time_frame) as usize;
	archive.intraday
		.get_adjusted_fallback()
		.range(from..to)
		.map(|(_, record)| record)
		.collect::<Vec<_>>()
		.chunks(chunk_size)
		.filter(|x| x.len() == chunk_size)
		.map(merge_ohlc_records)
		.collect()
}

fn merge_ohlc_records(data: &[&OhlcRecord]) -> Result<OhlcRecordWeb> {
	let first = data.first().unwrap();
	let last = data.last().unwrap();
	let symbol = first.symbol.clone();
	let time = first.time;
	let open = first.open;
	let high = data
		.iter()
		.max_by(|x, y| x.high.partial_cmp(&y.high).unwrap())
		.unwrap()
		.high;
	let low = data
		.iter()
		.min_by(|x, y| x.low.partial_cmp(&y.low).unwrap())
		.unwrap()
		.low;
	let close = last.close;
	let volume = data.iter().map(|x| x.volume).sum();
	let open_interest = data.iter().map(|x| x.open_interest).sum();
	let record = OhlcRecordWeb {
		symbol,
		time,
		open: WebF64::new(open),
		high: WebF64::new(high),
		low: WebF64::new(low),
		close: WebF64::new(close),
		volume,
		open_interest
	};
	Ok(record)
}

fn get_unprocessed_records(from: &NaiveDateTime, to: &NaiveDateTime, source: &OhlcMap) -> Vec<OhlcRecordWeb>
{
	source
		.range(from..to)
		.map(|(_, record)| OhlcRecordWeb::new(record))
		.collect()
}

fn get_backtest_result(request: RunBacktestRequest, asset_manager: &AssetManager, backtest_configuration: &BacktestConfiguration) -> Result<BacktestResult> {
	let archives = get_ticker_archives(&request.symbols, asset_manager)?;
	let from = request.from.resolve(&request.to, &request.time_frame, &archives)?;
	let to = request.to.resolve(&request.from, &request.time_frame, &archives)?;
	let backtest = Backtest::new(from, to, request.time_frame, backtest_configuration.clone(), asset_manager)?;
	let backtest_refcell = RefCell::new(backtest);
	let parameters = StrategyParameters(request.parameters);
	let mut strategy = get_strategy(&request.strategy, request.symbols, &parameters, &backtest_refcell)?;
	let mut done = false;
	while !done {
		strategy.next()?;
		let mut backtest_mut = backtest_refcell.borrow_mut();
		done = backtest_mut.next()?;
	}
	let result;
	let backtest = backtest_refcell.borrow_mut();
	result = backtest.get_result()?;
	Ok(result)
}