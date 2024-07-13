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
use chrono::{DateTime, Duration, FixedOffset, Local, Months, NaiveDateTime, TimeDelta, Utc};
use chrono_tz::Tz;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::services::ServeDir;
use dashmap::DashMap;
use common::*;

#[derive(Deserialize, Clone)]
enum OffsetUnit {
	#[serde(rename = "m")]
	Minutes,
	#[serde(rename = "h")]
	Hours,
	#[serde(rename = "d")]
	Days,
	#[serde(rename = "w")]
	Weeks,
	#[serde(rename = "mo")]
	Months,
	#[serde(rename = "y")]
	Years
}

#[derive(Deserialize, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
enum SpecialDateTime {
	First,
	Last,
	Now
}

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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RelativeDateTime {
	date: Option<DateTime<FixedOffset>>,
	/*
	offset and offset_unit encode relative offsets such as +15m, -1w and -4y.
	If set, all other members of RelativeDateTime must be set to None.
	The following unit strings are supported:
	- "m": minutes
	- "h": hours
	- "d": days
	- "w": weeks
	- "mo": months
	- "y": years
	*/
	offset: Option<i16>,
	offset_unit: Option<OffsetUnit>,
	/*
	This optional member is used for the special keywords in the Unquantified prompt language:
	- "first": Evaluates to the first point in time at which data is available for the specified symbol.
	  If it is being used with multiple symbols, the minmum point in time out of all of them is used.
	  This keyword may only be used for the "from" parameter.
	- "last": Evaluates to the last point in time at wich data is available. With multiple symbols, the maximum is used.
	  This keyword may only be used for the "to" parameter.
	- "now": Evaluates to the current point in time.
	  This keyword may only be used for the "to" parameter.
	*/
	special_keyword: Option<SpecialDateTime>
}

#[derive(Serialize)]
struct GetHistoryResponse {
	tickers: Option<HashMap<String, Vec<OhlcRecordWeb>>>,
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

impl RelativeDateTime {
	pub fn resolve(&self, other: &RelativeDateTime, archives: &Vec<Arc<OhlcArchive>>) -> Result<DateTime<FixedOffset>, Box<dyn Error>> {
		match (self.date.is_some(), self.offset.is_some(), self.offset_unit.is_some(), self.special_keyword.is_some()) {
			(true, false, false, false) => Ok(self.date.unwrap()),
			(false, true, true, false) => {
				let other_time = other.to_fixed(archives)?;
				let offset_time = get_offset_date_time(&other_time, self.offset.unwrap(), self.offset_unit.clone().unwrap())
					.expect("Invalid offset calculation".into());
				Ok(offset_time)
			},
			(false, false, false, true) => {
				let special_time = resolve_keyword(self.special_keyword.clone().unwrap(), archives)?;
				Ok(special_time)
			},
			_ => Err("Invalid relative date time".into())
		}
	}

	fn to_fixed(&self, archives: &Vec<Arc<OhlcArchive>>) -> Result<DateTime<FixedOffset>, Box<dyn Error>> {
		match (self.date.is_some(), self.special_keyword.is_some()) {
			(true, false) => Ok(self.date.unwrap()),
			(false, true) => {
				let special_time = resolve_keyword(self.special_keyword.clone().unwrap(), archives)?;
				Ok(special_time)
			},
			_ => Err("Invalid combination of relative date times".into())
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

fn get_date_time_tz(time: NaiveDateTime, tz: &Tz) -> DateTime<Tz> {
	let time_utc = DateTime::<Utc>::from_naive_utc_and_offset(time, Utc);
	time_utc.with_timezone(tz)
}

fn get_date_time_fixed(time: NaiveDateTime, tz: &Tz) -> DateTime<FixedOffset> {
	let time_tz = get_date_time_tz(time, tz);
	time_tz.fixed_offset()
}

fn get_date_time_string(time: NaiveDateTime, tz: &Tz) -> Result<String, Box<dyn Error>> {
	let time_tz = get_date_time_tz(time, &tz);
	Ok(time_tz.to_rfc3339())
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

fn resolve_keyword(special_keyword: SpecialDateTime, archives: &Vec<Arc<OhlcArchive>>) -> Result<DateTime<FixedOffset>, Box<dyn Error>> {
	if special_keyword == SpecialDateTime::Now {
		let now: DateTime<Local> = Local::now();
		let now_with_timezone: DateTime<FixedOffset> = now.with_timezone(now.offset());
		Ok(now_with_timezone)
	}
	else {
		let is_first = special_keyword == SpecialDateTime::First;
		let times = archives
			.iter()
			.map(|x| resolve_first_last(is_first, x))
			.collect::<Result<Vec<DateTime<FixedOffset>>, Box<dyn Error>>>()?;
		let time = if is_first {
			times.iter().min()
		}
		else {
			times.iter().max()
		};
		match time {
			Some(x) => Ok(*x),
			None => Err("No records available".into())
		}
	}
}

fn resolve_first_last(is_first: bool, archive: &Arc<OhlcArchive>) -> Result<DateTime<FixedOffset>, Box<dyn Error>> {
	let tz = Tz::from_str(archive.time_zone.as_str())?;
	let mut time_values = archive.intraday
		.iter()
		.map(|x| x.time);
	let get_some_time = |time| match time {
		Some(x) => Ok(get_date_time_fixed(x, &tz)),
		None => Err("No records available".into())
	};
	if is_first {
		get_some_time(time_values.next())
	}
	else {
		get_some_time(time_values.last())
	}
}

fn get_offset_date_time(time: &DateTime<FixedOffset>, offset: i16, offset_unit: OffsetUnit) -> Option<DateTime<FixedOffset>> {
	let add_signed = |duration: fn(i64) -> TimeDelta, x: i16|
		time.checked_add_signed(duration(x as i64));
	let get_months = |x: i16| if x >= 0 {
		Months::new(x as u32)
	}
	else {
		Months::new((- x) as u32)
	};
	let add_sub_months = |x| {
		let months = get_months(x);
		if offset >= 0 {
			time.checked_add_months(months)
		}
		else {
			time.checked_sub_months(months)
		}
	};
	match offset_unit {
		OffsetUnit::Minutes => add_signed(Duration::minutes, offset),
		OffsetUnit::Hours => add_signed(Duration::hours, offset),
		OffsetUnit::Days => add_signed(Duration::days, offset),
		OffsetUnit::Weeks => add_signed(Duration::days, 7 * offset),
		OffsetUnit::Months => add_sub_months(offset),
		OffsetUnit::Years => add_sub_months(12 * offset),
	}
}