use std::{
	arch::x86_64, collections::BTreeMap, env, fs, path::PathBuf
};
use serde;
use rkyv::{
	Archive,
	Deserialize,
	Serialize
};
use chrono::{
	NaiveDate,
	NaiveDateTime
};
use stopwatch::Stopwatch;
use rayon::prelude::*;

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CsvRecord<'a> {
	symbol: Option<&'a str>,
	time: &'a str,
	open: f64,
	high: f64,
	low: f64,
	#[serde(rename = "Last")]
	close: f64,
	volume: i32,
	#[serde(rename = "Open Int", default)]
	open_interest: &'a str
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct OhlcRecord {
	symbol: Option<String>,
	time: NaiveDateTime,
	open: f64,
	high: f64,
	low: f64,
	close: f64,
	volume: i32,
	open_interest: Option<i32>
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct OhlcKey {
	symbol: Option<String>,
	time: NaiveDateTime
}

fn main() {
	let arguments: Vec<String> = env::args().collect();
	if arguments.len() != 2 {
		println!("Usage:");
		let application = env::current_exe().unwrap();
		println!("{} <path to Barchart .csv files>", application.display());
		return;
	}
	let path = PathBuf::from(&arguments[1]);
	read_ticker_directories(path);
}

fn read_ticker_directories(path: PathBuf) {
	let stopwatch = Stopwatch::start_new();
	for_each_directory(path, read_time_directories, "Unable to read ticker directory");
	println!("Processed all directories in {} ms", stopwatch.elapsed_ms());
}

fn read_time_directories(path: PathBuf) {
	for_each_directory(path, process_time_frame_data, "Unable to read time frames");
}

fn for_each_directory(path: PathBuf, handler: impl FnMut(PathBuf) -> (), error_message: &str) {
	fs::read_dir(path)
		.expect(error_message)
		.filter(|x| x.is_ok())
		.map(|x| x.unwrap().path())
		.filter(|x| x.is_dir())
		.for_each(handler);
}

fn process_time_frame_data(path: PathBuf) {
	let csv_paths = fs::read_dir(path.clone())
		.expect("Unable to get list of .csv files")
		.filter(|x| x.is_ok())
		.map(|x| x.unwrap().path())
		.filter(|x|
			x.is_file() &&
			x.extension().is_some() &&
			x.extension().unwrap() == "csv"
		);
	println!("Processing files in {}", path.to_str().unwrap());
	let stopwatch = Stopwatch::start_new();
	let mut ohlc_map = BTreeMap::new();
	for csv_path in csv_paths {
		// println!("Processing {}", csv_path.to_str().unwrap());
		let mut reader = csv::Reader::from_path(csv_path)
			.expect("Unable to read .csv file");
		let headers = reader.headers()
			.expect("Unable to parse headers")
			.clone();
		let mut string_record = csv::StringRecord::new();
		while reader.read_record(&mut string_record).is_ok() {
			match string_record.deserialize(Some(&headers)) {
				Ok(record) => {
					let record: CsvRecord = record;
					let mut time = NaiveDateTime::parse_from_str(record.time, "%m/%d/%Y %H:%M");
					if time.is_err() {
						let date = NaiveDate::parse_from_str(record.time, "%m/%d/%Y");
						if date.is_err() {
							continue;
						}
						time = Ok(date.unwrap().and_hms_opt(0, 0, 0).unwrap());
					}
					let symbol = record.symbol.map(|x| x.to_string());
					let key = OhlcKey {
						symbol: symbol.clone(),
						time: time.unwrap()
					};
					/*
					if ohlc_map.contains_key(&key) {
						continue;
					}
					 */
					let mut open_interest: Option<i32> = None;
					match record.open_interest.parse::<i32>() {
						Ok(interest) => {
							open_interest = Some(interest);
						}
						Err(_) => {}
					}
					let value = OhlcRecord {
						symbol: symbol,
						time: time.unwrap(),
						open: record.open,
						high: record.high,
						low: record.low,
						close: record.close,
						volume: record.volume,
						open_interest: open_interest
					};
					ohlc_map.insert(key, value);
				}
				Err(error) => {
					eprintln!("Failed to deserialize record: {error}");
				}
			}
		}
	}
	println!("Merged {} records into B-tree in {} ms", ohlc_map.len(), stopwatch.elapsed_ms());
}