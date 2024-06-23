use std::{
	collections::BTreeMap, env, fs::{self, File}, io::Write, path::{Path, PathBuf}
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

#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
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
	if arguments.len() != 3 {
		println!("Usage:");
		let application = env::current_exe().unwrap();
		println!("{} <path to Barchart .csv files> <output directory>", application.display());
		return;
	}
	let input_directory = PathBuf::from(&arguments[1]);
	let output_directory = PathBuf::from(&arguments[2]);
	read_ticker_directories(&input_directory, &output_directory);
}

fn read_ticker_directories(input_directory: &PathBuf, output_directory: &PathBuf) {
	let stopwatch = Stopwatch::start_new();
	let mut paths = Vec::new();
	for_each_directory(input_directory, "Unable to read ticker directory", |tickers_path| {
		for_each_directory(&tickers_path, "Unable to read time frames", |time_frame_path| {
			paths.push(time_frame_path);
		});
	});
	paths.par_iter().for_each(|x| process_time_frame_data(x, output_directory));
	println!("Processed all directories in {} ms", stopwatch.elapsed_ms());
}

fn for_each_directory(path: &PathBuf, error_message: &str, handler: impl FnMut(PathBuf) -> ()) {
	fs::read_dir(path)
		.expect(error_message)
		.filter(|x| x.is_ok())
		.map(|x| x.unwrap().path())
		.filter(|x| x.is_dir())
		.for_each(handler);
}

fn process_time_frame_data(path: &PathBuf, output_directory: &PathBuf) {
	let csv_paths = fs::read_dir(path.clone())
		.expect("Unable to get list of .csv files")
		.filter(|x| x.is_ok())
		.map(|x| x.unwrap().path())
		.filter(|x|
			x.is_file() &&
			x.extension().is_some() &&
			x.extension().unwrap() == "csv"
		);
	println!("Processing files in \"{}\"", path.to_str().unwrap());
	let mut stopwatch = Stopwatch::start_new();
	let mut ohlc_map = BTreeMap::new();
	for csv_path in csv_paths {
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
	println!("Merged {} records from {} in {} ms", ohlc_map.len(), path.to_str().unwrap(), stopwatch.elapsed_ms());
	stopwatch.restart();
	let iter = path.iter();
	let count = 2;
	let mut final_tokens = iter.skip(path.components().count() - count).take(count);
	let mut get_token = || final_tokens.next().unwrap().to_str().unwrap();
	let symbol = get_token();
	let time_frame =  get_token();
	let file_name = format!("{symbol}.{time_frame}");
	let output_path = Path::new(output_directory).join(file_name);
	let ohlc_records = ohlc_map.values().cloned().collect::<Vec<OhlcRecord>>();
	match rkyv::to_bytes::<_, 1024>(&ohlc_records) {
		Ok(binary_data) => {
			match File::create(output_path.clone()) {
				Ok(mut file) => {
					match zstd::stream::copy_encode(binary_data.as_slice(), file, 1) {
						Ok(_) => {
							println!("Serialized records to \"{}\" in {} ms", output_path.to_str().unwrap(), stopwatch.elapsed_ms());
						}
						Err(error) => {
							eprintln!("Failed to write output to file \"{}\": {error}", output_path.to_str().unwrap());
						}
					}
				}
				Err(error) => {
					eprintln!("Failed to create output file \"{}\": {error}", output_path.to_str().unwrap());
				}
			}
		}
		Err(error) => {
			eprintln!("Failed to serialize B-tree map: {error}");
		}
	}
}