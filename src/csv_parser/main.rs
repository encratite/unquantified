use std::{
	collections::BTreeMap,
	env,
	fs::{self, File},
	path::{Path, PathBuf}
};
use serde;
use rkyv::{
	Archive,
	Deserialize,
	Serialize,
	util::AlignedVec
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
	let get_argument = |i| PathBuf::from(&arguments[i]);
	let input_directory = get_argument(1);
	let output_directory = get_argument(2);
	read_ticker_directories(&input_directory, &output_directory);
}

fn read_ticker_directories(input_directory: &PathBuf, output_directory: &PathBuf) {
	let stopwatch = Stopwatch::start_new();
	get_directories(input_directory, "Unable to read ticker directory")
		.map(|x| get_directories(&x, "Unable to read time frames"))
		.flatten()
		.collect::<Vec<PathBuf>>()
		.par_iter()
		.for_each(|x| process_time_frame_data(x, output_directory));
	println!("Processed all directories in {} ms", stopwatch.elapsed_ms());
}

fn get_directories(path: &PathBuf, error_message: &str) -> impl Iterator<Item = PathBuf> {
	fs::read_dir(path)
		.expect(error_message)
		.filter(|x| x.is_ok())
		.map(|x| x.unwrap().path())
		.filter(|x| x.is_dir())
}

fn process_time_frame_data(path: &PathBuf, output_directory: &PathBuf) {
	let csv_paths = get_csv_paths(path);
	println!("Processing files in \"{}\"", path.to_str().unwrap());
	let stopwatch = Stopwatch::start_new();
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
					add_ohlc_record(&record, &mut ohlc_map);
				}
				Err(error) => {
					eprintln!("Failed to deserialize record: {error}");
				}
			}
		}
	}
	println!("Merged {} records from {} in {} ms", ohlc_map.len(), path.to_str().unwrap(), stopwatch.elapsed_ms());
	serialize_ohlc_records(path, output_directory, &ohlc_map);
}

fn get_csv_paths(path: &PathBuf) -> impl Iterator<Item = PathBuf> {
	fs::read_dir(path.clone())
		.expect("Unable to get list of .csv files")
		.filter(|x| x.is_ok())
		.map(|x| x.unwrap().path())
		.filter(|x|
			x.is_file() &&
			x.extension().is_some() &&
			x.extension().unwrap() == "csv"
		)
}

fn add_ohlc_record(record: &CsvRecord, ohlc_map: &mut BTreeMap<OhlcKey, OhlcRecord>) {
	let mut time = NaiveDateTime::parse_from_str(record.time, "%m/%d/%Y %H:%M");
	if time.is_err() {
		let date = NaiveDate::parse_from_str(record.time, "%m/%d/%Y");
		if date.is_err() {
			return;
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

fn serialize_ohlc_records(path: &PathBuf, output_directory: &PathBuf, ohlc_map: &BTreeMap<OhlcKey, OhlcRecord>) {
	let stopwatch = Stopwatch::start_new();
	let output_path = get_output_path(path, output_directory);
	let ohlc_records = ohlc_map.values().cloned().collect::<Vec<OhlcRecord>>();
	match rkyv::to_bytes::<_, 1024>(&ohlc_records) {
		Ok(binary_data) => {
			write_archive(&binary_data, &output_path, &stopwatch);
		}
		Err(error) => {
			eprintln!("Failed to serialize B-tree map: {error}");
		}
	}
}

fn get_output_path(path: &PathBuf, output_directory: &PathBuf) -> PathBuf {
	let iter = path.iter();
	let count = 2;
	let mut final_tokens = iter.skip(path.components().count() - count).take(count);
	let mut get_token = || final_tokens.next().unwrap().to_str().unwrap();
	let symbol = get_token();
	let time_frame =  get_token();
	let file_name = format!("{symbol}.{time_frame}");
	return Path::new(output_directory).join(file_name);
}

fn write_archive(binary_data: &AlignedVec, output_path: &PathBuf, stopwatch: &Stopwatch) {
	match File::create(output_path.clone()) {
		Ok(file) => {
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