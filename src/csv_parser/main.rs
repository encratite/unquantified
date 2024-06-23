use std::{
	collections::{BTreeMap, HashMap},
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

type OhlcTreeMap = BTreeMap<OhlcKey, OhlcRecord>;

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

#[derive(Archive, Serialize, Deserialize)]
struct OhlcArchive {
	time_frames: HashMap<String, Vec<OhlcRecord>>
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
	process_ticker_directories(&input_directory, &output_directory);
}

fn process_ticker_directories(input_directory: &PathBuf, output_directory: &PathBuf) {
	let stopwatch = Stopwatch::start_new();
	get_directories(input_directory, "Unable to read ticker directory")
		.collect::<Vec<PathBuf>>()
		.par_iter()
		.for_each(|ticker_directory| {
			let mut archive = OhlcArchive {
				time_frames: HashMap::new()
			};
			let stopwatch = Stopwatch::start_new();
			get_directories(&ticker_directory, "Unable to read time frames")
				.for_each(|time_frame_directory| {
					let time_frame = get_last_token(&time_frame_directory);
					let time_frame_data = get_time_frame_data(&time_frame_directory);
					archive.time_frames.insert(time_frame, time_frame_data);
				});
			write_archive(ticker_directory, output_directory, &stopwatch, &archive);
		});
	println!("Processed all directories in {} ms", stopwatch.elapsed_ms());
}

fn get_last_token(path: &PathBuf) -> String {
	path
		.components()
		.last()
		.unwrap()
		.as_os_str()
		.to_str()
		.unwrap()
		.to_string()
}

fn get_directories(path: &PathBuf, error_message: &str) -> impl Iterator<Item = PathBuf> {
	fs::read_dir(path)
		.expect(error_message)
		.filter(|x| x.is_ok())
		.map(|x| x.unwrap().path())
		.filter(|x| x.is_dir())
}

fn get_time_frame_data(path: &PathBuf) -> Vec<OhlcRecord> {
	let csv_paths = get_csv_paths(path);
	let mut ohlc_map = OhlcTreeMap::new();
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
	ohlc_map.into_values().collect()
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

fn add_ohlc_record(record: &CsvRecord, ohlc_map: &mut OhlcTreeMap) {
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

fn write_archive(time_frame_directory: &PathBuf, output_directory: &PathBuf, stopwatch: &Stopwatch, archive: &OhlcArchive) {
	let output_path = get_output_path(time_frame_directory, output_directory);
	match rkyv::to_bytes::<_, 1024>(archive) {
		Ok(binary_data) => {
			let count: usize = archive.time_frames
				.values()
				.map(|x| x.len())
				.sum();
			compress_archive(&binary_data, &output_path);
			println!(
				"Loaded {} records from \"{}\" and wrote them to \"{}\" in {} ms",
				count,
				time_frame_directory.to_str().unwrap(),
				output_path.to_str().unwrap(),
				stopwatch.elapsed_ms()
			);
		}
		Err(error) => {
			eprintln!("Failed to serialize records: {error}");
		}
	}
}

fn get_output_path(time_frame_directory: &PathBuf, output_directory: &PathBuf) -> PathBuf {
	let symbol = get_last_token(time_frame_directory);
	let file_name = format!("{symbol}.zrk");
	return Path::new(output_directory).join(file_name);
}

fn compress_archive(binary_data: &AlignedVec, output_path: &PathBuf) {
	match File::create(output_path.clone()) {
		Ok(file) => {
			match zstd::stream::copy_encode(binary_data.as_slice(), file, 1) {
				Ok(_) => {}
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