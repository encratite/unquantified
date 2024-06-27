use std::{	
	error::Error, ffi::OsStr, fs, path::{Component, Path, PathBuf}
};
use std::collections::BTreeMap;
use regex::Regex;
use serde;
use chrono::{
	NaiveDate, NaiveDateTime
};
use chrono_tz::Tz;
use stopwatch::Stopwatch;
use rayon::prelude::*;
use common::*;

type OhlcTreeMap = BTreeMap<OhlcKey, OhlcRecord>;

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CsvRecord<'a> {
	ticker: Option<&'a str>,
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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct OhlcKey {
	ticker: Option<String>,
	time: NaiveDateTime
}

pub struct CsvParser<'a> {
	time_zone: &'a Tz,
	input_directory: &'a PathBuf,
	output_directory: &'a PathBuf
}

impl<'a> CsvParser<'a> {
	pub fn new(time_zone: &'a Tz, input_directory: &'a PathBuf, output_directory: &'a PathBuf) -> CsvParser<'a> {
		CsvParser {
			time_zone,
			input_directory,
			output_directory
		}
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

	fn get_directories(path: &PathBuf) -> impl Iterator<Item = PathBuf> {
		fs::read_dir(path)
			.expect(format!("Unable to read directory \"{}\"", path.to_str().unwrap()).as_str())
			.filter(|x| x.is_ok())
			.map(|x| x.unwrap().path())
			.filter(|x| x.is_dir())
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

	fn parse_date_time(time_string: &str) -> Result<NaiveDateTime, Box<dyn Error>>  {
		match NaiveDateTime::parse_from_str(time_string, "%m/%d/%Y %H:%M") {
			Ok(datetime) => Ok(datetime),
			Err(_) => match NaiveDate::parse_from_str(time_string, "%m/%d/%Y") {
				Ok(date) => Ok(date.and_hms_opt(0, 0, 0).unwrap()),
				Err(_) => Err("Failed to parse datetime".into())
			}
		}
	}

	fn get_time_frame_directory<'b>(prefix: &str, time_frame_directories: &'b Vec<PathBuf>) -> Option<(&'b PathBuf, u16)> {
		for directory in time_frame_directories.iter() {
			if let Some(Component::Normal(last_component)) = directory.components().last() {
				if let Some(tuple) = Self::get_time_frame_captures(last_component, prefix, directory) {
					return Some(tuple);
				}
			}
		}
		None
	}

	fn get_time_frame_captures<'b>(last_component: &'b OsStr, prefix: &str, directory: &'b PathBuf) -> Option<(&'b PathBuf, u16)> {
		let pattern = Regex::new("^([A-Z])(\\d+)$").unwrap();
		if let Some(captures) = pattern.captures(last_component.to_str().unwrap()) {
			if captures.get(1).unwrap().as_str() == prefix {
				let time_frame: u16 = captures
					.get(2)
					.unwrap()
					.as_str()
					.parse()
					.unwrap();
				return Some((directory, time_frame))
			}
		}
		None
	}

	pub fn run(&self) {
		let stopwatch = Stopwatch::start_new();
		Self::get_directories(self.input_directory)
			.collect::<Vec<PathBuf>>()
			.par_iter()
			.for_each(|ticker_directory| {
				self.process_ticker_directory(ticker_directory);
			});
		println!("Processed all directories in {} ms", stopwatch.elapsed_ms());
	}

	fn process_ticker_directory(&self, ticker_directory: &PathBuf) {
		let stopwatch = Stopwatch::start_new();
		let time_frame_directories: Vec<PathBuf> = Self::get_directories(ticker_directory).collect();
		let directory_count = time_frame_directories.len();
		if directory_count != 2 {
			panic!("Unexpected sub directory count ({}) in path \"{}\"", directory_count, ticker_directory.to_str().unwrap());
		}
		let (daily_path, _) = Self::get_time_frame_directory("D", &time_frame_directories)
			.expect("Unable to find daily path");
		let (intraday_path, intraday_time_frame) = Self::get_time_frame_directory("M", &time_frame_directories)
			.expect("Unable to find intraday path");
		let daily = self.parse_csv_files(daily_path);
		let intraday = self.parse_csv_files(intraday_path);
		let archive_path = self.get_archive_path(ticker_directory);
		let time_zone = self.time_zone.to_string();
		let archive = OhlcArchive {
			daily,
			intraday,
			intraday_time_frame,
			time_zone
		};
		match write_archive(&archive_path, &archive) {
			Ok(_) => {}
			Err(error) => {
				eprintln!("Failed to write archive: {}", error);
				return;
			}
		}
		println!(
			"Loaded {} records from \"{}\" and wrote them to \"{}\" in {} ms",
			archive.daily.len() + archive.intraday.len(),
			ticker_directory.to_str().unwrap(),
			archive_path.to_str().unwrap(),
			stopwatch.elapsed_ms()
		);
	}

	fn parse_csv_files(&self, path: &PathBuf) -> Vec<OhlcRecord> {
		let csv_paths = Self::get_csv_paths(path);
		let mut ohlc_map = OhlcTreeMap::new();
		for csv_path in csv_paths {
			let mut reader = csv::Reader::from_path(csv_path)
				.expect("Unable to read .csv file");
			let headers = reader.headers()
				.expect("Unable to parse headers")
				.clone();
			let mut string_record = csv::StringRecord::new();
			while reader.read_record(&mut string_record).is_ok() {
				let record: CsvRecord = string_record.deserialize(Some(&headers))
					.expect("Failed to deserialize record");
				self.add_ohlc_record(&record, &mut ohlc_map);
			}
		}
		ohlc_map.into_values().collect()
	}

	fn add_ohlc_record(&self, record: &CsvRecord, ohlc_map: &mut OhlcTreeMap) {
		let Ok(time) = Self::parse_date_time(record.time)
		else {
			return;
		};
		let ticker = record.ticker.map(|x| x.to_string());
		let key = OhlcKey {
			ticker: ticker.clone(),
			time: time
		};
		let open_interest = record.open_interest.parse::<i32>().ok();
		let value = OhlcRecord {
			ticker,
			time: time,
			open: record.open,
			high: record.high,
			low: record.low,
			close: record.close,
			volume: record.volume,
			open_interest: open_interest
		};
		ohlc_map.insert(key, value);
	}

	fn get_archive_path(&self, time_frame_directory: &PathBuf) -> PathBuf {
		let ticker = Self::get_last_token(time_frame_directory);
		let file_name = get_archive_file_name(&ticker);
		return Path::new(self.output_directory).join(file_name);
	}
}
