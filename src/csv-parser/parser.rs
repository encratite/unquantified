use std::{	
	collections::HashSet, error::Error, fs, path::{Path, PathBuf}
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

use crate::filter::ContractFilter;

type OhlcTreeMap = BTreeMap<OhlcKey, RawOhlcRecord>;

#[derive(Debug, serde::Deserialize)]
struct CsvRecord {
	symbol: String,
	time: String,
	open: f64,
	high: f64,
	low: f64,
	close: f64,
	volume: u32,
	open_interest: Option<u32>
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct OhlcKey {
	symbol: String,
	time: NaiveDateTime
}

pub struct CsvParser {
	time_zone: Tz,
	intraday_time_frame: u16,
	input_directory: PathBuf,
	output_directory: PathBuf,
	filters: Vec<ContractFilter>
}

impl CsvParser {
	pub fn new(time_zone: Tz, intraday_time_frame: u16, input_directory: PathBuf, output_directory: PathBuf, filters: Vec<ContractFilter>) -> CsvParser {
		CsvParser {
			time_zone,
			intraday_time_frame,
			input_directory,
			output_directory,
			filters
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

	fn get_csv_paths(path: &PathBuf, filter: Regex) -> impl Iterator<Item = PathBuf> {
		fs::read_dir(path.clone())
			.expect("Unable to get list of .csv files")
			.filter_map(|x| x.ok())
			.map(|x| x.path())
			.filter(move |x|
				x.is_file() &&
				x.file_name()
					.and_then(|x| x.to_str())
					.map_or(false, |x| filter.is_match(x)))
	}

	fn parse_date_time(time_string: &str) -> Result<NaiveDateTime, ErrorBox>  {
		match NaiveDateTime::parse_from_str(time_string, "%Y-%m-%d %H:%M") {
			Ok(datetime) => Ok(datetime),
			Err(_) => match NaiveDate::parse_from_str(time_string, "%Y-%m-%d") {
				Ok(date) => Ok(date.and_hms_opt(0, 0, 0).unwrap()),
				Err(_) => Err("Failed to parse datetime".into())
			}
		}
	}

	pub fn run(&self) {
		let stopwatch = Stopwatch::start_new();
		Self::get_directories(&self.input_directory)
			.collect::<Vec<PathBuf>>()
			.par_iter()
			.for_each(|ticker_directory| {
				self.process_ticker_directory(ticker_directory);
			});
		println!("Processed all directories in {} ms", stopwatch.elapsed_ms());
	}

	fn process_ticker_directory(&self, ticker_directory: &PathBuf) {
		let stopwatch = Stopwatch::start_new();
		let get_regex = |x| Regex::new(x)
			.expect("Invalid regex"); 
		let daily_filter = get_regex(r"D1\.csv$");
		let intraday_filter = get_regex(r"(H1|M\d+)\.csv$");
		let (daily, daily_excluded) = self.parse_csv_files(ticker_directory, daily_filter, false);
		let (intraday, intraday_excluded) = self.parse_csv_files(ticker_directory, intraday_filter, true);
		let archive_path = self.get_archive_path(ticker_directory);
		let time_zone = self.time_zone.to_string();
		let archive = RawOhlcArchive {
			daily,
			intraday,
			intraday_time_frame: self.intraday_time_frame,
			time_zone
		};
		match write_archive(&archive_path, &archive) {
			Ok(_) => {}
			Err(error) => {
				eprintln!("Failed to write archive: {}", error);
				return;
			}
		}
		if daily_excluded + intraday_excluded > 0 {
			println!(
				"Loaded {} records from \"{}\", excluded {} daily contracts, {} intraday contracts and wrote them to \"{}\" in {} ms",
				archive.daily.len() + archive.intraday.len(),
				ticker_directory.to_str().unwrap(),
				daily_excluded,
				intraday_excluded,
				archive_path.to_str().unwrap(),
				stopwatch.elapsed_ms()
			);
		} else {
			println!(
				"Loaded {} records from \"{}\" and wrote them to \"{}\" in {} ms",
				archive.daily.len() + archive.intraday.len(),
				ticker_directory.to_str().unwrap(),
				archive_path.to_str().unwrap(),
				stopwatch.elapsed_ms()
			);
		}
	}

	fn parse_csv_files(&self, path: &PathBuf, filter: Regex, sort_by_symbol: bool) -> (Vec<RawOhlcRecord>, usize) {
		let csv_paths = Self::get_csv_paths(path, filter);
		let mut ohlc_map = OhlcTreeMap::new();
		let symbol_path = Path::new(path);
		let mut current_filter: Option<ContractFilter> = None;
		if let Some(root_os) = symbol_path.file_name() {
			let root = root_os.to_str().unwrap().to_string();
			if let Some(filter) = self.filters.iter().find(|x| x.root == root) {
				current_filter = Some(filter.clone());
			}
		}
		let mut excluded_contracts = HashSet::new();
		for csv_path in csv_paths {
			read_csv::<CsvRecord>(csv_path, |record| {
				if let Some(filter) = current_filter.as_mut() {
					if !filter.is_included(&record.symbol) {
						excluded_contracts.insert(record.symbol.clone());
						return;
					}
				}
				self.add_ohlc_record(&record, &mut ohlc_map);
			});
			if let Some(filter) = current_filter.as_mut() {
				filter.reset();
			}
		}
		if ohlc_map.values().len() < 250 {
			panic!("Missing data in {}", path.to_str().unwrap());
		}
		let mut records: Vec<RawOhlcRecord> = ohlc_map.into_values().collect();
		records.sort_by(|a, b| {
			if sort_by_symbol {
				a.symbol.cmp(&b.symbol).then_with(|| a.time.cmp(&b.time))
			} else {
				a.time.cmp(&b.time)
			}
		});
		return (records, excluded_contracts.len());
	}

	fn add_ohlc_record(&self, record: &CsvRecord, ohlc_map: &mut OhlcTreeMap) {
		let Ok(time) = Self::parse_date_time(record.time.as_str()) else {
			return;
		};
		let symbol = record.symbol.to_string();
		let key = OhlcKey {
			symbol: symbol.clone(),
			time: time
		};
		let value = RawOhlcRecord {
			symbol,
			time: time,
			open: record.open,
			high: record.high,
			low: record.low,
			close: record.close,
			volume: record.volume,
			open_interest: record.open_interest
		};
		ohlc_map.insert(key, value);
	}

	fn get_archive_path(&self, time_frame_directory: &PathBuf) -> PathBuf {
		let symbol = Self::get_last_token(time_frame_directory);
		let file_name = get_archive_file_name(&symbol);
		return Path::new(&self.output_directory).join(file_name);
	}
}