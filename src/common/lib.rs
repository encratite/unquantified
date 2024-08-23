pub mod backtest;
pub mod manager;
pub mod ohlc;
mod panama;

use std::{cmp::Ordering, fs::File, path::PathBuf};
use configparser::ini::Ini;
use serde::de::DeserializeOwned;
use lazy_static::lazy_static;
use regex::Regex;
use anyhow::{anyhow, Context, Result};
use crate::ohlc::{OhlcArchive, RawOhlcArchive};

lazy_static! {
	static ref GLOBEX_REGEX: Regex = Regex::new("^([A-Z0-9]{2,})([FGHJKMNQUVXZ])([0-9]{2})$").unwrap();
}

#[derive(Eq, PartialEq, PartialOrd)]
struct GlobexCode {
	pub symbol: String,
	pub month: String,
	pub year: u16
}

pub fn parse_globex_code(symbol: &String) -> Option<(String, String, String)> {
	match GLOBEX_REGEX.captures(symbol.as_str()) {
		Some(captures) => {
			let get_capture = |i: usize| captures[i].to_string();
			let root = get_capture(1);
			let month = get_capture(2);
			let year = get_capture(3);
			Some((root, month, year))
		},
		None => None
	}
}

pub fn read_archive(path: &PathBuf, skip_front_contract: bool) -> Result<OhlcArchive> {
	let file = File::open(path)?;
	let mut buffer = Vec::<u8>::new();
	zstd::stream::copy_decode(file, &mut buffer)?;
	let raw_archive: RawOhlcArchive = unsafe { rkyv::from_bytes_unchecked(&buffer)? };
	let archive = raw_archive.to_archive(skip_front_contract)?;
	return Ok(archive);
}

pub fn write_archive(path: &PathBuf, archive: &RawOhlcArchive) -> Result<()> {
	let binary_data = rkyv::to_bytes::<_, 1024>(archive)?;
	let file = File::create(path.clone())?;
	zstd::stream::copy_encode(binary_data.as_slice(), file, 1)?;
	Ok(())
}

pub fn get_ini(path: &str) -> Result<Ini> {
	let mut config = Ini::new();
	config.load(path)
		.map_err(|error| anyhow!(error))
		.with_context(|| format!("Failed to read configuration file \"{path}\""))?;
	Ok(config)
}

pub fn get_archive_file_name(symbol: &String) -> String {
	format!("{symbol}.zrk")
}

pub fn read_csv<T>(path: PathBuf, mut on_record: impl FnMut(T))
where
	T: DeserializeOwned
{
	let mut reader = csv::Reader::from_path(path)
		.expect("Unable to read .csv file");
	let headers = reader.headers()
		.expect("Unable to parse headers")
		.clone();
	let mut string_record = csv::StringRecord::new();
	while reader.read_record(&mut string_record).is_ok() && string_record.len() > 0 {
		let record: T = string_record.deserialize(Some(&headers))
			.expect("Failed to deserialize record");
		on_record(record);
	}
}

impl GlobexCode {
	fn new(symbol: &String) -> Option<GlobexCode> {
		let Some((_, month, year_string)) = parse_globex_code(symbol) else {
			return None;
		};
		let Ok(year) = str::parse::<u16>(year_string.as_str()) else {
			return None;
		};
		let adjusted_year = if year < 70 {
			year + 2000
		} else {
			year + 1900
		};
		let globex_code = GlobexCode {
			symbol: symbol.clone(),
			month,
			year: adjusted_year
		};
		Some(globex_code)
	}
}

impl Ord for GlobexCode {
	fn cmp(&self, other: &Self) -> Ordering {
		self.year
			.cmp(&other.year)
			.then_with(|| self.month.cmp(&other.month))
	}
}