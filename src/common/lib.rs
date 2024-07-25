use std::{
	collections::BTreeMap, error::Error, fs::File, path::PathBuf, str::FromStr
};
use chrono::{DateTime, NaiveDateTime, Utc};
use chrono_tz::Tz;
use rkyv::{
	Archive,
	Deserialize,
	Serialize
};
use configparser::ini::Ini;
use serde::de::DeserializeOwned;

pub type OhlcDailyMap = BTreeMap<DateTime<Utc>, Box<OhlcRecord>>;
pub type OhlcIntradayMap = BTreeMap<OhlcKey, Box<OhlcRecord>>;

#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct RawOhlcArchive {
	pub daily: Vec<RawOhlcRecord>,
	pub intraday: Vec<RawOhlcRecord>,
	pub intraday_time_frame: u16,
	pub time_zone: String
}

#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct RawOhlcRecord {
	pub symbol: String,
	pub time: NaiveDateTime,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
	pub volume: u32,
	pub open_interest: Option<u32>
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OhlcKey {
	pub symbol: String,
	pub time: DateTime<Utc>
}

#[derive(Debug)]
pub struct OhlcArchive {
	pub daily: OhlcDailyMap,
	pub intraday: OhlcIntradayMap,
	pub intraday_time_frame: u16
}

#[derive(Debug, Clone)]
pub struct OhlcRecord {
	pub symbol: String,
	pub time: DateTime<Tz>,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
	pub volume: u32,
	pub open_interest: Option<u32>,
	// The records within each archive form multiple reverse singly linked lists
	// This is relevant for efficiently calculating moving averages without having to repeatedly look up DateTime keys in the BTreeMap
	// In the case of the intraday archive each list stops where the symbol of the contract changes
	pub previous: Option<Box<OhlcRecord>>
}

pub fn read_archive(path: &PathBuf) -> Result<OhlcArchive, Box<dyn Error>> {
	let file = File::open(path)?;
	let mut buffer = Vec::<u8>::new();
	zstd::stream::copy_decode(file, &mut buffer)?;
	let raw_archive: RawOhlcArchive = unsafe { rkyv::from_bytes_unchecked(&buffer)? };
	let archive = raw_archive.to_archive();
	return Ok(archive);
}

pub fn write_archive(path: &PathBuf, archive: &RawOhlcArchive) -> Result<(), Box<dyn Error>> {
	let binary_data = rkyv::to_bytes::<_, 1024>(archive)?;
	let file = File::create(path.clone())?;
	zstd::stream::copy_encode(binary_data.as_slice(), file, 1)?;
	Ok(())
}

pub fn get_config(path: &str) -> Result<Ini, Box<dyn Error>> {
	let mut config = Ini::new();
	match config.load(path) {
		Ok(_) => Ok(config),
		Err(error) => Err(format!("Failed to read configuration file \"{}\": {}", path, error.to_string()).into())
	}
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

impl RawOhlcArchive {
	pub fn to_archive(&self) -> OhlcArchive {
		let time_zone = Tz::from_str(self.time_zone.as_str())
			.expect("Invalid time zone in archive");
		let daily = self.get_daily_archive(&time_zone);
		let intraday = self.get_intraday_archive(&time_zone);
		OhlcArchive {
			daily,
			intraday,
			intraday_time_frame: self.intraday_time_frame
		}
	}

	fn get_daily_archive(&self, time_zone: &Tz) -> OhlcDailyMap {
		let mut daily = OhlcDailyMap::new();
		let mut previous: Option<Box<OhlcRecord>> = None;
		self.daily.iter().for_each(|x| {
			let key = x.time.and_utc();
			let mut value = Box::new(x.to_archive(&time_zone));
			value.previous = previous.clone();
			daily.insert(key, value.clone());
			previous = Some(value);
		});
		daily
	}

	fn get_intraday_archive(&self, time_zone: &Tz) -> OhlcIntradayMap {
		let mut intraday = OhlcIntradayMap::new();
		let mut previous: Option<Box<OhlcRecord>> = None;
		self.intraday.iter().for_each(|x| {
			let key = OhlcKey {
				symbol: x.symbol.clone(),
				time: x.time.and_utc()
			};
			let mut value = Box::new(x.to_archive(&time_zone));
			if let Some(previous_value) = previous.clone() {
				if value.symbol == previous_value.symbol {
					value.previous = previous.clone();
				}
			}
			intraday.insert(key, value.clone());
			previous = Some(value);
		});
		intraday
	}
}

impl RawOhlcRecord {
	pub fn to_archive(&self, time_zone: &Tz) -> OhlcRecord {
		let time_utc = DateTime::<Utc>::from_naive_utc_and_offset(self.time, Utc);
		let time_tz = time_utc.with_timezone(time_zone);
		OhlcRecord {
			symbol: self.symbol.clone(),
			time: time_tz,
			open: self.open,
			high: self.high,
			low: self.low,
			close: self.close,
			volume: self.volume,
			open_interest: self.open_interest,
			previous: None
		}
	}
}