mod datetime;

use std::{
	error::Error,
	fs::File,
	path::PathBuf,
	str::FromStr
};
use chrono::{DateTime, NaiveDateTime, Utc};
use chrono_tz::Tz;
use datetime::DateTimeRkyv;
use rkyv::{
	Archive, Deserialize, Fallible, Serialize
};
use configparser::ini::Ini;
use serde::de::DeserializeOwned;

#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct OhlcArchive {
	pub daily: Vec<OhlcRecord>,
	pub intraday: Vec<OhlcRecord>,
	pub intraday_time_frame: u16,
	pub time_zone: String
}

#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct OhlcRecord {
	pub symbol: String,
	pub time: NaiveDateTime,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
	pub volume: u32,
	pub open_interest: Option<u32>,
	#[with(DateTimeRkyv)]
	pub test: DateTime<Tz>
}

pub struct ArchiveDeserializer;

impl Fallible for ArchiveDeserializer {
	type Error = chrono_tz::ParseError;
}

impl OhlcArchive {
	pub fn add_tz(&self, time: NaiveDateTime) -> DateTime<Tz> {
		let time_utc = DateTime::<Utc>::from_naive_utc_and_offset(time, Utc);
		let time_zone = Tz::from_str(self.time_zone.as_str())
			.expect("Invalid time zone in archive");
		time_utc.with_timezone(&time_zone)
	}
}

pub fn read_archive(path: &PathBuf) -> Result<OhlcArchive, Box<dyn Error>> {
	let file = File::open(path)?;
	let mut buffer = Vec::<u8>::new();
	zstd::stream::copy_decode(file, &mut buffer)?;
	let archived_root = unsafe { rkyv::archived_root::<OhlcArchive>(&buffer) };
    let archive: OhlcArchive = archived_root.deserialize(&mut ArchiveDeserializer).unwrap();
	return Ok(archive);
}

pub fn write_archive(path: &PathBuf, archive: &OhlcArchive) -> Result<(), Box<dyn Error>> {
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