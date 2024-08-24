pub mod backtest;
pub mod manager;
pub mod ohlc;
pub mod globex;
pub mod strategy;
mod panama;

use std::{fs::File, path::PathBuf};
use configparser::ini::Ini;
use serde::de::DeserializeOwned;
use anyhow::{anyhow, Context, Result};
use crate::ohlc::{OhlcArchive, RawOhlcArchive};

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