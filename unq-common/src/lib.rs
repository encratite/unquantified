pub mod backtest;
pub mod manager;
pub mod ohlc;
pub mod globex;
pub mod strategy;
pub mod web;
pub mod stats;
mod panama;

use std::{fs, fs::File, path::PathBuf};
use configparser::ini::Ini;
use serde::de::DeserializeOwned;
use anyhow::{anyhow, bail, Context, Error, Result};
use crate::ohlc::{OhlcArchive, RawOhlcArchive};

pub trait PathDisplay {
	fn to_string(&self) -> &str;
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
		.with_context(|| format!("Failed to read configuration file from \"{path}\""))?;
	Ok(config)
}

pub fn get_archive_file_name(symbol: &String) -> String {
	format!("{symbol}.zrk")
}

pub fn read_csv<T>(path: PathBuf, mut on_record: impl FnMut(T)) -> Result<()>
where
	T: DeserializeOwned
{
	let path_string = path.to_string();
	let mut reader = csv::Reader::from_path(path.clone())
		.with_context(|| anyhow!("Unable to read .csv file from \"{path_string}\""))?;
	let headers = reader.headers()
		.with_context(|| anyhow!("Unable to parse headers in \"{path_string}\""))?
		.clone();
	let mut string_record = csv::StringRecord::new();
	while reader.read_record(&mut string_record).is_ok() && string_record.len() > 0 {
		let record: T = string_record.deserialize(Some(&headers))
			.with_context(|| anyhow!("Failed to deserialize record in \"{path_string}\""))?;
		on_record(record);
	}
	Ok(())
}

fn get_files_by_extension(directory: String, extension: &str) -> Result<Vec<(String, PathBuf)>> {
	let entries = fs::read_dir(&directory)
		.map_err(Error::msg)
		.with_context(|| anyhow!("Failed to read list of files from {directory}"))?;
	let stem_paths = entries
		.filter_map(|x| x.ok())
		.map(|x| x.path())
		.filter(|x| x.is_file())
		.filter(|x| x.extension()
			.and_then(|x| x.to_str()) == Some(extension))
		.map(|path| {
			let Some(file_stem) = path.file_stem() else {
				bail!("Unable to determine file stem");
			};
			let Some(stem) = file_stem.to_str() else {
				bail!("OsPath conversion failed");
			};
			Ok((stem.to_string(), path.clone()))
		})
		.collect::<Result<Vec<(String, PathBuf)>>>()?;
	Ok(stem_paths)
}

impl PathDisplay for PathBuf {
	fn to_string(&self) -> &str {
		match self.to_str() {
			Some(string) => string,
			None => "?"
		}
	}
}