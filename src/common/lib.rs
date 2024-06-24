use std::{
	error::Error,
	fs::File,
	path::PathBuf
};
use chrono::NaiveDateTime;
use rkyv::{
	Archive,
	Deserialize,
	Serialize
};
use configparser::ini::Ini;

pub type OhlcArchive = Vec<OhlcRecord>;

#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct OhlcRecord {
	pub symbol: Option<String>,
	pub time: NaiveDateTime,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
	pub volume: i32,
	pub open_interest: Option<i32>
}

pub fn read_archive(path: &PathBuf) -> Result<OhlcArchive, Box<dyn Error>> {
	let file = File::open(path)?;
	let mut buffer = Vec::<u8>::new();
	zstd::stream::copy_decode(file, &mut buffer)?;
	let archive = unsafe { rkyv::from_bytes_unchecked(&buffer)? };
	return Ok(archive);
}

pub fn write_archive(path: &PathBuf, archive: &OhlcArchive) -> Result<(), Box<dyn Error>> {
	let binary_data = rkyv::to_bytes::<_, 1024>(archive)?;
	let file = File::create(path.clone())?;
	zstd::stream::copy_encode(binary_data.as_slice(), file, 1)?;
	Ok(())
}

pub fn get_config(path: &str) -> Result<Ini, String> {
	let mut config = Ini::new();
	match config.load(path) {
		Ok(_) => {
			return Ok(config);
		},
		Err(_) => {
			return Err(format!("Failed to read configuration file \"{}\"", path));
		}
	}
}