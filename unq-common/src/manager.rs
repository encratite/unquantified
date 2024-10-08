use std::collections::HashMap;
use std::collections::BTreeMap;
use std::path::PathBuf;
use serde::Deserialize;
use anyhow::{Context, Result, bail, Error, anyhow};
use chrono::NaiveDate;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use crate::{get_files_by_extension, read_archive, read_csv, OhlcArchive, PathDisplay};

#[derive(Deserialize, Clone, PartialEq)]
pub enum AssetType {
	Futures
}

#[derive(Deserialize, Clone)]
pub struct Asset {
	pub symbol: String,
	pub name: String,
	pub asset_type: AssetType,
	pub currency: String,
	pub tick_size: f64,
	pub tick_value: f64,
	pub margin: f64,
	pub overnight_margin: bool,
	pub broker_fee: f64,
	pub exchange_fee: f64,
	pub physical_delivery: bool
}

pub struct CsvTimeSeries {
	time_series: BTreeMap<NaiveDate, f64>
}

pub struct AssetManager {
	tickers: HashMap<String, OhlcArchive>,
	assets: HashMap<String, Asset>,
	time_series: HashMap<String, CsvTimeSeries>
}

impl AssetManager {
	pub fn new(ticker_directory: &String, csv_directory: &String, asset_path: &String) -> Result<AssetManager> {
		let assets = Self::load_assets(asset_path)?;
		let time_series = Self::load_csv_files(csv_directory)?;
		let tickers = Self::load_archives(ticker_directory, &assets)?;
		let manager = AssetManager {
			tickers,
			assets,
			time_series
		};
		Ok(manager)
	}

	pub fn get_archive(&self, symbol: &String) -> Result<&OhlcArchive> {
		if let Some(archive) = self.tickers.get(symbol) {
			Ok(archive)
		} else {
			bail!("Unable to find an archive for ticker {symbol}");
		}
	}

	pub fn resolve_symbols(&self, symbols: &Vec<String>) -> Result<Vec<String>> {
		let all_keyword = "all";
		if symbols.iter().any(|x| x == all_keyword) {
			let output = self.tickers
				.keys()
				.cloned()
				.collect();
			Ok(output)
		} else {
			Ok(symbols.clone())
		}
	}

	pub fn get_asset(&self, symbol: &String) -> Result<(Asset, &OhlcArchive)> {
		let asset = self.assets.get(symbol)
			.with_context(|| "Unable to find a matching asset definition")?;
		let archive = self.get_archive(symbol)?;
		Ok((asset.clone(), archive))
	}

	pub fn get_time_series(&self, name: &str) -> Result<&CsvTimeSeries> {
		let Some(time_series) = self.time_series.get(name) else {
			bail!("Unable to find time series \"{name}\"");
		};
		Ok(time_series)
	}

	fn load_assets(csv_path: &String) -> Result<HashMap<String, Asset>> {
		let mut assets = HashMap::new();
		read_csv::<Asset>(csv_path.into(), |record| {
			assets.insert(record.symbol.clone(), record);
		})?;
		Ok(assets)
	}

	fn load_archives(ticker_directory: &String, assets: &HashMap<String, Asset>) -> Result<HashMap<String, OhlcArchive>> {
		let stem_paths = get_files_by_extension(ticker_directory, "zrk")?;
		let tuples = stem_paths.par_iter().map(|(symbol, path)| {
			let physical_delivery = Self::physical_delivery(symbol.to_string(), assets);
			let archive = read_archive(path, physical_delivery)?;
			Ok((symbol.clone(), archive))
		}).collect::<Result<Vec<(String, OhlcArchive)>>>()?;
		let map: HashMap<String, OhlcArchive> = tuples.into_iter().collect();
		Ok(map)
	}

	fn physical_delivery(symbol: String, assets: &HashMap<String, Asset>) -> bool {
		assets.values().any(|x|
			x.symbol == *symbol &&
			x.asset_type == AssetType::Futures &&
			x.physical_delivery)
	}

	fn load_csv_files(csv_directory: &String) -> Result<HashMap<String, CsvTimeSeries>> {
		let stem_paths = get_files_by_extension(csv_directory, "csv")?;
		let tuples = stem_paths.par_iter().map(|(key, path)| {
			let time_series = CsvTimeSeries::new(path)?;
			Ok((key.clone(), time_series))
		}).collect::<Result<Vec<(String, CsvTimeSeries)>>>()?;
		let map: HashMap<String, CsvTimeSeries> = tuples.into_iter().collect();
		Ok(map)
	}
}

impl CsvTimeSeries {
	pub fn new(path: &PathBuf) -> Result<CsvTimeSeries> {
		let path_string = path.to_string();
		let mut reader = csv::Reader::from_path(path.clone())
			.with_context(|| anyhow!("Unable to read .csv file from \"{}\"", path_string))?;
		let mut string_record = csv::StringRecord::new();
		let mut map: BTreeMap<NaiveDate, f64> = BTreeMap::new();
		reader.headers()?;
		while reader.read_record(&mut string_record).is_ok() && string_record.len() > 0  {
			let get_string = |i| string_record.get(i)
				.with_context(|| anyhow!("Missing column in .csv file \"{}\"", path_string));
			let date_string = get_string(0)?;
			let date = NaiveDate::parse_from_str(date_string, "%Y-%m-%d")
				.map_err(Error::msg)?;
			let value_string = get_string(1)?;
			let value = value_string.parse::<f64>()?;
			map.insert(date, value);
		}
		let time_series = CsvTimeSeries {
			time_series: map
		};
		Ok(time_series)
	}

	pub fn get(&self, date: &NaiveDate) -> Result<f64> {
		let Some((_, value)) = self.time_series.range(..=date).next_back() else {
			bail!("Unable to find a matching value for date {}", date);
		};
		Ok(*value)
	}
}