use std::{collections::HashMap, fs};
use std::path::PathBuf;
use serde::Deserialize;
use anyhow::{Context, Result, bail, Error, anyhow};
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use crate::{read_archive, read_csv, OhlcArchive};

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

pub struct AssetManager {
	tickers: HashMap<String, OhlcArchive>,
	assets: HashMap<String, Asset>
}

impl AssetManager {
	pub fn new(ticker_directory: String, asset_path: String) -> Result<AssetManager> {
		let assets = Self::load_assets(asset_path);
		let tickers = Self::load_archives(ticker_directory, &assets)?;
		let manager = AssetManager {
			tickers,
			assets
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

	fn load_assets(csv_path: String) -> HashMap<String, Asset> {
		let mut assets = HashMap::new();
		read_csv::<Asset>(csv_path.into(), |record| {
			assets.insert(record.symbol.clone(), record);
		});
		return assets;
	}

	fn load_archives(ticker_directory: String, assets: &HashMap<String, Asset>) -> Result<HashMap<String, OhlcArchive>> {
		let entries = fs::read_dir(&ticker_directory)
			.map_err(Error::msg)
			.with_context(|| anyhow!("Unable to get list of archives from {ticker_directory}"))?;
		let paths: Vec<PathBuf> = entries
			.filter_map(|x| x.ok())
			.map(|x| x.path())
			.filter(|x| x.is_file())
			.filter(|x| x.extension()
				.and_then(|x| x.to_str()) == Some("zrk"))
			.collect();
		let tuples = paths.par_iter().map(|path| {
			let Some(file_stem) = path.file_stem() else {
				bail!("Unable to determine file stem");
			};
			let Some(symbol) = file_stem.to_str() else {
				bail!("OsPath conversion failed");
			};
			let physical_delivery = Self::physical_delivery(symbol.to_string(), assets);
			let archive = read_archive(path, physical_delivery)?;
			Ok((symbol.to_string(), archive))
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
}