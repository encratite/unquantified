use std::{collections::HashMap, error::Error, fs, path::Path, sync::Arc};

use dashmap::DashMap;
use regex::Regex;
use serde::Deserialize;

use common::*;

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub enum AssetType {
	Futures
}

#[derive(Debug, Deserialize, Clone)]
pub struct Asset {
	pub symbol: String,
	pub name: String,
	pub asset_type: AssetType,
	pub data_symbol: String,
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
	ticker_directory: String,
	tickers: DashMap<String, Arc<OhlcArchive>>,
	assets: HashMap<String, Asset>
}

impl AssetManager {
	pub fn new(ticker_directory: String, asset_path: String) -> AssetManager {
		let assets = Self::load_assets(asset_path);
		AssetManager {
			ticker_directory,
			tickers: DashMap::new(),
			assets: assets
		}
	}

	pub fn get_archive(&self, symbol: &String) -> Result<Arc<OhlcArchive>, ErrorBox> {
		// Simple directory traversal check
		let pattern = Regex::new("^[A-Z0-9]+$")?;
		if !pattern.is_match(symbol) {
			return Err("Unable to find an OHLC archive with that symbol".into());
		}
		if let Some(archive_ref) = self.tickers.get(symbol) {
			Ok(Arc::clone(archive_ref.value()))
		}
		else {
			let file_name = get_archive_file_name(symbol);
			let archive_path = Path::new(&self.ticker_directory).join(file_name);
			let physical_delivery = self.physical_delivery(symbol);
			let archive = read_archive(&archive_path, physical_delivery)?;
			let archive_arc = Arc::new(archive);
			self.tickers.insert(symbol.to_string(), Arc::clone(&archive_arc));
			Ok(archive_arc)
		}
	}

	pub fn resolve_symbols(&self, symbols: &Vec<String>) -> Result<Vec<String>, ErrorBox> {
		let all_keyword = "all";
		if symbols.iter().any(|x| x == all_keyword) {
			let data_directory = &self.ticker_directory;
			let entries = fs::read_dir(data_directory)
				.expect("Unable to get list of archives");
			let result = entries
				.filter_map(|x| x.ok())
				.map(|x| x.path())
				.filter(|x| x.is_file())
				.filter(|x| x.extension()
					.and_then(|x| x.to_str()) == Some("zrk"))
				.filter_map(|x| x.file_stem()
					.and_then(|x| x.to_str())
					.map(|x| x.to_string()))
				.collect();
			Ok(result)
		}
		else {
			Ok(symbols.clone())
		}
	}

	pub fn get_asset(&self, symbol: &String) -> Result<(Asset, Arc<OhlcArchive>), ErrorBox> {
		let asset = self.assets.get(symbol)
			.ok_or_else(|| "Unable to find a matching asset definition")?;
		let archive = self.get_archive(&asset.data_symbol)?;
		Ok((asset.clone(), archive))
	}

	fn load_assets(csv_path: String) -> HashMap<String, Asset> {
		let mut assets = HashMap::new();
		read_csv::<Asset>(csv_path.into(), |record| {
			assets.insert(record.symbol.clone(), record);
		});
		return assets;
	}

	fn physical_delivery(&self, symbol: &String) -> bool {
		self.assets.values().any(|x|
			x.data_symbol == *symbol &&
			x.asset_type == AssetType::Futures &&
			x.physical_delivery)
	}
}