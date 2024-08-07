use std::{collections::{BTreeSet, HashMap, VecDeque}, error::Error, sync::Arc};

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use lazy_static::lazy_static;

use common::{OhlcArchive, OhlcRecord};
use regex::Regex;

use crate::manager::{Asset, AssetManager, AssetType};

const FOREX_USD: &str = "USD";
const FOREX_EUR: &str = "EUR";
const FOREX_GBP: &str = "GBP";
const FOREX_JPY: &str = "JPY";

lazy_static! {
	static ref FOREX_MAP: HashMap<String, String> = {
		let mut map: HashMap<String, String> = HashMap::new();
		map.insert(FOREX_EUR.to_string(), "^EURUSD".to_string());
		map.insert(FOREX_GBP.to_string(), "^GBPUSD".to_string());
		map.insert(FOREX_JPY.to_string(), "^JPYUSD".to_string());
		map
	};
}

#[derive(Debug, Clone)]
pub enum PositionSide {
	Long,
	Short
}

#[derive(Debug, Clone, PartialEq)]
pub enum TimeFrame {
	Daily,
	Intraday
}

pub struct Backtest {
	configuration: BacktestConfiguration,
	// The asset manager is used to access asset definitions and OHLC records
	asset_manager: Arc<AssetManager>,
	// All cash is kept in USD. There are no separate currency accounts.
	// Buying or selling securities that are traded in other currencies cause implicit conversion.
	cash: f64,
	// Long and short positions held by the account
	positions: Vec<Position>,
	// The point in time where the simulation starts
	from: DateTime<Utc>,
	// The point in time where the simulation stops
	to: DateTime<Utc>,
	// The current point in time
	now: DateTime<Utc>,
	// Controls the speed of the event loop, specified by the strategy
	time_frame: TimeFrame,
	// Fixed points in time the simulation will iterate over
	time_sequence: VecDeque<DateTime<Utc>>,
	// Sequential position ID
	next_position_id: u32
}

#[derive(Debug, Clone)]
pub struct BacktestConfiguration {
	// Initial cash the backtest starts with, in USD
	pub starting_cash: f64,
	// Commission charged by broker on each currency order, in USD
	pub forex_order_fee: f64,
	// Bid/ask spread on currencies
	// Since OHLC records only contain bid prices, ask prices for currencies are simulated like this:
	// ask = forex_spread * bid
	pub forex_spread: f64,
	// Bid/ask spread on all futures, in ticks:
	// ask = bid + futures_spread_ticks * asset.tick_value
	pub futures_spread_ticks: u8,
	// Initial margin of futures contracts:
	// initial_margin = initial_margin_ratio * asset.margin
	pub initial_margin_ratio: f64,
	// Overnight margin of index futures:
	// overnight_margin = overnight_margin_ratio * asset.margin
	pub overnight_margin_ratio: f64,
	// If asset.physical_delivery is true, then the contract features a close-out period of n days prior to expiration
	// Usually the asset would be forcefully liquidated by that time but this backtest performs an automatic rollover instead
	pub close_out_period: u8
}

#[derive(Debug, Clone)]
pub struct Position {
	// Positions are uniquely identified by a sequential ID
	pub id: u32,
	// In case of futures this is the full name, i.e. a Globex code such as "ESU24"
	pub symbol: String,
	// The underlying asset definition featuring a reference to the OHLC data symbol and contract specs
	pub asset: Asset,
	// Number of contracts
	pub count: u32,
	// Long or short side
	pub side: PositionSide,
	// The price the contracts were originally purchased at
	// Per contract, in the currency of the asset
	pub price: f64,
	// Initial margin that was subtracted from the account's cash value, in USD, per contract.
	// The simulation does not differentiate between initial margin, maintenance margin and overnight margin.
	// This amount is later re-added to the account when the position is closed.
	pub margin: f64,
	// Underlying OHLC archive of the asset
	pub archive: Arc<OhlcArchive>,
	// Time the position was created
	pub time: DateTime<Utc>
}

#[derive(Debug)]
pub struct BacktestResult {
	// Just a placeholder for now
}

impl Backtest {
	pub fn new(&self, from: DateTime<Tz>, to: DateTime<Tz>, time_frame: TimeFrame, configuration: BacktestConfiguration, asset_manager: Arc<AssetManager>) -> Result<Backtest, Box<dyn Error>> {
		if from >= to {
			return Err("Invalid from/to parameters".into());
		}
		let time_sequence = Self::get_time_sequence(&from, &to, &time_frame, &asset_manager)?;
		let backtest = Backtest {
			configuration: configuration.clone(),
			asset_manager,
			cash: configuration.starting_cash,
			positions: Vec::new(),
			from: from.to_utc(),
			to: to.to_utc(),
			now: from.to_utc(),
			time_frame,
			time_sequence,
			next_position_id: 1
		};
		Ok(backtest)
	}

	// Advances the simulation to the next point in time
	// This sequence is pre-filtered and excludes days on which there was no trading due to holidays etc.
	// Returns true if the time was successfully advanced or false if the end of the simulation has been reached
	pub fn next(&mut self) -> Result<bool, Box<dyn Error>> {
		match self.time_sequence.pop_front() {
			Some(now) => {
				// To do: check for margin call
				// To do: settle expired contracts
				panic!("Not implemented");
				self.now = now;
				Ok(true)
			}
			None => Ok(false)
		}
	}

	pub fn open_position(&mut self, symbol: String, count: u32, side: PositionSide) -> Result<Position, Box<dyn Error>> {
		let root = Self::get_contract_root(&symbol)
			.ok_or_else(|| "Unable to parse Globex code")?;
		let (asset, archive) = self.asset_manager.get_asset(&root)?;
		if asset.asset_type == AssetType::Future {
			let (maintenance_margin_per_contract, current_record) = self.get_margin(&asset, archive.clone())?;
			let maintenance_margin = (count as f64) * maintenance_margin_per_contract;
			let (maintenance_margin_usd, forex_fee) = self.convert_currency(&FOREX_USD.to_string(), &asset.currency, maintenance_margin)?;
			// Approximate initial margin with a static factor
			let initial_margin = self.configuration.initial_margin_ratio * maintenance_margin_usd;
			let cost = initial_margin + forex_fee + asset.broker_fee + asset.exchange_fee;
			if cost >= self.cash {
				return Err(format!("Not enough cash to open a position with {} contract(s) of \"{}\" with an initial margin requirement of ${}", count, symbol, initial_margin).into());
			}
			self.cash -= cost;
			let price = current_record.close + (self.configuration.futures_spread_ticks as f64) * asset.tick_size;
			let position = Position {
				id: self.next_position_id,
				symbol: current_record.symbol,
				asset: asset.clone(),
				count,
				side,
				price: price,
				margin: maintenance_margin_usd,
				archive: archive,
				time: self.now.clone()
			};
			self.next_position_id += 1;
			self.positions.push(position.clone());
			Ok(position)
		}
		else {
			panic!("Encountered an unknown asset type");
		}
	}

	pub fn close_position(&mut self, position_id: u32, count: u32) -> Result<(), Box<dyn Error>> {
		let mut position = self.positions.iter().find(|x| x.id == position_id)
			.ok_or_else(|| format!("Unable to find a position with ID {}", position_id))?;
		panic!("Not implemented");
	}

	fn get_margin(&self, asset: &Asset, archive: Arc<OhlcArchive>) -> Result<(f64, Box<OhlcRecord>), Box<dyn Error>> {
		let date = self.now.naive_utc().date().and_hms_opt(0, 0, 0)
			.ok_or_else(|| "Date conversion failed")?;
		let date_utc = DateTime::<Utc>::from_naive_utc_and_offset(date, Utc);
		let current_record = archive.daily.time_map.get(&date_utc)
			.ok_or_else(|| format!("Unable to find current record for symbol \"{}\" at {}", asset.data_symbol, date))?;
		let last_record = archive.daily.unadjusted.last()
			.ok_or_else(|| "Last record missing")?;
		// Attempt to reconstruct historical maintenance margin using price ratio
		let margin = current_record.close / last_record.close * asset.margin;
		Ok((margin, current_record.clone()))
	}

	fn convert_currency(&self, from: &String, to: &String, amount: f64) -> Result<(f64, f64), Box<dyn Error>> {
		let get_record = |currency, reciprocal| -> Result<(f64, f64), Box<dyn Error>> {
			let symbol = FOREX_MAP.get(currency)
					.ok_or_else(|| "Unable to find currency")?;
			let record = self.get_current_record(symbol)?;
			let value = if reciprocal {
				amount / record.close
			}
			else {
				amount * record.close
			};
			let converted_amount = value / self.configuration.forex_spread;
			Ok((converted_amount, self.configuration.forex_order_fee))
		};
		if from == FOREX_USD {
			if to == FOREX_USD {
				Ok((amount, 0f64))
			}
			else {
				get_record(to, true)
			}
		}
		else if to == FOREX_USD {
			get_record(from, false)
		}
		else {
			Err("Invalid currency pair".into())
		}
	}

	fn get_current_record(&self, symbol: &String) -> Result<Box<OhlcRecord>, Box<dyn Error>> {
		let archive = self.asset_manager.get_archive(symbol)?;
		let error = || format!("Unable to find a record for {} at {}", symbol, self.now);
		let source = if self.time_frame == TimeFrame::Daily {
			&archive.daily
		}
		else {
			&archive.intraday
		};
		let record = source.time_map.get(&self.now)
			.ok_or_else(error)?;
		Ok(record.clone())
	}

	fn get_time_sequence(from: &DateTime<Tz>, to: &DateTime<Tz>, time_frame: &TimeFrame, asset_manager: &Arc<AssetManager>) -> Result<VecDeque<DateTime<Utc>>, Box<dyn Error>> {
		// Use S&P 500 futures as a timestamp reference for the core loop
		// This only makes sense because the backtest currently targets futures
		let time_reference_symbol = "ES".to_string();
		let time_reference = asset_manager.get_archive(&time_reference_symbol)?;
		// Skip samples outside the configured time range
		let is_daily = *time_frame == TimeFrame::Daily;
		let source = if is_daily {
			&time_reference.daily
		}
		else {
			&time_reference.intraday
		};
		let time_keys: Box<dyn Iterator<Item = &DateTime<Utc>>> = Box::new(source.time_map.keys());
		let time_keys_in_range = time_keys
			.filter(|&&x|
				x >= *from &&
				x < *to)
			.cloned();
		// Reduce points in time using a B-tree set
		// This is necessary because intraday OHLC archives contain overlapping ranges of contracts
		let time_sequence = BTreeSet::from_iter(time_keys_in_range)
			.into_iter()
			.collect();
		Ok(time_sequence)
	}

	fn get_contract_root(symbol: &String) -> Option<String> {
		let regex = Regex::new(r"^([0-9A-Z][A-Z]{1,2})[F-Z]\d+$").unwrap();
		if let Some(captures) = regex.captures(symbol) {
			let root = captures[1].to_string();
			Some(root)
		}
		else {
			None
		}
	}
}