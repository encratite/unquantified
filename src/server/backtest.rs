use std::{collections::BTreeSet, error::Error, sync::Arc};

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use common::OhlcArchive;

use crate::manager::{Asset, AssetManager};

#[derive(Debug)]
pub enum PositionSide {
	Long,
	Short
}

#[derive(Debug, Clone, PartialEq)]
pub enum TimeFrame {
	Daily,
	Intraday
}

pub trait Strategy {
	fn next(&self);
}

pub struct Backtest {
	// The strategy that is being executed.
	// Only one strategy can be executed at a time.
	strategy: Box<dyn Strategy>,
	configuration: BacktestConfiguration,
	// The asset manager is used to access asset definitions and OHLC records
	asset_manager: Arc<AssetManager>,
	// All cash is kept in USD. There are no separate currency accounts.
	// Buying or selling securities that are traded in other currencies cause implicit conversion.
	cash: f64,
	// Long and short positions held by the account
	positions: Vec<Position>,
	// The current point in time
	now: DateTime<Utc>
}

#[derive(Debug, Clone)]
pub struct BacktestConfiguration {
	// At which point in time to start the backtest
	from: DateTime<Tz>,
	// And when to stop
	to: DateTime<Tz>,
	// Initial cash the backtest starts with, in USD
	starting_cash: f64,
	// Determines how frequently the strategy's next method is invoked by the backtest
	time_frame: TimeFrame,
	// Bid/ask spread on all assets, in ticks
	// Since OHLC records only contain bid prices, ask prices are simulated like this:
	// ask = bid + spread * asset.tick_value
	spread: u8
}

#[derive(Debug)]
pub struct Position {
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
	pub time: DateTime<Tz>
}

#[derive(Debug)]
pub struct BacktestResult {
	// Just a placeholder for now
}

impl Backtest {
	pub fn new(&self, strategy: Box<dyn Strategy>, configuration: BacktestConfiguration, asset_manager: Arc<AssetManager>) -> Result<Backtest, Box<dyn Error>> {
		if configuration.from >= configuration.to {
			return Err("Invalid from/to parameters".into());
		}
		Ok(Backtest {
			strategy,
			configuration: configuration.clone(),
			asset_manager,
			cash: configuration.starting_cash,
			positions: Vec::new(),
			now: configuration.from.to_utc()
		})
	}

	pub fn run(&mut self) -> Result<BacktestResult, Box<dyn Error>> {
		// Use ES timestamps as a timestamp reference for the core loop
		// This only makes sense because the backtest currently targets futures
		let time_reference_symbol = "ES".to_string();
		let time_reference = self.asset_manager.get_archive(&time_reference_symbol)?;
		// Skip samples outside the configured time range
		let is_daily = self.configuration.time_frame == TimeFrame::Daily;
		let time_keys: Box<dyn Iterator<Item = &DateTime<Utc>>> = if is_daily {
			Box::new(time_reference.daily.keys())
		}
		else {
			Box::new(time_reference.intraday
				.keys()
				.map(|x| &x.time))
		};
		let in_range = time_keys
			.filter(|&&x|
				x >= self.configuration.from &&
				x < self.configuration.to);
		// Reduce points in time using a B-tree set
		// This is necessary because intraday OHLC archives contain overlapping ranges of contracts
		let points_in_time: BTreeSet<&DateTime<Utc>> = BTreeSet::from_iter(in_range);
		// Core engine loop
		for &&time in points_in_time.iter() {
			self.now = time.clone();
			self.strategy.next();
		}
		let result = BacktestResult { };
		Ok(result)
	}

	pub fn open_position(&self, symbol: String, count: u32, side: PositionSide) -> Result<Position, Box<dyn Error>> {
		panic!("Not implemented");
	}

	pub fn close_position(&self, position: Position, count: u32) -> Result<(), Box<dyn Error>> {
		panic!("Not implemented");
	}
}