use std::{collections::{BTreeSet, VecDeque}, error::Error, sync::Arc};

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
	time_sequence: VecDeque<DateTime<Utc>>
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
	pub overnight_margin_ratio: f64
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
			time_sequence
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
				panic!("Not implemented");
				self.now = now;
				Ok(true)
			}
			None => Ok(false)
		}
	}

	pub fn open_position(&mut self, symbol: String, count: u32, side: PositionSide) -> Result<Position, Box<dyn Error>> {
		panic!("Not implemented");
	}

	pub fn close_position(&mut self, position: Position, count: u32) -> Result<(), Box<dyn Error>> {
		panic!("Not implemented");
	}

	fn get_time_sequence(from: &DateTime<Tz>, to: &DateTime<Tz>, time_frame: &TimeFrame, asset_manager: &Arc<AssetManager>) -> Result<VecDeque<DateTime<Utc>>, Box<dyn Error>> {
		// Use S&P 500 futures as a timestamp reference for the core loop
		// This only makes sense because the backtest currently targets futures
		let time_reference_symbol = "ES".to_string();
		let time_reference = asset_manager.get_archive(&time_reference_symbol)?;
		// Skip samples outside the configured time range
		let is_daily = *time_frame == TimeFrame::Daily;
		let time_keys: Box<dyn Iterator<Item = &DateTime<Utc>>> = if is_daily {
			Box::new(time_reference.daily.keys())
		}
		else {
			Box::new(time_reference.intraday
				.keys()
				.map(|x| &x.time))
		};
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
}