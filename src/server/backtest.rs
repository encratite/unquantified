use std::{error::Error, sync::Arc};

use common::OhlcArchive;

use crate::manager::Asset;

#[derive(Debug)]
pub enum PositionSide {
	Long,
	Short
}

#[derive(Debug, Clone)]
pub enum TimeFrame {
	Daily,
	Intraday
}

pub struct Backtest {
	configuration: BacktestConfiguration,
	// All cash is kept in USD. There are no separate currency accounts.
	// Buying or selling securities that are traded in other currencies cause implicit conversion.
	cash: f64,
	// Long and short positions held by the account
	positions: Vec<Position>
}

#[derive(Debug, Clone)]
pub struct BacktestConfiguration {
	// Initial cash the backtest starts with, in USD
	starting_cash: f64,
	// Determines how frequently the strategy's next method is invoked by the backtest.
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
	pub archive: Arc<OhlcArchive>
}

impl Backtest {
	pub fn new(&self, configuration: BacktestConfiguration) -> Backtest {
		Backtest {
			configuration: configuration.clone(),
			cash: configuration.starting_cash,
			positions: Vec::new()
		}
	}

	pub fn open_position(&self, symbol: String, count: u32, side: PositionSide) -> Result<Position, Box<dyn Error>> {
		panic!("Not implemented");
	}

	pub fn close_position(&self, position: Position, count: u32) -> Result<(), Box<dyn Error>> {
		panic!("Not implemented");
	}
}