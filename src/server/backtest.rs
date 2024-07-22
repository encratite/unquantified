use std::sync::Arc;

use common::OhlcArchive;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub enum AssetType {
	Future
}

#[derive(Debug)]
pub enum PositionSide {
	Long,
	Short
}

pub struct Backtest {
	configuration: BacktestConfiguration,
	// All cash is kept in USD. There are no separate currency accounts.
	// Buying or selling securities that are traded in other currencies cause implicit conversion.
	cash: f64,
	// Long and short positions held by the account
	positions: Vec<Position>
}

pub struct BacktestConfiguration {
	starting_cash: f64
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
	pub broker_fee: f64,
	pub exchange_fee: f64
}

#[derive(Debug)]
pub struct Position {
	pub asset: Asset,
	// Number of contracts
	pub count: u32,
	// Long or short side
	pub side: PositionSide,
	// Initial margin that was subtracted from the account's cash value, in USD, per contract.
	// The simulation does not differentiate between initial margin, maintenance margin and overnight margin.
	// This amount is later re-added to the account when the position is closed.
	pub margin: f64,
	// Underlying OHLC archive of the asset
	pub archive: Arc<OhlcArchive>
}

impl Backtest {
}