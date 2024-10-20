use std::{collections::{BTreeSet, HashMap, VecDeque}};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;
use std::sync::Arc;
use chrono::NaiveDateTime;
use lazy_static::lazy_static;
use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;
use strum_macros::Display;
use stopwatch::Stopwatch;
use crate::{globex::parse_globex_code, manager::{Asset, AssetManager, AssetType}};
use crate::globex::GlobexCode;
use crate::manager::CsvTimeSeries;
use crate::OhlcArchive;
use crate::ohlc::{OhlcRecord, TimeFrame};
use crate::stats::{mean, standard_deviation_mean};
use crate::strategy::StrategyParameters;
use crate::web::WebF64;

const FOREX_USD: &str = "USD";
const FOREX_EUR: &str = "EUR";
const FOREX_GBP: &str = "GBP";
const FOREX_JPY: &str = "JPY";

const TRADING_DAYS_PER_YEAR: f64 = 252.0;

lazy_static! {
	static ref FOREX_MAP: HashMap<String, String> = {
		let mut map: HashMap<String, String> = HashMap::new();
		map.insert(FOREX_EUR.to_string(), "^EURUSD".to_string());
		map.insert(FOREX_GBP.to_string(), "^GBPUSD".to_string());
		map.insert(FOREX_JPY.to_string(), "^JPYUSD".to_string());
		map
	};
}

type BacktestOrderKeys = (f64, f64, f64);

#[derive(Clone, PartialEq, Display, Debug)]
pub enum PositionSide {
	#[strum(serialize = "long")]
	Long,
	#[strum(serialize = "short")]
	Short
}

#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum EventType {
	OpenPosition,
	ClosePosition,
	Rollover,
	MarginCall,
	Information,
	Warning,
	Error
}

#[derive(Clone)]
pub struct Backtest {
	// Point in time when the backtest starts (from <= t < to)
	from: NaiveDateTime,
	// Point in time when the backtest terminates (from <= t < to)
	to: NaiveDateTime,
	// This struct is where all the backtest parameters other than from/to are from
	configuration: BacktestConfiguration,
	// The asset manager is used to access asset definitions and OHLC records
	asset_manager: Arc<AssetManager>,
	// All cash is kept in USD. There are no separate currency accounts.
	// Buying or selling securities that are traded in other currencies cause implicit conversion.
	cash: f64,
	// Long and short positions held by the account
	positions: Vec<Position>,
	// The current point in time
	now: NaiveDateTime,
	// Controls the speed of the event loop, specified by the strategy
	time_frame: TimeFrame,
	// Fixed points in time the simulation will iterate over
	time_sequence: VecDeque<NaiveDateTime>,
	// Sequential ID used to uniquely identify positions
	next_position_id: u32,
	// Text-based event log, in ascending order
	events: Vec<BacktestEvent>,
	// Daily equity curve data
	equity_curve_daily: Vec<DailyStats>,
	// Equity curve, by trades
	equity_curve_trades: Vec<EquityCurveData>,
	// Maximum account value, used to track drawdowns for equity/drawdown curves
	max_account_value: f64,
	// Maximum drawdown, ranging from 0.0 to -1.0, which technically makes it the minimum... whatever
	max_drawdown: f64,
	// Total fees paid
	fees: f64,
	// Statistics for profits and losses and bars spent in trades specific to long/short side
	profit_duration_stats: Vec<ProfitDurationStats>,
	// Interest rate time series for calculating interest
	fed_funds_rate: Arc<CsvTimeSeries>,
	// Total interest accumulated
	interest: f64,
	// Indicates whether the backtest is still running (terminated = false) or not (terminated = true)
	terminated: bool
}

#[derive(Clone)]
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
	// When account value drops below ruin_ratio * starting_cash, the simulation terminates prematurely
	pub ruin_ratio: f64,
	// If enabled, cash in the margin account will gain interest based on a fixed formula
	pub enable_interest: bool,
	// Enables/disables the event log,
	pub enable_logging: bool
}

#[derive(Clone)]
pub struct Position {
	// Positions are uniquely identified by a sequential ID
	pub id: u32,
	// In case of futures this is the full name of the contract, i.e. a Globex code such as "ESU24"
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
	// Time the position was opened, doesn't get updated when it's partially sold off
	pub time_opened: NaiveDateTime,
	// Number of bars spent in the trade, relevant for statistics
	pub bars_in_trade: u32,
	// Only used for futures, determines if the position should be automatically rolled over
	pub automatic_rollover: Option<bool>
}

#[derive(Clone)]
pub struct SimplePosition {
	pub id: u32,
	pub count: u32,
	pub side: PositionSide
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BacktestEvent {
	time: NaiveDateTime,
	event_type: EventType,
	message: String
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BacktestResult {
	starting_cash: WebF64,
	final_cash: WebF64,
	events: Vec<BacktestEvent>,
	equity_curve_daily: Vec<DailyStats>,
	equity_curve_trades: Vec<EquityCurveData>,
	fees: WebF64,
	fees_percent: WebF64,
	interest: WebF64,
	profit: WebF64,
	annual_average_profit: WebF64,
	total_return: WebF64,
	annual_average_return: WebF64,
	compound_annual_growth_rate: WebF64,
	sharpe_ratio: WebF64,
	sortino_ratio: WebF64,
	calmar_ratio: WebF64,
	max_drawdown: WebF64,
	all_trades: TradeResults,
	long_trades: TradeResults,
	short_trades: TradeResults
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SimplifiedBacktestResult {
	parameters: StrategyParameters,
	trades: u32,
	final_cash: WebF64,
	profit: WebF64,
	annual_average_profit: WebF64,
	total_return: WebF64,
	annual_average_return: WebF64,
	compound_annual_growth_rate: WebF64,
	sharpe_ratio: WebF64,
	sortino_ratio: WebF64,
	calmar_ratio: WebF64,
	max_drawdown: WebF64
}

/*
This data type is the universal result wrapper for both a single backtest as well as a series of backtests
performed by recursive strategy parameter expansion. Since it would be too costly to return the full set
of BacktestResult objects, it only returns the one with the highest Sortino ratio and reduces the others
to a simplified representation that doesn't require as much memory. This simplified representation is used
to render a table of parameters and their performance in the web UI.
*/
#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BacktestSeries {
	best_parameters: StrategyParameters,
	best_result: BacktestResult,
	results: Vec<SimplifiedBacktestResult>,
	median_result: SimplifiedBacktestResult,
	stopwatch: WebF64
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeResults {
	trades: u32,
	profit: WebF64,
	profit_per_trade: WebF64,
	win_rate: WebF64,
	profit_factor: WebF64,
	bars_in_trade: WebF64
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EquityCurveData {
	account_value: WebF64,
	drawdown: WebF64,
	drawdown_percent: WebF64
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DailyStats {
	date: NaiveDateTime,
	equity_curve: EquityCurveData,
	maintenance_margin: WebF64,
	overnight_margin: WebF64
}

#[derive(Clone)]
struct ProfitDurationStats {
	side: PositionSide,
	profit: f64,
	bars_in_trade: u32
}

impl Position {
	pub fn simple(&self) -> SimplePosition {
		SimplePosition {
			id: self.id,
			count: self.count,
			side: self.side.clone()
		}
	}
}

impl Backtest {
	pub fn new(from: NaiveDateTime, to: NaiveDateTime, time_frame: TimeFrame, configuration: BacktestConfiguration, asset_manager: Arc<AssetManager>) -> Result<Rc<RefCell<Backtest>>> {
		if from >= to {
			bail!("Invalid from/to parameters");
		}
		let time_sequence = Self::get_time_sequence(&from, &to, &time_frame, asset_manager.clone())?;
		let equity_curve_data = EquityCurveData {
			account_value: WebF64::new(configuration.starting_cash),
			drawdown: WebF64::new(0.0),
			drawdown_percent: WebF64::precise(0.0),
		};
		let equity_curve_data_daily = DailyStats {
			date: from,
			equity_curve: equity_curve_data.clone(),
			maintenance_margin: WebF64::new(0.0),
			overnight_margin: WebF64::new(0.0)
		};
		let equity_curve_daily = vec![equity_curve_data_daily];
		let equity_curve_trades = vec![equity_curve_data];
		let fed_funds_rate = asset_manager.get_time_series("FEDFUNDS")?;
		let backtest = Backtest {
			from,
			to,
			configuration: configuration.clone(),
			asset_manager,
			cash: configuration.starting_cash,
			positions: Vec::new(),
			now: from,
			time_frame,
			time_sequence,
			next_position_id: 1,
			events: Vec::new(),
			equity_curve_daily,
			equity_curve_trades,
			max_account_value: configuration.starting_cash,
			max_drawdown: 0.0,
			fees: 0.0,
			profit_duration_stats: Vec::new(),
			fed_funds_rate,
			interest: 0.0,
			terminated: false
		};
		Ok(Rc::new(RefCell::new(backtest)))
	}

	/*
	Advances the simulation to the next point in time.
	This sequence is pre-filtered and excludes days on which there was no trading due to holidays etc.
	Returns true if the time was successfully advanced or false if the end of the simulation has been reached.
	An error indicates that a fatal occurred and that the simulation terminated prematurely.
	This may happen due to one of the following reasons:
	- An overnight margin call occurred and liquidating the positions failed due to missing OHLC data
	- An automatic rollover was triggered and the new contract cannot be determined due to missing data
	- The end of the simulated period has been reached and positions cannot be closed
	Returns true if the simulation is done, false otherwise.
	*/
	pub fn next(&mut self) -> Result<bool> {
		if self.terminated {
			bail!("Backtest has been terminated");
		}
		match self.next_internal() {
			Ok(result) => Ok(result),
			Err(error) => {
				self.terminated = true;
				Err(error)
			}
		}
	}

	pub fn open_position(&mut self, symbol: &String, count: u32, side: PositionSide) -> Result<u32> {
		self.open_position_internal(symbol, count, side, Some(true), true, true)
	}

	pub fn close_position(&mut self, position_id: u32, count: u32) -> Result<()> {
		self.close_position_internal(position_id, count, true, true, true)
	}

	pub fn get_result(&self) -> Result<BacktestResult> {
		const DAYS_PER_YEAR: f64 = 365.25;
		let events = self.events.iter().rev().cloned().collect();
		let profit = self.cash - self.configuration.starting_cash;
		let time_difference = self.to - self.from;
		let years = (time_difference.num_days() as f64) / DAYS_PER_YEAR;
		let annual_average_profit = profit / years;
		let return_ratio = self.cash / self.configuration.starting_cash;
		let total_return = return_ratio - 1.0;
		let annual_average_return = total_return / years;
		let compound_annual_growth_rate = return_ratio.powf(1.0 / years) - 1.0;
		let equity_curve_daily = self.equity_curve_daily.clone();
		let (sharpe_ratio, sortino_ratio, calmar_ratio) = self.get_ratios(self.max_drawdown, &equity_curve_daily)?;
		let fees_percent = if profit >= 0.0 {
			self.fees / profit
		} else {
			f64::NAN
		};
		let all_trades = self.get_trade_results(true, true)?;
		let long_trades = self.get_trade_results(true, false)?;
		let short_trades = self.get_trade_results(false, true)?;
		let result = BacktestResult {
			starting_cash: WebF64::new(self.configuration.starting_cash),
			final_cash: WebF64::new(self.cash),
			events,
			equity_curve_daily,
			equity_curve_trades: self.equity_curve_trades.clone(),
			fees: WebF64::new(self.fees),
			fees_percent: WebF64::new(fees_percent),
			interest: WebF64::new(self.interest),
			profit: WebF64::precise(profit),
			annual_average_profit: WebF64::precise(annual_average_profit),
			total_return: WebF64::precise(total_return),
			annual_average_return: WebF64::precise(annual_average_return),
			compound_annual_growth_rate: WebF64::precise(compound_annual_growth_rate),
			sharpe_ratio: WebF64::new(sharpe_ratio),
			sortino_ratio: WebF64::new(sortino_ratio),
			calmar_ratio: WebF64::new(calmar_ratio),
			max_drawdown: WebF64::precise(self.max_drawdown),
			all_trades,
			long_trades,
			short_trades
		};
		Ok(result)
	}

	pub fn get_positions(&self) -> Vec<Position> {
		self.positions.clone()
	}

	pub fn get_position(&self, id: u32) -> Result<Position> {
		self.positions
			.iter()
			.find(|x| x.id == id)
			.cloned()
			.with_context(|| anyhow!("Unable to find position with ID {id}"))
	}

	pub fn get_position_by_root(&self, symbol: &String) -> Option<Position> {
		self.positions
			.iter()
			.find(|x| {
				if let Some(globex_code) = GlobexCode::new(&x.symbol) {
					if globex_code.root == *symbol {
						return true;
					}
				}
				false
			})
			.cloned()
	}

	pub fn get_positions_by_root(&self, symbol: &String) -> Vec<Position> {
		self.positions
			.iter()
			.filter(|x| {
				if let Some(globex_code) = GlobexCode::new(&x.symbol) {
					if globex_code.root == *symbol {
						return true;
					}
				}
				false
			})
			.cloned()
			.collect()
	}

	pub fn get_records(&self, symbol: &String, bars: usize) -> Result<Vec<OhlcRecord>> {
		let archive = self.get_symbol_archive(symbol)?;
		let source = archive.get_data(&self.time_frame);
		/*
		Use .. instead of ..= because the primary use case of this function is filling up buffers with data
		from outside the backtest from/to configuration, when the "next" function of a strategy is executed
		for the first time. The "get_record" function uses ..= instead.
		*/
		let records = source
			.get_adjusted_fallback()
			.range(..self.now)
			.rev()
			.take(bars)
			.map(|(_, record)| record)
			.cloned()
			.collect::<Vec<OhlcRecord>>();
		Ok(records)
	}

	pub fn get_close_values(&self, symbol: &String, bars: usize) -> Result<Vec<f64>> {
		let archive = self.get_symbol_archive(symbol)?;
		let source = archive.get_data(&self.time_frame);
		let records = source
			.get_adjusted_fallback()
			.range(..self.now)
			.rev()
			.take(bars)
			.map(|(_, record)| record.close)
			.collect::<Vec<f64>>();
		Ok(records)
	}

	pub fn get_current_record(&self, symbol: &String) -> Result<OhlcRecord> {
		self.get_record(symbol, self.now, false)
	}

	pub fn most_recent_record(&self, symbol: &String) -> Result<OhlcRecord> {
		self.get_record(symbol, self.now, true)
	}

	pub fn is_available(&self, symbol: &String) -> Result<bool> {
		let archive = self.get_symbol_archive(symbol)?;
		let source = archive.get_data(&self.time_frame);
		let Some((first_timestamp, _)) = source.get_adjusted_fallback().first_key_value() else {
			bail!("Unable to find any records for symbol {symbol}");
		};
		let ordering = self.now.cmp(first_timestamp);
		let is_available = ordering == Ordering::Equal || ordering == Ordering::Greater;
		Ok(is_available)
	}

	pub fn log_event(&mut self, event_type: EventType, message: String) {
		if self.configuration.enable_logging {
			let event = BacktestEvent {
				time: self.now,
				event_type,
				message
			};
			self.events.push(event);
		}
	}

	pub fn disable_logging(&mut self) {
		self.configuration.enable_logging = false;
	}

	pub fn get_time(&self) -> &NaiveDateTime {
		&self.now
	}

	pub fn get_time_frame(&self) -> &TimeFrame {
		&self.time_frame
	}

	pub fn get_configuration(&self) -> &BacktestConfiguration {
		&self.configuration
	}

	pub fn get_asset_manager(&self) -> Arc<AssetManager> {
		self.asset_manager.clone()
	}

	pub fn get_account_value(&self) -> f64 {
		self.get_account_value_internal(true)
	}

	pub fn get_margin(&self, symbol: &String) -> Result<f64> {
		let (_, asset, archive) = self.get_asset(symbol)?;
		let maintenance_margin = self.get_asset_margin(&asset, archive.clone())?;
		let (maintenance_margin_usd, _) = self.convert_currency(&FOREX_USD.to_string(), &asset.currency, maintenance_margin)?;
		Ok(maintenance_margin_usd)
	}

	fn next_internal(&mut self) -> Result<bool> {
		if let Some(now) = self.time_sequence.pop_front() {
			self.margin_call_check()?;
			self.gain_interest()?;
			self.now = now;
			self.update_position_bars();
			self.rollover_contracts()?;
			self.update_daily_stats()?;
			self.ruin_check()?;
		} else {
			// Cash out
			self.close_all_positions()?;
			self.terminated = true;
		}
		Ok(self.terminated)
	}

	fn get_asset(&self, symbol: &String) -> Result<(String, Asset, Arc<OhlcArchive>)> {
		let (root, symbol) = match parse_globex_code(&symbol) {
			Some((root, _, _)) => (root, symbol.clone()),
			None => {
				// Try to interpret the symbol as a futures root
				let root = symbol;
				let Some(resolved_symbol) = self.get_symbol_from_root(root) else {
					bail!("Unable to parse symbol {root}");
				};
				(root.clone(), resolved_symbol)
			}
		};
		let (asset, archive) = self.asset_manager.get_asset(&root)?;
		let output = (symbol, asset, archive);
		Ok(output)
	}

	fn open_position_internal(&mut self, symbol: &String, count: u32, side: PositionSide, automatic_rollover: Option<bool>, enable_fees: bool, enable_logging: bool) -> Result<u32> {
		if count == 0 {
			bail!("Invalid count");
		}
		let (symbol, asset, archive) = self.get_asset(symbol)?;
		if asset.asset_type == AssetType::Futures {
			let current_record = self.current_record(&symbol)?;
			let maintenance_margin = self.get_asset_margin(&asset, archive.clone())?;
			let (maintenance_margin_usd, forex_fee) = self.convert_currency(&FOREX_USD.to_string(), &asset.currency, maintenance_margin)?;
			// Approximate initial margin with a static factor
			let initial_margin = (count as f64) * self.configuration.initial_margin_ratio * maintenance_margin_usd;
			let fees = if enable_fees {
				forex_fee + asset.broker_fee + asset.exchange_fee
			} else {
				0.0
			};
			if initial_margin + fees >= self.cash {
				bail!("Not enough cash to open a position with {count} contract(s) of {symbol} with an initial margin requirement of ${initial_margin:.2}");
			}
			let cost = (count as f64) * maintenance_margin_usd + fees;
			self.cash -= cost;
			self.fees += fees;
			let ask = current_record.close + (self.configuration.futures_spread_ticks as f64) * asset.tick_size;
			let position = Position {
				id: self.next_position_id,
				symbol: current_record.symbol.clone(),
				asset: asset.clone(),
				count,
				side: side.clone(),
				price: ask,
				margin: maintenance_margin_usd,
				archive,
				time_opened: self.now.clone(),
				bars_in_trade: 0,
				automatic_rollover
			};
			self.next_position_id += 1;
			self.positions.push(position.clone());
			if enable_logging {
				let message = format!("Opened {side} position: {count} x {symbol} @ {ask:.2} (ID {})", position.id);
				self.log_event(EventType::OpenPosition, message);
			}
			Ok(position.id)
		} else {
			panic!("Encountered an unknown asset type");
		}
	}

	fn close_position_internal(&mut self, position_id: u32, count: u32, enable_fees: bool, enable_logging: bool, enable_equity_curve: bool) -> Result<()> {
		if count == 0 {
			bail!("Invalid count");
		}
		let position = self.positions
			.iter()
			.find(|x| x.id == position_id)
			.with_context(|| anyhow!("Unable to find a position with ID {position_id}"))?
			.clone();
		if count > position.count {
			bail!("Unable to close position with ID {position_id}, {count} contracts specified but only {} available", position.count);
		}
		let asset = &position.asset;
		let bid;
		if asset.asset_type == AssetType::Futures {
			let (value, profit, position_bid, fees) = self.get_position_value(&position, count, enable_fees)?;
			bid = position_bid;
			self.cash += value;
			self.fees += fees;
			let profit_duration_stats = ProfitDurationStats {
				side: position.side.clone(),
				profit,
				bars_in_trade: position.bars_in_trade
			};
			self.profit_duration_stats.push(profit_duration_stats);
			let new_count = position.count - count;
			if new_count == 0 {
				// The entire position has been sold, remove it
				self.positions.retain(|x| x.id != position_id);
			} else {
				// Awkward workaround to avoid multiple mutable borrows
				for x in self.positions.iter_mut() {
					if x.id == position_id {
						x.count = new_count;
						break;
					}
				}
			}
		} else {
			panic!("Encountered an unknown asset type");
		}
		if enable_logging {
			let message = format!("Closed {} position: {count} x {} @ {bid:.2} (ID {})", position.side, position.symbol, position.id);
			self.log_event(EventType::ClosePosition, message);
		}
		if enable_equity_curve {
			let equity_curve_data = self.update_equity_curve();
			self.equity_curve_trades.push(equity_curve_data);
		}
		Ok(())
	}

	fn update_equity_curve(&mut self) -> EquityCurveData {
		let account_value = self.get_account_value_internal(true);
		if account_value > self.max_account_value {
			self.max_account_value = account_value;
		}
		let drawdown = account_value - self.max_account_value;
		let drawdown_percent = account_value / self.max_account_value - 1.0;
		if drawdown_percent < self.max_drawdown {
			self.max_drawdown = drawdown_percent;
		}
		EquityCurveData {
			account_value: WebF64::new(account_value),
			drawdown: WebF64::new(drawdown),
			drawdown_percent: WebF64::precise(drawdown_percent)
		}
	}

	fn get_symbol_from_root(&self, root: &String) -> Option<String> {
		let Ok(archive) = self.asset_manager.get_archive(root) else {
			return None;
		};
		let data = archive.get_data(&self.time_frame);
		let latest_record_opt = data
			.get_adjusted_fallback()
			.range(..=self.now)
			.next_back()
			.map(|(_, record)| record);
		let Some(latest_record) = latest_record_opt else {
			return None
		};
		Some(latest_record.symbol.clone())
	}

	fn get_asset_margin(&self, asset: &Asset, archive: Arc<OhlcArchive>) -> Result<f64> {
		let current_record = archive.daily.unadjusted
			.range(..=self.now)
			.next_back()
			.map(|(_, record)| record)
			.with_context(|| anyhow!("Unable to find current record for symbol {} at {}", asset.symbol, self.now))?;
		let last_record = archive.daily.unadjusted
			.values()
			.last()
			.with_context(|| "Last record missing")?;
		// Attempt to reconstruct historical maintenance margin using price ratio
		let margin;
		if current_record.close > 0.0 && last_record.close > 0.0 {
			// Try to limit the ratio even though it may very well result in a margin call either way
			let max_ratio = 10f64;
			let price_ratio = f64::min(current_record.close / last_record.close, max_ratio);
			margin = price_ratio * asset.margin;
		} else {
			// Fallback for pathological cases like negative crude
			margin = asset.margin;
		}
		Ok(margin)
	}

	fn convert_currency(&self, from: &String, to: &String, amount: f64) -> Result<(f64, f64)> {
		let get_record = |currency, reciprocal| -> Result<(f64, f64)> {
			let symbol = FOREX_MAP.get(currency)
				.with_context(|| "Unable to find currency")?;
			let record = self.current_record(symbol)?;
			let value = if reciprocal {
				amount / record.close
			} else {
				amount * record.close
			};
			let converted_amount = value / self.configuration.forex_spread;
			Ok((converted_amount, self.configuration.forex_order_fee))
		};
		if from == FOREX_USD {
			if to == FOREX_USD {
				// No conversion required, fees are zero
				Ok((amount, 0.0))
			} else {
				get_record(to, true)
			}
		} else if to == FOREX_USD {
			get_record(from, false)
		} else {
			bail!("Invalid currency pair")
		}
	}

	fn current_record(&self, symbol: &String) -> Result<OhlcRecord> {
		self.get_record(symbol, self.now, false)
	}

	fn get_record(&self, symbol: &String, time: NaiveDateTime, most_recent: bool) -> Result<OhlcRecord> {
		let record;
		let map_error = || anyhow!("Unable to find a record for {symbol} at {}", self.now);
		let get_record = |archive: Arc<OhlcArchive>| -> Result<OhlcRecord> {
			let source = archive.get_data(&self.time_frame);
			let adjusted = source.get_adjusted_fallback();
			let record = if most_recent {
				adjusted.range(..=time)
					.next_back()
					.map(|(_, value)| value.clone())
					.with_context(map_error)?
			} else {
				adjusted.get(&time)
					.with_context(map_error)?
					.clone()
			};
			Ok(record)
		};
		if let Some((root, _, _)) = parse_globex_code(&symbol) {
			let (_, archive) = self.asset_manager.get_asset(&root)?;
			let source = archive.get_data(&self.time_frame);
			let contract_map = source.contract_map
				.as_ref()
				.with_context(|| anyhow!("Archive for {symbol} lacks a contract map"))?;
			if most_recent {
				for (_, contract_records) in contract_map.range(..=time).rev() {
					let record_opt = contract_records
						.iter()
						.find(|x| x.symbol == *symbol);
					if let Some(record) = record_opt {
						return Ok(record.clone());
					}
				}
				bail!("Failed to find a recent matching contract set for {symbol} at {}", self.now);
			} else {
				let contract_records = contract_map.get(&time)
					.with_context(|| anyhow!("Failed to find a contract set for {symbol} at {}", self.now))?;
				record = contract_records
					.iter()
					.find(|x| x.symbol == *symbol)
					.with_context(map_error)?
					.clone();
			}
		} else if FOREX_MAP.values().any(|x| x == symbol) {
			// Bypass asset manager for currencies
			let archive = self.asset_manager.get_archive(symbol)?;
			record = get_record(archive)?;
		} else {
			let (_, archive) = self.asset_manager.get_asset(symbol)?;
			record = get_record(archive)?;
		}
		Ok(record)
	}

	fn get_time_sequence(from: &NaiveDateTime, to: &NaiveDateTime, time_frame: &TimeFrame, asset_manager: Arc<AssetManager>) -> Result<VecDeque<NaiveDateTime>> {
		// Use S&P 500 futures as a timestamp reference for the core loop
		// This only makes sense because the backtest currently targets futures
		let time_reference_symbol = "ES".to_string();
		let time_reference = asset_manager.get_archive(&time_reference_symbol)?;
		// Skip samples outside the configured time range
		let source = time_reference.get_data(time_frame);
		let time_keys: Box<dyn Iterator<Item = &NaiveDateTime>> = Box::new(source.unadjusted.keys());
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

	fn get_account_value_internal(&self, enable_fees: bool) -> f64 {
		let position_value: f64 = self.positions
			.iter()
			.map(|position| self.get_position_value(position, position.count, enable_fees)
				.map(|(value, _, _, _)| value)
				.unwrap_or(0.0))
			.sum();
		let account_value = self.cash + position_value;
		account_value
	}

	fn get_position_value(&self, position: &Position, count: u32, enable_fees: bool) -> Result<(f64, f64, f64, f64)> {
		let asset = &position.asset;
		/*
		This is a questionable workaround for holidays on which lots of CME futures are not being traded,
		while ES is still available and causes the core event loop to iterate over those dates.
		If we used current_record rather than most_recent_record, the equity curve would erroneously show a massive drawdown.
		*/
		let record = self.most_recent_record(&position.symbol)?;
		let margin = (position.count as f64) * position.margin;
		let bid = record.close;
		let ticks = (count as f64) * (bid - position.price) / asset.tick_size;
		let mut profit = ticks * asset.tick_value;
		if position.side == PositionSide::Short {
			profit = -profit;
		}
		let (gain_usd, forex_fee) = self.convert_currency(&asset.currency, &FOREX_USD.to_string(), profit)?;
		let fees = if enable_fees {
			forex_fee + asset.broker_fee + asset.exchange_fee
		} else {
			0.0
		};
		let value = margin + gain_usd - fees;
		Ok((value, profit, bid, fees))
	}

	fn get_account_margin(&self, overnight: bool) -> f64 {
		self.positions
			.iter()
			.map(|x| {
				let mut margin = (x.count as f64) * x.margin;
				if overnight && x.asset.overnight_margin {
					margin *= self.configuration.overnight_margin_ratio;
				}
				margin
			})
			.sum()
	}

	fn margin_call_check(&mut self) -> Result<()> {
		let mut log_margin_call = true;
		loop {
			let Some((position_id, position_count)) = self.get_first_position() else {
				break;
			};
			let account_value = self.get_account_value_internal(true);
			/*
			The current overnight margin check is wrong for two reasons:
			1. It doesn't differentiate between different time zones (US session vs. European session vs. Asian session)
			2. Positions are liquidated at the next close, which is particularly incorrect when using daily rather than intraday data
			*/
			let overnight_margin = self.get_account_margin(true);
			if overnight_margin > account_value {
				// Keep on closing positions until there's enough collateral
				if log_margin_call {
					let message = format!("The overnight margin of ${overnight_margin:.2} exceeds the account value of ${account_value:.2}, closing positions");
					self.log_event(EventType::MarginCall, message);
				}
				let close_result = self.close_position(position_id, position_count);
				if close_result.is_err() {
					let message = "Received a margin call with positions that cannot be liquidated";
					self.log_event(EventType::Error, message.to_string());
					self.terminated = true;
					return close_result;
				}
				log_margin_call = false;
			} else {
				break;
			}
		}
		Ok(())
	}

	fn get_first_position(&self) -> Option<(u32, u32)> {
		match self.positions.first() {
			Some(first_position) => Some((first_position.id, first_position.count)),
			None => None
		}
	}

	fn close_all_positions(&mut self) -> Result<()> {
		let positions = self.positions.clone();
		for position in positions {
			self.close_position(position.id, position.count)
				.with_context(|| "Failed to close all positions at the end of the simulation")?;
		}
		Ok(())
	}

	fn rollover_contracts(&mut self) -> Result<()> {
		let positions = self.positions.clone();
		let futures = positions
			.iter()
			.filter(|position|
				position.asset.asset_type == AssetType::Futures &&
				position.automatic_rollover.is_some_and(|x| x)
			);
		for position in futures {
			let symbol = &position.asset.symbol;
			let Ok(record_now) = self.current_record(symbol) else {
				continue;
			};
			if record_now.symbol != position.symbol {
				// Check if the new contract is more recent than the one in the position we are currently holding
				let globex_current = Self::get_globex_code(&position.symbol)?;
				let globex_new = Self::get_globex_code(&record_now.symbol)?;
				if globex_current.cmp(&globex_new) == Ordering::Less {
					self.close_position_internal(position.id, position.count, false, false, false)?;
					let open_position_result = self.open_position_internal(&record_now.symbol, position.count, position.side.clone(), position.automatic_rollover, true, false);
					match open_position_result {
						Ok(position_id) => {
							let new_position = self.get_position(position_id)?;
							let message = format!("Rolled over {} position: {} x {} @ {:.2} (ID {})", new_position.side, new_position.count, new_position.symbol, new_position.price, new_position.id);
							self.log_event(EventType::Rollover, message);
						},
						Err(error) => {
							// The automatic rollover failed, possibly due to a lack of funds
							// Log the error but do not attempt to re-open the position, let the strategy deal with it
							let message = format!("Rollover failed: {error}");
							self.log_event(EventType::Error, message);
						}
					}
				}
			}
		}
		Ok(())
	}

	fn get_globex_code(symbol: &String) -> Result<GlobexCode> {
		GlobexCode::new(symbol)
			.with_context(|| anyhow!("Unable to parse Globex code {symbol}"))
	}

	fn update_daily_stats(&mut self) -> Result<()> {
		let last_date_opt: Option<NaiveDateTime> = self.equity_curve_daily
			.last()
			.map(|x| x.date);
		let Some(last_date) = last_date_opt else {
			bail!("Equity curve daily data missing");
		};
		// Only update stats if at least one day passed since the last update
		if self.now > last_date {
			let equity_curve_data = self.update_equity_curve();
			let maintenance_margin = self.get_account_margin(false);
			let overnight_margin = self.get_account_margin(true);
			let equity_curve_daily = DailyStats {
				date: self.now,
				equity_curve: equity_curve_data,
				maintenance_margin: WebF64::new(maintenance_margin),
				overnight_margin: WebF64::new(overnight_margin)
			};
			self.equity_curve_daily.push(equity_curve_daily);
		}
		Ok(())
	}

	fn ruin_check(&mut self) -> Result<()> {
		let last = self.equity_curve_daily.last()
			.with_context(|| "Unable to retrieve most recent equity curve value")?;
		if last.equity_curve.account_value.get() < self.configuration.ruin_ratio * self.configuration.starting_cash {
			let message = "Account value too low, terminating backtest";
			self.log_event(EventType::Error, message.to_string());
			self.terminated = true;
		}
		Ok(())
	}

	fn mean_nan(samples: &Vec<f64>) -> f64 {
		let mean = mean(samples.iter());
		Self::error_to_nan(mean)
	}

	fn standard_deviation_nan(samples: &Vec<f64>, mean: f64) -> f64 {
		let standard_deviation = standard_deviation_mean(samples.iter(), mean);
		Self::error_to_nan(standard_deviation)
	}

	fn error_to_nan(result: Result<f64>) -> f64 {
		if let Ok(value) = result {
			value
		} else {
			f64::NAN
		}
	}

	fn get_daily_returns(equity_curve_daily: &Vec<DailyStats>) -> Vec<f64> {
		equity_curve_daily
			.windows(2)
			.filter_map(|window| {
				if let [daily1, daily2] = window {
					let value2 = daily2.equity_curve.account_value.get();
					let value1 = daily1.equity_curve.account_value.get();
					// Filter out the final pathological values of a failed run
					if value1 > 0.0 && value2 > 0.0 {
						let daily_return = value2 / value1 - 1.0;
						Some(daily_return)
					} else {
						None
					}
				} else {
					None
				}
			})
			.collect()
	}

	fn get_trade_results(&self, long: bool, short: bool) -> Result<TradeResults> {
		let source = self.profit_duration_stats
			.iter()
			.filter(|x|
				(long && x.side == PositionSide::Long) ||
				(short && x.side == PositionSide::Short)
			);
		let mut profits_only = 0.0;
		let mut profits_count = 0u32;
		let mut losses_only = 0.0;
		let mut losses_count = 0u32;
		let mut bars_in_trade_sum = 0u32;
		for x in source {
			let profit = x.profit;
			if profit >= 0.0 {
				profits_only += profit;
				profits_count += 1;
			} else {
				losses_only += profit;
				losses_count += 1;
			}
			bars_in_trade_sum += x.bars_in_trade;
		}
		let trades = profits_count + losses_count;
		let profit = profits_only + losses_only;
		let profit_per_trade = profit / (trades as f64);
		let win_rate = (profits_count as f64) / (trades as f64);
		let profit_factor = if profits_count == 0 || losses_count == 0 {
			f64::NAN
		} else {
			(profits_only / losses_only).abs()
		};
		let bars_in_trade = (bars_in_trade_sum as f64) / (trades as f64);
		let results = TradeResults {
			trades,
			profit: WebF64::new(profit),
			profit_per_trade: WebF64::new(profit_per_trade),
			win_rate: WebF64::precise(win_rate),
			profit_factor: WebF64::new(profit_factor),
			bars_in_trade: WebF64::new(bars_in_trade)
		};
		Ok(results)
	}

	fn get_ratios(&self, max_drawdown: f64, equity_curve_daily: &Vec<DailyStats>) -> Result<(f64, f64, f64)> {
		let daily_returns = Self::get_daily_returns(equity_curve_daily);
		let mean_daily_returns = Self::mean_nan(&daily_returns);
		let daily_standard_deviation = Self::standard_deviation_nan(&daily_returns, mean_daily_returns);
		let mean_annual_returns = TRADING_DAYS_PER_YEAR * mean_daily_returns;
		let standard_deviation_factor = TRADING_DAYS_PER_YEAR.sqrt();
		let standard_deviation = standard_deviation_factor * daily_standard_deviation;
		let risk_fre_rate = self.get_risk_free_rate()?;
		let excess_returns = mean_annual_returns - risk_fre_rate;
		let sharpe_ratio = excess_returns / standard_deviation;
		let downside_daily_returns: Vec<f64> = daily_returns
			.iter()
			.filter(|x| **x < 0.0)
			.cloned()
			.collect();
		let mean_downside_daily_returns = Self::mean_nan(&downside_daily_returns);
		let daily_downside_standard_deviation = Self::standard_deviation_nan(&downside_daily_returns, mean_downside_daily_returns);
		let downside_standard_deviation = standard_deviation_factor * daily_downside_standard_deviation;
		let sortino_ratio = excess_returns / downside_standard_deviation;
		let calmar_ratio = mean_annual_returns / max_drawdown.abs();
		let result = (sharpe_ratio, sortino_ratio, calmar_ratio);
		Ok(result)
	}

	fn update_position_bars(&mut self) {
		for position in self.positions.iter_mut() {
			position.bars_in_trade += 1;
		}
	}

	fn get_risk_free_rate(&self) -> Result<f64> {
		let t_bills = self.asset_manager.get_time_series("TB3MS")?;
		let from = self.from.date();
		let to = self.to.date();
		let days = (to - from).num_days().max(1);
		let mut sum = 0.0;
		for day in from.iter_days().take(days as usize) {
			let rate = t_bills.get(&day)? / 100.0;
			sum += rate;
		}
		let mean = sum / (days as f64);
		Ok(mean)
	}

	fn gain_interest(&mut self) -> Result<()> {
		// This is an approximation of the interest formula used by Interactive Brokers
		// Multi-currency interest is currently not supported
		let interpolation_table = vec![
			(       0, 0.0000),
			(   10_000, 0.0000),
			(   20_000, 0.1000),
			(  100_000, 0.9000),
			(  110_000, 0.9091),
			(  120_000, 0.9168),
			(  150_000, 0.9333),
			(  200_000, 0.9501),
			(1_000_000, 0.9901)
		];
		if self.configuration.enable_interest {
			let date = self.now.date();
			let annual_rate = (self.fed_funds_rate.get(&date)? / 100.0 - 0.005).max(0.0);
			let daily_rate = (annual_rate + 1.0).powf(1.0 / TRADING_DAYS_PER_YEAR) - 1.0;
			let Some((_, maximum_ratio)) = interpolation_table.last() else {
				bail!("Empty interpolation table");
			};
			let mut blended_ratio = *maximum_ratio;
			for window in interpolation_table.windows(2) {
				if let [(cash1_int, rate1), (cash2_int, rate2)] = window {
					let cash1 = *cash1_int as f64;
					let cash2 = *cash2_int as f64;
					if self.cash >= cash1 && self.cash < cash2 {
						blended_ratio = (self.cash - cash1) / (cash2 - cash1) * (*rate2 - *rate1) + *rate1;
						break;
					}
				}
			}
			let blended_rate = blended_ratio * daily_rate;
			let interest = blended_rate * self.cash;
			self.cash += interest;
			self.interest += interest;
		}
		Ok(())
	}

	fn get_symbol_archive(&self, symbol: &String) -> Result<Arc<OhlcArchive>> {
		let root = match parse_globex_code(symbol) {
			Some((root, _, _)) => root,
			None => symbol.clone()
		};
		let (_, archive) = self.asset_manager.get_asset(&root)?;
		Ok(archive)
	}
}

impl BacktestResult {
	pub fn simple(&self, parameters: StrategyParameters) -> SimplifiedBacktestResult {
		SimplifiedBacktestResult {
			parameters,
			trades: self.all_trades.trades,
			final_cash: self.final_cash.clone(),
			profit: self.profit.clone(),
			annual_average_profit: self.annual_average_profit.clone(),
			total_return: self.total_return.clone(),
			annual_average_return: self.annual_average_return.clone(),
			compound_annual_growth_rate: self.compound_annual_growth_rate.clone(),
			sharpe_ratio: self.sharpe_ratio.clone(),
			sortino_ratio: self.sortino_ratio.clone(),
			calmar_ratio: self.calmar_ratio.clone(),
			max_drawdown: self.max_drawdown.clone()
		}
	}

	fn get_keys(&self) -> BacktestOrderKeys {
		(self.sortino_ratio.get(), self.sharpe_ratio.get(), self.total_return.get())
	}
}

impl PartialEq for BacktestResult {
	fn eq(&self, other: &Self) -> bool {
		let key1 = self.get_keys();
		let key2 = other.get_keys();
		keys_partial_eq(key1, key2)
	}
}

impl Eq for BacktestResult {}

impl PartialOrd for BacktestResult {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for BacktestResult {
	fn cmp(&self, other: &Self) -> Ordering {
		let keys1 = self.get_keys();
		let keys2 = other.get_keys();
		keys_cmp(keys1, keys2)
	}
}

impl SimplifiedBacktestResult {
	fn get_keys(&self) -> BacktestOrderKeys {
		(self.sortino_ratio.get(), self.sharpe_ratio.get(), self.total_return.get())
	}

	fn average(&self, other: &Self) -> Self {
		let parameters = self.parameters.clone();
		let trades = (self.trades + other.trades) / 2;
		let final_cash = self.final_cash.average(&other.final_cash);
		let profit = self.profit.average(&other.profit);
		let annual_average_profit = self.annual_average_profit.average(&other.annual_average_profit);
		let total_return = self.total_return.average(&other.total_return);
		let annual_average_return = self.annual_average_return.average(&other.annual_average_return);
		let compound_annual_growth_rate = self.compound_annual_growth_rate.average(&other.compound_annual_growth_rate);
		let sharpe_ratio = self.sharpe_ratio.average(&other.sharpe_ratio);
		let sortino_ratio = self.sortino_ratio.average(&other.sortino_ratio);
		let calmar_ratio = self.calmar_ratio.average(&other.calmar_ratio);
		let max_drawdown = self.max_drawdown.average(&other.max_drawdown);
		SimplifiedBacktestResult {
			parameters,
			trades,
			final_cash,
			profit,
			annual_average_profit,
			total_return,
			annual_average_return,
			compound_annual_growth_rate,
			sharpe_ratio,
			sortino_ratio,
			calmar_ratio,
			max_drawdown
		}
	}
}

impl PartialEq for SimplifiedBacktestResult {
	fn eq(&self, other: &Self) -> bool {
		let keys1 = self.get_keys();
		let keys2 = other.get_keys();
		keys_partial_eq(keys1, keys2)
	}
}

impl Eq for SimplifiedBacktestResult {}

impl PartialOrd for SimplifiedBacktestResult {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for SimplifiedBacktestResult {
	fn cmp(&self, other: &Self) -> Ordering {
		let key1 = self.get_keys();
		let key2 = other.get_keys();
		keys_cmp(key1, key2)
	}
}

impl BacktestSeries {
	pub fn new(best_parameters: StrategyParameters, best_result: BacktestResult, results: &Vec<(&StrategyParameters, BacktestResult)>, stopwatch: Stopwatch) -> BacktestSeries {
		let mut simplified_results: Vec<SimplifiedBacktestResult> = results
			.iter()
			.map(|(parameters, result)| result.simple((*parameters).clone()))
			.collect();
		simplified_results.sort_by(|x, y| y.cmp(x));
		let median_result = Self::get_median_result(&simplified_results);
		let stopwatch_secs = WebF64::new(stopwatch.elapsed().as_secs_f64());
		Self {
			best_parameters,
			best_result,
			results: simplified_results,
			median_result,
			stopwatch: stopwatch_secs
		}
	}

	fn get_median_result(simplified_results: &Vec<SimplifiedBacktestResult>) -> SimplifiedBacktestResult {
		let n = simplified_results.len();
		let odd = n % 2 == 1;
		let index1 = n / 2;
		let index2 = index1 + 1;
		let result1 = &simplified_results[index1];
		if odd {
			result1.clone()
		} else {
			let result2 = &simplified_results[index2];
			result1.average(&result2)
		}
	}
}

fn keys_partial_eq(keys1: BacktestOrderKeys, keys2: BacktestOrderKeys) -> bool {
	compare_f64(keys1.0, keys2.0) == Ordering::Equal &&
		compare_f64(keys1.1, keys2.1) == Ordering::Equal &&
		compare_f64(keys1.2, keys2.2) == Ordering::Equal
}

fn keys_cmp(keys1: BacktestOrderKeys, keys2: BacktestOrderKeys) -> Ordering {
	compare_f64(keys1.0, keys2.0)
		.then(compare_f64(keys1.1, keys2.1))
		.then(compare_f64(keys1.2, keys2.2))
}

fn compare_f64(x: f64, y: f64) -> Ordering {
	match (x.is_nan(), y.is_nan()) {
		(true, true) => Ordering::Equal,
		(true, false) => Ordering::Less,
		(false, true) => Ordering::Greater,
		(false, false) => x.partial_cmp(&y).unwrap()
	}
}