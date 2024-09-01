use std::{collections::{BTreeSet, HashMap, VecDeque}};
use std::cmp::Ordering;
use std::sync::Arc;
use chrono::NaiveDateTime;
use lazy_static::lazy_static;
use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;
use strum_macros::Display;
use crate::{globex::parse_globex_code, manager::{Asset, AssetManager, AssetType}};
use crate::globex::GlobexCode;
use crate::OhlcArchive;
use crate::ohlc::{OhlcArc, TimeFrame};
use crate::web::WebF64;

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

#[derive(Clone, PartialEq, Display)]
pub enum PositionSide {
	#[strum(serialize = "long")]
	Long,
	#[strum(serialize = "short")]
	Short
}

#[derive(Clone, PartialEq, Serialize)]
pub enum EventType {
	OpenPosition,
	ClosePosition,
	Rollover,
	MarginCall,
	Ruin,
	Warning,
	Error
}

pub struct Backtest<'a> {
	configuration: BacktestConfiguration,
	// The asset manager is used to access asset definitions and OHLC records
	asset_manager: &'a AssetManager,
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
	equity_curve_daily: Vec<EquityCurveData>,
	// Equity curve, by trades
	equity_curve_trades: Vec<WebF64>,
	// Total fees paid
	fees: f64,
	// Indicates whether the backtest is still running or not
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
	pub ruin_ratio: f64
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
	// Time the position was created
	pub time: NaiveDateTime,
	// Only used for futures, determines if the position should be automatically rolled over
	pub automatic_rollover: Option<bool>
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BacktestEvent {
	pub time: NaiveDateTime,
	pub event_type: EventType,
	pub message: String
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BacktestResult {
	starting_cash: WebF64,
	final_cash: WebF64,
	events: Vec<BacktestEvent>,
	equity_curve_daily: Vec<EquityCurveData>,
	equity_curve_trades: Vec<WebF64>,
	fees: WebF64
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EquityCurveData {
	pub date: NaiveDateTime,
	pub account_value: WebF64
}

impl<'a> Backtest<'a> {
	pub fn new(from: NaiveDateTime, to: NaiveDateTime, time_frame: TimeFrame, configuration: BacktestConfiguration, asset_manager: &AssetManager) -> Result<Backtest> {
		if from >= to {
			bail!("Invalid from/to parameters");
		}
		let time_sequence = Self::get_time_sequence(&from, &to, &time_frame, asset_manager)?;
		let equity_curve_data = EquityCurveData {
			date: from,
			account_value: WebF64(configuration.starting_cash)
		};
		let equity_curve_daily = vec![equity_curve_data];
		let equity_curve_trades = vec![WebF64(configuration.starting_cash)];
		let backtest = Backtest {
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
			fees: 0f64,
			terminated: false
		};
		Ok(backtest)
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

	pub fn get_result(&self) -> BacktestResult {
		BacktestResult {
			starting_cash: WebF64(self.configuration.starting_cash),
			final_cash: WebF64(self.cash),
			events: self.events.clone(),
			equity_curve_daily: self.equity_curve_daily.clone(),
			equity_curve_trades: self.equity_curve_trades.clone(),
			fees: WebF64(self.fees)
		}
	}

	pub fn get_position(&self, id: u32) -> Result<Position> {
		self.positions
			.iter()
			.find(|x| x.id == id)
			.cloned()
			.with_context(|| anyhow!("Unable to find position with ID {id}"))
	}

	fn next_internal(&mut self) -> Result<bool> {
		match self.time_sequence.pop_front() {
			Some(now) => {
				self.margin_call_check()?;
				self.now = now;
				self.rollover_contracts()?;
				self.update_equity_curve()?;
				self.ruin_check()?;
				Ok(false)
			}
			None => {
				// Cash out
				self.close_all_positions()?;
				self.terminated = true;
				Ok(true)
			}
		}
	}

	fn open_position_internal(&mut self, symbol: &String, count: u32, side: PositionSide, automatic_rollover: Option<bool>, enable_fees: bool, enable_logging: bool) -> Result<u32> {
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
		if asset.asset_type == AssetType::Futures {
			let current_record = self.get_current_record(&symbol)?;
			let maintenance_margin_per_contract = self.get_margin(&asset, archive.clone())?;
			let maintenance_margin = (count as f64) * maintenance_margin_per_contract;
			let (maintenance_margin_usd, forex_fee) = self.convert_currency(&FOREX_USD.to_string(), &asset.currency, maintenance_margin)?;
			// Approximate initial margin with a static factor
			let initial_margin = self.configuration.initial_margin_ratio * maintenance_margin_usd;
			let fees = if enable_fees {
				forex_fee + asset.broker_fee + asset.exchange_fee
			} else {
				0.0
			};
			let cost = initial_margin + fees;
			if cost >= self.cash {
				bail!("Not enough cash to open a position with {count} contract(s) of {symbol} with an initial margin requirement of ${initial_margin}");
			}
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
				time: self.now.clone(),
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
			let (value, position_bid, fees) = self.get_position_value(&position, count, enable_fees)?;
			bid = position_bid;
			self.cash += value;
			self.fees += fees;
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
			let account_value = self.get_account_value(true);
			self.equity_curve_trades.push(WebF64(account_value));
		}
		Ok(())
	}

	fn get_symbol_from_root(&mut self, root: &String) -> Option<String> {
		let Ok(archive) = self.asset_manager.get_archive(root) else {
			return None;
		};
		let data = archive.get_data(&self.time_frame);
		let latest_record = if let Some(record) = data.time_map.get(&self.now) {
			record
		} else {
			let adjusted = data.get_adjusted_fallback();
			let Some(record) = adjusted
				.iter()
				.rev()
				.filter(|x| x.time < self.now)
				.next()
			else {
				return None
			};
			record
		};
		Some(latest_record.symbol.clone())
	}

	fn get_margin(&self, asset: &Asset, archive: Arc<OhlcArchive>) -> Result<f64> {
		let current_record = archive.daily.time_map.get(&self.now)
			.with_context(|| anyhow!("Unable to find current record for symbol {} at {}", asset.symbol, self.now))?;
		let last_record = archive.daily.unadjusted.last()
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
			let record = self.get_current_record(symbol)?;
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
			Err(anyhow!("Invalid currency pair"))
		}
	}

	fn get_current_record(&self, symbol: &String) -> Result<OhlcArc> {
		self.get_record(symbol, self.now)
	}

	fn get_record(&self, symbol: &String, time: NaiveDateTime) -> Result<OhlcArc> {
		let record;
		let map_error = || anyhow!("Unable to find a record for {symbol} at {}", self.now);
		let get_record = |archive: Arc<OhlcArchive>| -> Result<OhlcArc> {
			let source = archive.get_data(&self.time_frame);
			let record = source.time_map.get(&time)
				.with_context(map_error)?
				.clone();
			Ok(record)
		};
		if let Some((root, _, _)) = parse_globex_code(&symbol) {
			let (_, archive) = self.asset_manager.get_asset(&root)?;
			let source = archive.get_data(&self.time_frame);
			let contract_map = source.contract_map
				.as_ref()
				.with_context(|| anyhow!("Archive for {symbol} lacks a contract map"))?;
			let contracts = contract_map.get(&time)
				.with_context(map_error)?;
			record = contracts.iter().find(|&x| x.symbol == *symbol)
				.with_context(|| anyhow!("Unable to find a record for contract {symbol}"))?
				.clone();
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

	fn get_time_sequence(from: &NaiveDateTime, to: &NaiveDateTime, time_frame: &TimeFrame, asset_manager: &AssetManager) -> Result<VecDeque<NaiveDateTime>> {
		// Use S&P 500 futures as a timestamp reference for the core loop
		// This only makes sense because the backtest currently targets futures
		let time_reference_symbol = "ES".to_string();
		let time_reference = asset_manager.get_archive(&time_reference_symbol)?;
		// Skip samples outside the configured time range
		let source = time_reference.get_data(time_frame);
		let time_keys: Box<dyn Iterator<Item = &NaiveDateTime>> = Box::new(source.time_map.keys());
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

	fn get_account_value(&self, enable_fees: bool) -> f64 {
		let position_value: f64 = self.positions
			.iter()
			.map(|position| self.get_position_value(position, position.count, enable_fees)
				.map(|(value, _, _)| value)
				.unwrap_or(0.0))
			.sum();
		let account_value = self.cash + position_value;
		account_value
	}

	fn get_position_value(&self, position: &Position, count: u32, enable_fees: bool) -> Result<(f64, f64, f64)> {
		let asset = &position.asset;
		let record = self.get_current_record(&position.symbol)?;
		let bid = record.close;
		let ticks = (count as f64) * (bid - position.price) / asset.tick_size;
		let mut gain = ticks * asset.tick_value;
		if position.side == PositionSide::Short {
			gain = - gain;
		}
		let (gain_usd, forex_fee) = self.convert_currency(&asset.currency, &FOREX_USD.to_string(), gain)?;
		let fees = if enable_fees {
			forex_fee + asset.broker_fee + asset.exchange_fee
		} else {
			0.0
		};
		let margin_released = (count as f64) * asset.margin;
		let value = margin_released + gain_usd - fees;
		Ok((value, bid, fees))
	}

	fn get_overnight_margin(&self) -> f64 {
		self.positions
			.iter()
			.map(|x| {
				let mut margin = (x.count as f64) * x.margin;
				if x.asset.overnight_margin {
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
			let account_value = self.get_account_value(true);
			/*
			The current overnight margin check is wrong for two reasons:
			1. It doesn't differentiate between different time zones (US session vs. European session vs. Asian session)
			2. Positions are liquidated at the next close, which is particularly incorrect when using daily rather than intraday data
			*/
			let overnight_margin = self.get_overnight_margin();
			if overnight_margin > account_value {
				// Keep on closing positions until there's enough collateral
				if log_margin_call {
					let message = format!("The overnight margin of ${overnight_margin} exceeds the account value of ${account_value}, closing positions");
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

	fn log_event(&mut self, event_type: EventType, message: String) {
		let event = BacktestEvent {
			time: self.now,
			event_type,
			message
		};
		self.events.push(event);
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
			let Ok(record_now) = self.get_current_record(symbol) else {
				continue;
			};
			if record_now.symbol != position.symbol {
				// Check if the new contract is more recent than the one in the position we are currently holding
				let globex_current = Self::get_globex_code(&position.symbol)?;
				let globex_new = Self::get_globex_code(&record_now.symbol)?;
				if globex_current.cmp(&globex_new) == Ordering::Less {
					self.close_position_internal(position.id, position.count, false, false, false)?;
					let position_id = self.open_position_internal(&record_now.symbol, position.count, position.side.clone(), position.automatic_rollover, true, false)?;
					let new_position = self.get_position(position_id)?;
					let message = format!("Rolled over {} position: {} x {} @ {:.2} (ID {})", new_position.side, new_position.count, new_position.symbol, new_position.price, new_position.id);
					self.log_event(EventType::Rollover, message);
				}
			}
		}
		Ok(())
	}

	fn get_globex_code(symbol: &String) -> Result<GlobexCode> {
		GlobexCode::new(symbol)
			.with_context(|| anyhow!("Unable to parse Globex code {symbol}"))
	}

	fn update_equity_curve(&mut self) -> Result<()> {
		let last_date_opt: Option<NaiveDateTime> = self.equity_curve_daily
			.last()
			.map(|x| x.date);
		let Some(last_date) = last_date_opt else {
			bail!("Equity curve daily data missing");
		};
		// Only update the equity curve
		if self.now > last_date {
			let account_value = self.get_account_value(true);
			let equity_curve_data = EquityCurveData {
				date: self.now,
				account_value: WebF64(account_value)
			};
			self.equity_curve_daily.push(equity_curve_data);
		}
		Ok(())
	}

	fn ruin_check(&mut self) -> Result<()> {
		let last = self.equity_curve_daily.last()
			.with_context(|| anyhow!("Unable to retrieve most recent equity curve value"))?;
		if last.account_value.get() < self.configuration.ruin_ratio * self.configuration.starting_cash {
			let message = "Backtest has been terminated because the account value dropped below the ruin ratio";
			self.log_event(EventType::Ruin, message.to_string());
			bail!(message);
		}
		Ok(())
	}
}