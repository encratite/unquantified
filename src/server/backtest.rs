use std::{any::Any, cmp::min, collections::{BTreeSet, HashMap, VecDeque}, error::Error, sync::Arc};

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use lazy_static::lazy_static;

use common::{parse_globex_code, ErrorBox, OhlcArchive, OhlcData, OhlcRecord};
use strum_macros::Display;

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

#[derive(Debug, Clone, PartialEq, Display)]
pub enum PositionSide {
	#[strum(serialize = "long")]
	Long,
	#[strum(serialize = "short")]
	Short
}

#[derive(Debug, Clone, PartialEq)]
pub enum TimeFrame {
	Daily,
	Intraday
}

#[derive(Debug, Clone, PartialEq)]
pub enum EventType {
	OpenPosition,
	ClosePosition,
	MarginCall,
	Error
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
	// Sequential ID used to uniquely identify positions
	next_position_id: u32,
	// Text-based event log, in ascending order
	events: Vec<BacktestEvent>,
	// Indicates whether the backtest is still running or not
	terminated: bool,
	// Internally enables/disables Forex/broker/exchange fees, as a temporary bypass for rolling over contracts
	enable_fees: bool
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
	// If true, automatically roll over futures contracts
	pub automatic_rollover: bool
}

#[derive(Debug, Clone)]
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
	pub time: DateTime<Utc>
}

#[derive(Debug)]
pub struct BacktestEvent {
	pub time: DateTime<Utc>,
	pub event_type: EventType,
	pub message: String
}

#[derive(Debug)]
pub struct BacktestResult {
	// Just a placeholder for now
}

impl Backtest {
	pub fn new(&self, from: DateTime<Tz>, to: DateTime<Tz>, time_frame: TimeFrame, configuration: BacktestConfiguration, asset_manager: Arc<AssetManager>) -> Result<Backtest, ErrorBox> {
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
			next_position_id: 1,
			events: Vec::new(),
			terminated: false,
			enable_fees: true
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
	*/
	pub fn next(&mut self) -> Result<bool, ErrorBox> {
		if self.terminated {
			return Err("Backtest has been terminated".into());
		}
		match self.time_sequence.pop_front() {
			Some(now) => {
				self.margin_call_check()?;
				let then = self.now;
				if self.configuration.automatic_rollover {
					self.rollover_contracts(now, then)?;
				}
				self.now = now;
				Ok(true)
			}
			None => {
				// Cash out
				self.close_all_positions()?;
				self.terminated = true;
				Ok(false)
			}
		}
	}

	pub fn open_position(&mut self, symbol: String, count: u32, side: PositionSide) -> Result<Position, ErrorBox> {
		let root = Self::get_contract_root(&symbol)
			.ok_or_else(|| "Unable to parse Globex code")?;
		let (asset, archive) = self.asset_manager.get_asset(&root)?;
		if asset.asset_type == AssetType::Futures {
			let (maintenance_margin_per_contract, current_record) = self.get_margin(&asset, archive.clone())?;
			let maintenance_margin = (count as f64) * maintenance_margin_per_contract;
			let (maintenance_margin_usd, forex_fee) = self.convert_currency(&FOREX_USD.to_string(), &asset.currency, maintenance_margin)?;
			// Approximate initial margin with a static factor
			let initial_margin = self.configuration.initial_margin_ratio * maintenance_margin_usd;
			let fees = if self.enable_fees {
				forex_fee + asset.broker_fee + asset.exchange_fee
			} else {
				0f64
			};
			let cost = initial_margin + fees;
			if cost >= self.cash {
				return Err(format!("Not enough cash to open a position with {count} contract(s) of {symbol} with an initial margin requirement of ${initial_margin}", ).into());
			}
			self.cash -= cost;
			let ask = current_record.close + (self.configuration.futures_spread_ticks as f64) * asset.tick_size;
			let position = Position {
				id: self.next_position_id,
				symbol: current_record.symbol,
				asset: asset.clone(),
				count,
				side: side.clone(),
				price: ask,
				margin: maintenance_margin_usd,
				archive: archive,
				time: self.now.clone()
			};
			self.next_position_id += 1;
			self.positions.push(position.clone());
			let message = format!("Opened position: {count} x {symbol} @ {ask}, {side} (ID {})", position.id);
			self.log_event(EventType::OpenPosition, message);
			Ok(position)
		} else {
			panic!("Encountered an unknown asset type");
		}
	}

	pub fn close_position(&mut self, position_id: u32, count: u32) -> Result<(), ErrorBox> {
		let position = self.positions
			.iter()
			.find(|x| x.id == position_id)
			.ok_or_else(|| format!("Unable to find a position with ID {position_id}"))?
			.clone();
		if count > position.count {
			let message = format!("Unable to close position with ID {position_id}, {count} contracts specified but only {} available", position.count);
			return Err(message.into())
		}
		let asset = &position.asset;
		let bid;
		if asset.asset_type == AssetType::Futures {
			let (value, position_bid) = self.get_position_value(&position, count)?;
			bid = position_bid;
			self.cash += value;
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
		let message = format!("Closed position: {count} x {} @ {bid}, {} (ID {})", position.symbol, position.side, position.id);
		self.log_event(EventType::ClosePosition, message);
		Ok(())
	}

	fn get_margin(&self, asset: &Asset, archive: Arc<OhlcArchive>) -> Result<(f64, Box<OhlcRecord>), ErrorBox> {
		let date = self.now.naive_utc().date().and_hms_opt(0, 0, 0)
			.ok_or_else(|| "Date conversion failed")?;
		let date_utc = DateTime::<Utc>::from_naive_utc_and_offset(date, Utc);
		let current_record = archive.daily.time_map.get(&date_utc)
			.ok_or_else(|| format!("Unable to find current record for symbol {} at {date}", asset.data_symbol))?;
		let last_record = archive.daily.unadjusted.last()
			.ok_or_else(|| "Last record missing")?;
		// Attempt to reconstruct historical maintenance margin using price ratio
		let margin;
		if current_record.close > 0f64 && last_record.close > 0f64 {
			// Try to limit the ratio even though it may very well result in a margin call either way
			let max_ratio = 10f64;
			let price_ratio = f64::min(current_record.close / last_record.close, max_ratio);
			margin = price_ratio * asset.margin;
		} else {
			// Fallback for pathological cases like negative crude
			margin = asset.margin;
		}
		Ok((margin, Box::clone(current_record)))
	}

	fn convert_currency(&self, from: &String, to: &String, amount: f64) -> Result<(f64, f64), ErrorBox> {
		let get_record = |currency, reciprocal| -> Result<(f64, f64), ErrorBox> {
			let symbol = FOREX_MAP.get(currency)
					.ok_or_else(|| "Unable to find currency")?;
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
				Ok((amount, 0f64))
			} else {
				get_record(to, true)
			}
		} else if to == FOREX_USD {
			get_record(from, false)
		} else {
			Err("Invalid currency pair".into())
		}
	}

	fn get_current_record(&self, symbol: &String) -> Result<Box<OhlcRecord>, ErrorBox> {
		self.get_record(symbol, self.now)
	}

	fn get_record(&self, symbol: &String, time: DateTime<Utc>) -> Result<Box<OhlcRecord>, ErrorBox> {
		let archive = self.asset_manager.get_archive(symbol)?;
		let error = || format!("Unable to find a record for {symbol} at {}", self.now);
		let source = Self::get_archive_data(&archive, &self.time_frame);
		let record = source.time_map.get(&self.now)
			.ok_or_else(error)?;
		Ok(Box::clone(record))
	}

	fn get_time_sequence(from: &DateTime<Tz>, to: &DateTime<Tz>, time_frame: &TimeFrame, asset_manager: &Arc<AssetManager>) -> Result<VecDeque<DateTime<Utc>>, ErrorBox> {
		// Use S&P 500 futures as a timestamp reference for the core loop
		// This only makes sense because the backtest currently targets futures
		let time_reference_symbol = "ES".to_string();
		let time_reference = asset_manager.get_archive(&time_reference_symbol)?;
		// Skip samples outside the configured time range
		let source = Self::get_archive_data(&time_reference, time_frame);
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
		match parse_globex_code(symbol) {
			Some((root, _, _)) => Some(root),
			None => None
		}
	}

	fn get_archive_data<'a>(archive: &'a OhlcArchive, time_frame: &TimeFrame) -> &'a OhlcData {
		if *time_frame == TimeFrame::Daily {
			&archive.daily
		} else {
			&archive.intraday
		}
	}

	fn get_account_value(&self) -> f64 {
		let position_value: f64 = self.positions
			.iter()
			.map(|position| self.get_position_value(position, position.count)
				.map(|(value, _)| value)
				.unwrap_or(0f64))
			.sum();
		let account_value = self.cash + position_value;
		account_value
	}

	fn get_position_value(&self, position: &Position, count: u32) -> Result<(f64, f64), ErrorBox> {
		let asset = &position.asset;
		let record = self.get_current_record(&position.symbol)?;
		let bid = record.close;
		let ticks = (count as f64) * (bid - position.price) / asset.tick_size;
		let mut gain = ticks * asset.tick_value;
		if position.side == PositionSide::Short {
			gain = - gain;
		}
		let (gain_usd, forex_fee) = self.convert_currency(&asset.currency, &FOREX_USD.to_string(), gain)?;
		let cost = if self.enable_fees {
			forex_fee + asset.broker_fee + asset.exchange_fee
		} else {
			0f64
		};
		let margin_released = (count as f64) * asset.margin;
		let value = margin_released + gain_usd - cost;
		Ok((value, bid))
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

	fn margin_call_check(&mut self) -> Result<(), ErrorBox> {
		let mut log_margin_call = true;
		loop {
			let Some((position_id, position_count)) = self.get_first_position() else {
				break;
			};
			let account_value = self.get_account_value();
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
					let message = format!("Received a margin call with positions that cannot be liquidated");
					self.log_event(EventType::Error, message);
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

	fn close_all_positions(&mut self) -> Result<(), ErrorBox> {
		let positions = self.positions.clone();
		for position in positions {
			self.close_position(position.id, position.count)
				.map_err(|error| format!("Failed to close all positions at the end of the simulation: {error}"))?;
		}
		Ok(())
	}

	fn rollover_contracts(&mut self, now: DateTime<Utc>, then: DateTime<Utc>) -> Result<(), ErrorBox> {
		let positions = self.positions.clone();
		let futures = positions
			.iter()
			.filter(|position| position.asset.asset_type == AssetType::Futures);
		for position in futures {
			let symbol = &position.asset.data_symbol;
			let (Ok(record_now), Ok(record_then)) = (self.get_record(symbol, now), self.get_record(symbol, then)) else {
				let message = format!("Failed to roll over contract on asset {} at {}", position.symbol, now);
				return Err(message.into());
			};
			if record_now.symbol != record_then.symbol {
				// Roll over into new contract without even checking if the position matches the old one
				// Temporarily disable fees to simulate the cost of a spread trade
				self.enable_fees = false;
				self.close_position(position.id, position.count)?;
				self.enable_fees = true;
				let new_symbol = Self::get_contract_name(&position.symbol, &record_now.symbol)?;
				self.open_position(new_symbol, position.count, position.side.clone())?;
			}
		}
		Ok(())
	}

	fn parse_globex_code(symbol: &String) -> Result<(String, String, String), ErrorBox> {
		parse_globex_code(symbol)
			.ok_or_else(|| "Invalid Globex code".into())
	}

	fn get_contract_name(root_symbol: &String, data_symbol: &String) -> Result<String, ErrorBox> {
		let (root, _, _) = Self::parse_globex_code(root_symbol)?;
		let (_, month, year) = Self::parse_globex_code(data_symbol)?;
		let new_symbol = format!("{root}{month}{year}");
		Ok(new_symbol)
	}
}