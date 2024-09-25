use std::cell::{Ref, RefCell};
use anyhow::{bail, Result};
use unq_common::backtest::{Backtest, PositionSide, SimplePosition};
use unq_common::strategy::{Strategy, StrategyParameters};
use crate::get_symbol_contracts;
use crate::technical::*;

pub struct SymbolIndicator {
	pub symbol: String,
	pub contracts: u32,
	pub indicator: Box<dyn Indicator>
}

impl Clone for SymbolIndicator {
	fn clone(&self) -> Self {
		SymbolIndicator {
			symbol: self.symbol.clone(),
			contracts: self.contracts,
			indicator: self.indicator.clone_box()
		}
	}
}

pub struct IndicatorStrategy<'a> {
	indicators: Vec<SymbolIndicator>,
	enable_long: bool,
	enable_short: bool,
	backtest: &'a RefCell<Backtest<'a>>
}

impl<'a> IndicatorStrategy<'a> {
	pub const ID: &'static str = "indicator";

	pub fn new(indicators: Vec<SymbolIndicator>, enable_long: bool, enable_short: bool, backtest: &'a RefCell<Backtest<'a>>) -> Result<Self> {
		let strategy = Self {
			indicators,
			enable_long,
			enable_short,
			backtest
		};
		Ok(strategy)
	}

	pub fn from_parameters(symbols: &Vec<String>, parameters: &StrategyParameters, backtest: &'a RefCell<Backtest<'a>>) -> Result<Self> {
		let Some(indicator_string) = parameters.get_string("indicator")? else {
			bail!("Missing required parameter \"indicator\"");
		};
		let enable_long = parameters.get_bool("long")?.unwrap_or(true);
		let enable_short = parameters.get_bool("short")?.unwrap_or(true);
		let get_period = |period_opt: Option<usize>| -> Result<usize> {
			if let Some(period) = period_opt {
				Ok(period)
			} else {
				bail!("Missing period parameter")
			}
		};
		let get_multiplier = || {
			let multiplier_opt = parameters.get_value("multiplier")?;
			if let Some(multiplier) = multiplier_opt {
				Ok(multiplier)
			} else {
				bail!("Missing multiplier parameter");
			}
		};
		let get_high_low = |name: &str| -> Result<f64> {
			if let Some(threshold) = parameters.get_value(name)? {
				Ok(threshold)
			} else {
				bail!("Missing threshold parameter")
			}
		};
		let period_opt = Self::get_period("period", parameters)?;
		let signal_period_opt = Self::get_period("signalPeriod", parameters)?;
		let fast_period_opt = Self::get_period("fastPeriod", parameters)?;
		let slow_period_opt = Self::get_period("slowPeriod", parameters)?;
		let indicator: Box<dyn Indicator> = match indicator_string.as_str() {
			MomentumIndicator::ID => {
				let period = get_period(period_opt)?;
				let indicator = MomentumIndicator::new(period)?;
				Box::new(indicator)
			},
			SimpleMovingAverage::ID => {
				let period = get_period(period_opt)?;
				let indicator = SimpleMovingAverage::new(period, None)?;
				Box::new(indicator)
			},
			LinearMovingAverage::ID => {
				let period = get_period(period_opt)?;
				let indicator = LinearMovingAverage::new(period, None)?;
				Box::new(indicator)
			},
			ExponentialMovingAverage::ID => {
				let period = get_period(period_opt)?;
				let indicator = ExponentialMovingAverage::new(period, None)?;
				Box::new(indicator)
			},
			SimpleMovingAverage::CROSSOVER_ID => {
				let fast_period = get_period(fast_period_opt)?;
				let indicator = SimpleMovingAverage::new(fast_period, slow_period_opt)?;
				Box::new(indicator)
			},
			LinearMovingAverage::CROSSOVER_ID => {
				let fast_period = get_period(fast_period_opt)?;
				let indicator = LinearMovingAverage::new(fast_period, slow_period_opt)?;
				Box::new(indicator)
			},
			ExponentialMovingAverage::CROSSOVER_ID => {
				let fast_period = get_period(fast_period_opt)?;
				let indicator = ExponentialMovingAverage::new(fast_period, slow_period_opt)?;
				Box::new(indicator)
			},
			RelativeStrengthIndicator::ID => {
				let period = get_period(period_opt)?;
				let low_threshold = get_high_low("lowThreshold")?;
				let high_threshold = get_high_low("highThreshold")?;
				let indicator = RelativeStrengthIndicator::new(period, low_threshold, high_threshold)?;
				Box::new(indicator)
			},
			MovingAverageConvergence::ID => {
				let signal_period = get_period(signal_period_opt)?;
				let fast_period = get_period(fast_period_opt)?;
				let slow_period = get_period(slow_period_opt)?;
				let indicator = MovingAverageConvergence::new(signal_period, fast_period, slow_period)?;
				Box::new(indicator)
			},
			PercentagePriceOscillator::ID => {
				let signal_period = get_period(signal_period_opt)?;
				let fast_period = get_period(fast_period_opt)?;
				let slow_period = get_period(slow_period_opt)?;
				let indicator = PercentagePriceOscillator::new(signal_period, fast_period, slow_period)?;
				Box::new(indicator)
			},
			BollingerBands::ID => {
				let period = get_period(period_opt)?;
				let multiplier = get_multiplier()?;
				let indicator = BollingerBands::new(period, multiplier)?;
				Box::new(indicator)
			},
			other => bail!("Unknown indicator type \"{other}\"")
		};
		let symbol_contracts = get_symbol_contracts(&symbols, parameters)?;
		let indicators: Vec<SymbolIndicator> = symbol_contracts
			.into_iter()
			.map(|(symbol, contracts)| {
				SymbolIndicator {
					symbol,
					contracts,
					indicator: indicator.clone_box()
				}
			})
			.collect();
		let strategy = Self::new(indicators, enable_long, enable_short, backtest)?;
		Ok(strategy)
	}

	pub fn trade(signal: TradeSignal, enable_long: bool, enable_short: bool, indicator_data: &SymbolIndicator, backtest: &'a RefCell<Backtest<'a>>) -> Result<()> {
		let position_opt = backtest
			.borrow()
			.get_position_by_root(&indicator_data.symbol)
			.map(|x| x.simple());
		if signal == TradeSignal::Close {
			// Close the existing position and do not create a new one
			Self::close_position(&position_opt, backtest);
			return Ok(());
		}
		let target_side = Self::get_target_side(&signal)?;
		if let Some(position) = &position_opt {
			// We already created a position for this symbol, ensure that the side matches
			if position.side != target_side {
				/*
				Two possibilities:
				1. We have a long position and the signal is short
				2. We have a short position and the signal is long
				Close the current position and create a new one with the correct side.
				*/
				Self::close_position(&position_opt, backtest);
				Self::open_position(enable_long, enable_short, target_side, indicator_data, backtest);
			}
		} else {
			// Create a new position for the symbol based on the signal
			Self::open_position(enable_long, enable_short, target_side, indicator_data, backtest);
		};
		Ok(())
	}

	pub fn get_position_state(symbol: &String, backtest: &Ref<Backtest>) -> PositionState {
		let position_opt = backtest.get_position_by_root(symbol);
		let state = if let Some(position) = position_opt {
			if position.side == PositionSide::Long {
				PositionState::Long
			} else {
				PositionState::Short
			}
		} else {
			PositionState::None
		};
		state
	}

	fn get_period(name: &str, parameters: &StrategyParameters) -> Result<Option<usize>> {
		let value = parameters.get_value(name)?;
		let output = value.map(|x| x as usize);
		Ok(output)
	}

	fn get_target_side(signal: &TradeSignal) -> Result<PositionSide> {
		let target_side = match signal {
			TradeSignal::Long => PositionSide::Long,
			TradeSignal::Short => PositionSide::Short,
			_ => bail!("Unknown trade signal")
		};
		Ok(target_side)
	}

	fn open_position(enable_long: bool, enable_short: bool, target_side: PositionSide, indicator_data: &SymbolIndicator, backtest: &'a RefCell<Backtest<'a>>) {
		let long_valid = enable_long && target_side == PositionSide::Long;
		let short_valid = enable_short && target_side == PositionSide::Short;
		if long_valid || short_valid {
			// Suppress errors due to margin requirements or lack of liquidity, it will keep on trying anyway
			let _ = backtest
				.borrow_mut()
				.open_position(&indicator_data.symbol, indicator_data.contracts, target_side);
		}
	}

	fn close_position(position_opt: &Option<SimplePosition>, backtest: &'a RefCell<Backtest<'a>>) {
		if let Some(position) = position_opt {
			let _ = backtest
				.borrow_mut()
				.close_position(position.id, position.count);
		}
	}
}

impl<'a> Strategy for IndicatorStrategy<'a> {
	fn next(&mut self) -> Result<()> {
		for indicator_data in self.indicators.iter_mut() {
			let signal = {
				let symbol = &indicator_data.symbol;
				let indicator = &mut indicator_data.indicator;
				let backtest = self.backtest.borrow();
				if !backtest.is_available(symbol)? {
					// This symbol isn't available on the exchange yet, skip it
					continue;
				}
				if let Some(initialization_bars) = indicator.needs_initialization() {
					// It's the first time the indicator is being invoked
					// Try to fill up its buffer with OHLC data from outside the from/to range to speed up signal generation
					// This can actually make a big difference with big buffers (e.g. EMA)
					let initialization_records = backtest.get_records(symbol, initialization_bars)?;
					indicator.initialize(&initialization_records);
				}
				let record = backtest.most_recent_record(symbol)?;
				let state = Self::get_position_state(symbol, &backtest);
				let Some(signal) = indicator.next(&record, state) else {
					return Ok(());
				};
				signal
			};
			Self::trade(signal, self.enable_long, self.enable_short, indicator_data, &self.backtest)?;
		}
		Ok(())
	}
}