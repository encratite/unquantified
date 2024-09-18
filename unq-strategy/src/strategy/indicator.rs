use std::cell::RefCell;
use anyhow::{bail, Result};
use unq_common::backtest::{Backtest, PositionSide};
use unq_common::strategy::{Strategy, StrategyParameters};
use crate::technical::*;

pub struct SymbolIndicator {
	symbol: String,
	contracts: u32,
	indicator: Box<dyn Indicator>
}

pub struct IndicatorStrategy<'a> {
	indicators: Vec<SymbolIndicator>,
	enable_long: bool,
	enable_short: bool,
	backtest: &'a RefCell<Backtest<'a>>
}

impl<'a> IndicatorStrategy<'a> {
	pub fn new(indicators: Vec<SymbolIndicator>, enable_long: bool, enable_short: bool, backtest: &'a RefCell<Backtest<'a>>) -> Result<Self> {
		let strategy = Self {
			indicators,
			enable_long,
			enable_short,
			backtest
		};
		Ok(strategy)
	}

	pub fn from_parameters(symbols: Vec<String>, parameters: &StrategyParameters, backtest: &'a RefCell<Backtest<'a>>) -> Result<Self> {
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
		let long_threshold = Self::get_threshold("longThreshold", parameters)?;
		let short_threshold = Self::get_threshold("shortThreshold", parameters)?;
		let indicator: Box<dyn Indicator> = match indicator_string.as_str() {
			"momentum" => {
				let period = get_period(period_opt)?;
				let indicator = MomentumIndicator::new(period, long_threshold, short_threshold)?;
				Box::new(indicator)
			},
			"sma" => {
				let fast_period = get_period(fast_period_opt)?;
				let indicator = SimpleMovingAverage::new(fast_period, slow_period_opt, long_threshold, short_threshold)?;
				Box::new(indicator)
			},
			"wma" => {
				let fast_period = get_period(fast_period_opt)?;
				let indicator = WeightedMovingAverage::new(fast_period, slow_period_opt, long_threshold, short_threshold)?;
				Box::new(indicator)
			},
			"ema" => {
				let fast_period = get_period(fast_period_opt)?;
				let indicator = ExponentialMovingAverage::new(fast_period, slow_period_opt, long_threshold, short_threshold)?;
				Box::new(indicator)
			},
			"atr" => {
				let period = get_period(period_opt)?;
				let multiplier = get_multiplier()?;
				let indicator = AverageTrueRange::new(period, multiplier)?;
				Box::new(indicator)
			},
			"rsi" => {
				let period = get_period(period_opt)?;
				let high_threshold = get_high_low("highThreshold")?;
				let low_threshold = get_high_low("lowThreshold")?;
				let indicator = RelativeStrengthIndicator::new(period, high_threshold, low_threshold)?;
				Box::new(indicator)
			},
			"macd" => {
				let signal_period = get_period(signal_period_opt)?;
				let fast_period = get_period(fast_period_opt)?;
				let slow_period = get_period(slow_period_opt)?;
				let indicator = MovingAverageConvergence::new(signal_period, fast_period, slow_period)?;
				Box::new(indicator)
			},
			"ppo" => {
				let signal_period = get_period(signal_period_opt)?;
				let fast_period = get_period(fast_period_opt)?;
				let slow_period = get_period(slow_period_opt)?;
				let indicator = PercentagePriceOscillator::new(signal_period, fast_period, slow_period)?;
				Box::new(indicator)
			},
			"bollinger" => {
				let period = get_period(period_opt)?;
				let multiplier = get_multiplier()?;
				let indicator = BollingerBands::new(period, multiplier)?;
				Box::new(indicator)
			},
			other => bail!("Unknown indicator type \"{other}\"")
		};
		let indicators: Vec<SymbolIndicator> = symbols
			.iter()
			.map(|symbol| {
				SymbolIndicator {
					symbol: symbol.clone(),
					contracts: 1,
					indicator: indicator.clone_box()
				}
			})
			.collect();
		let strategy = Self::new(indicators, enable_long, enable_short, backtest)?;
		Ok(strategy)
	}

	fn get_threshold(name: &str, parameters: &StrategyParameters) -> Result<f64> {
		match parameters.get_value(name)? {
			Some(threshold) => Ok(threshold),
			None => Ok(0.0)
		}
	}

	fn get_period(name: &str, parameters: &StrategyParameters) -> Result<Option<usize>> {
		let value = parameters.get_value(name)?;
		let output = value.map(|x| x as usize);
		Ok(output)
	}

	fn trade(&mut self, indicator_index: usize) -> Result<()> {
		let indicator_data = &mut self.indicators[indicator_index];
		let mut backtest = self.backtest.borrow_mut();
		let record = backtest.most_recent_record(&indicator_data.symbol)?;
		let Some(signal) = indicator_data.indicator.next(&record) else {
			return Ok(());
		};
		if signal == TradeSignal::Hold {
			return Ok(());
		}
		// Hack to make the borrow checker happy
		let position_data = match backtest.get_position_by_root(&indicator_data.symbol) {
			Some(position) => Some((position.id, position.count, position.side)),
			None => None
		};
		let target_side = match signal {
			TradeSignal::Long => PositionSide::Long,
			TradeSignal::Short => PositionSide::Short,
			_ => bail!("Unknown trade signal")
		};
		let open_position = if let Some((position_id, position_count, position_side)) = position_data {
			// We already created a position for this symbol, ensure that the side matches
			if position_side != target_side {
				/*
				Two possibilities:
				1. We have a long position and the signal is short
				2. We have a short position and the signal is long
				Close the position and create a new one, suppressing errors.
				*/
				let _ = backtest.close_position(position_id, position_count);
				true
			} else {
				false
			}
		} else {
			// Create a new position for the symbol based on the signal
			true
		};
		if open_position {
			let long_valid = self.enable_long && target_side == PositionSide::Long;
			let short_valid = self.enable_short && target_side == PositionSide::Short;
			if long_valid || short_valid {
				// Suppress errors due to margin requirements or lack of liquidity, it will keep on trying anyway
				let _ = backtest.open_position(&indicator_data.symbol, indicator_data.contracts, target_side);
			}
		}
		Ok(())
	}
}

impl<'a> Strategy for IndicatorStrategy<'a> {
	fn next(&mut self) -> Result<()> {
		// Hack to make the borrow checker happy
		for indicator_index in 0..self.indicators.len() {
			self.trade(indicator_index)?;
		}
		Ok(())
	}
}