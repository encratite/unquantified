use std::cell::RefCell;
use anyhow::{bail, Result};
use unq_common::backtest::Backtest;
use unq_common::strategy::{Strategy, StrategyParameters};
use crate::technical::{AverageTrueRange, BollingerBands, ExponentialMovingAverage, Indicator, MomentumIndicator, MovingAverageConvergence, PercentagePriceOscillator, RelativeStrengthIndicator, SimpleMovingAverage, WeightedMovingAverage};

pub struct IndicatorStrategy<'a> {
	symbols: Vec<String>,
	indicator: Box<dyn Indicator>,
	backtest: &'a RefCell<Backtest<'a>>
}

impl<'a> IndicatorStrategy<'a> {
	pub fn new(symbols: Vec<String>, indicator: Box<dyn Indicator>, backtest: &'a RefCell<Backtest<'a>>) -> Result<Self> {
		let strategy = Self {
			symbols,
			indicator,
			backtest
		};
		Ok(strategy)
	}

	pub fn from_parameters(symbols: Vec<String>, parameters: &StrategyParameters, backtest: &'a RefCell<Backtest<'a>>) -> Result<Self> {
		let Some(indicator_string) = parameters.get_string("indicator")? else {
			bail!("Missing required parameter \"indicator\"");
		};
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
		let strategy = Self {
			symbols,
			indicator,
			backtest
		};
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
}

impl<'a> Strategy for IndicatorStrategy<'a> {
	fn next(&mut self) -> Result<()> {
		let backtest = self.backtest.borrow_mut();
		todo!()
	}
}