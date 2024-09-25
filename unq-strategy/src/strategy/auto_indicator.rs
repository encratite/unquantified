use std::cell::{Ref, RefCell};
use std::ops::Add;
use anyhow::{bail, Result};
use chrono::TimeDelta;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use unq_common::backtest::Backtest;
use unq_common::strategy::{Strategy, StrategyParameters};
use crate::strategy::indicator::{IndicatorStrategy, SymbolIndicator};
use crate::{get_symbol_contracts, SymbolContracts};
use crate::technical::*;

const WALK_FORWARD_WINDOW_MINIMUM: i64 = 60;
const OPTIMIZATION_PERIOD_MINIMUM: usize = 20;

pub struct AutoIndicatorStrategy<'a> {
	symbol_contracts: SymbolContracts,
	enabled_indicators: Vec<String>,
	indicators: Vec<AutoIndicator>,
	walk_forward_window: i64,
	optimization_period: usize,
	periods_since_optimization: usize,
	backtest: &'a RefCell<Backtest<'a>>
}

pub struct AutoIndicator {
	symbol_indicator: SymbolIndicator,
	enable_long: bool,
	enable_short: bool
}

impl<'a> AutoIndicatorStrategy<'a> {
	pub const ID: &'static str = "auto indicator";

	pub fn new(symbol_contracts: &SymbolContracts, enabled_indicators: &Vec<String>, walk_forward_window: i64, optimization_period: usize, backtest: &'a RefCell<Backtest<'a>>) -> Result<Self> {
		if symbol_contracts.is_empty() {
			bail!("No symbols have been specified");
		}
		if enabled_indicators.is_empty() {
			bail!("No indicators have been specified");
		}
		let known_indicators = vec![
			MomentumIndicator::ID,
			SimpleMovingAverage::ID,
			LinearMovingAverage::ID,
			ExponentialMovingAverage::ID,
			SimpleMovingAverage::CROSSOVER_ID,
			LinearMovingAverage::CROSSOVER_ID,
			ExponentialMovingAverage::CROSSOVER_ID,
			RelativeStrengthIndicator::ID,
			MovingAverageConvergence::ID,
			PercentagePriceOscillator::ID,
			BollingerBands::ID
		];
		for x in enabled_indicators {
			if !known_indicators.contains(&x.as_str()) {
				bail!("Unknown indicator \"{x}\"");
			}
		}
		if walk_forward_window < WALK_FORWARD_WINDOW_MINIMUM {
			bail!("Walk forward window size must be at least {WALK_FORWARD_WINDOW_MINIMUM} bars");
		}
		if optimization_period < OPTIMIZATION_PERIOD_MINIMUM {
			bail!("Optimization period must be at least {OPTIMIZATION_PERIOD_MINIMUM} bars");
		}
		let strategy = Self {
			symbol_contracts: symbol_contracts.clone(),
			enabled_indicators: enabled_indicators.clone(),
			indicators: Vec::new(),
			walk_forward_window,
			optimization_period,
			periods_since_optimization: 0,
			backtest
		};
		Ok(strategy)
	}

	pub fn from_parameters(symbols: &Vec<String>, parameters: &StrategyParameters, backtest: &'a RefCell<Backtest<'a>>) -> Result<Self> {
		let Some(enabled_indicators) = parameters.get_strings("indicators")? else {
			bail!("Missing indicators argument");
		};
		let Some(walk_forward_window) = parameters.get_value("window")? else {
			bail!("Walk forward window size parameter hasn't been specified");
		};
		let walk_forward_window = walk_forward_window as i64;
		let Some(optimization_period) = parameters.get_value("optimization")? else {
			bail!("Optimization period hasn't been specified");
		};
		let optimization_period = optimization_period as usize;
		let symbol_contracts = get_symbol_contracts(&symbols, parameters)?;
		let strategy = AutoIndicatorStrategy::new(&symbol_contracts, &enabled_indicators, walk_forward_window, optimization_period, backtest)?;
		Ok(strategy)
	}

	fn optimize_indicator(&self, symbol: &String, contracts: u32, backtest: &Ref<Backtest>) -> Result<AutoIndicator> {
		let (now, time_frame, configuration, asset_manager) = backtest.get_state();
		let from = now.add(TimeDelta::days(- self.walk_forward_window));
		let to = now.clone();
		let indicators = self.get_indicators()?;
		let performance_results = indicators.par_iter().map(|indicator| {
			let optimization_backtest = Backtest::new(from, to, time_frame.clone(), configuration.clone(), asset_manager);
			todo!()
		});
		todo!()
	}

	fn get_indicators(&self) -> Result<Vec<Box<dyn Indicator>>> {
		let mut indicators: Vec<Box<dyn Indicator>> = Vec::new();
		let periods = vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 40, 50];
		let fast_periods = vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20];
		let slow_periods = vec![10, 15, 20, 25, 30, 40, 50];
		let signal_periods = vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
		let high_thresholds = vec![70.0, 80.0, 85.0, 90.0, 95.0];
		let low_thresholds = vec![5.0, 10.0, 15.0, 20.0, 30.0];
		let multipliers = vec![1.0, 1.5, 1.75, 2.0, 2.25, 2.5, 3.0];
		for indicator_string in self.enabled_indicators.iter() {
			match indicator_string.as_str() {
				MomentumIndicator::ID => {
					for period in periods.iter() {
						let indicator_result = MomentumIndicator::new(*period);
						Self::add_indicator(indicator_result, &mut indicators);
					}
				},
				SimpleMovingAverage::ID => {
					for period in periods.iter() {
						let indicator_result = SimpleMovingAverage::new(*period, None);
						Self::add_indicator(indicator_result, &mut indicators);
					}
				},
				LinearMovingAverage::ID => {
					for period in periods.iter() {
						let indicator_result = LinearMovingAverage::new(*period, None);
						Self::add_indicator(indicator_result, &mut indicators);
					}
				},
				ExponentialMovingAverage::ID => {
					for period in periods.iter() {
						let indicator_result = ExponentialMovingAverage::new(*period, None);
						Self::add_indicator(indicator_result, &mut indicators);
					}
				},
				SimpleMovingAverage::CROSSOVER_ID => {
					for fast_period in fast_periods.iter() {
						for slow_period in slow_periods.iter() {
							let indicator_result = SimpleMovingAverage::new(*fast_period, Some(*slow_period));
							Self::add_indicator(indicator_result, &mut indicators);
						}
					}
				},
				LinearMovingAverage::CROSSOVER_ID => {
					for fast_period in fast_periods.iter() {
						for slow_period in slow_periods.iter() {
							let indicator_result = LinearMovingAverage::new(*fast_period, Some(*slow_period));
							Self::add_indicator(indicator_result, &mut indicators);
						}
					}
				},
				ExponentialMovingAverage::CROSSOVER_ID => {
					for fast_period in fast_periods.iter() {
						for slow_period in slow_periods.iter() {
							let indicator_result = ExponentialMovingAverage::new(*fast_period, Some(*slow_period));
							Self::add_indicator(indicator_result, &mut indicators);
						}
					}
				},
				RelativeStrengthIndicator::ID => {
					for period in periods.iter() {
						for high_threshold in high_thresholds.iter() {
							for low_threshold in low_thresholds.iter() {
								let indicator_result = RelativeStrengthIndicator::new(*period, *high_threshold, *low_threshold);
								Self::add_indicator(indicator_result, &mut indicators);
							}
						}
					}
				},
				MovingAverageConvergence::ID => {
					for signal_period in signal_periods.iter() {
						for fast_period in fast_periods.iter() {
							for slow_period in slow_periods.iter() {
								let indicator_result = MovingAverageConvergence::new(*signal_period, *fast_period, *slow_period);
								Self::add_indicator(indicator_result, &mut indicators);
							}
						}
					}
				},
				PercentagePriceOscillator::ID => {
					for signal_period in signal_periods.iter() {
						for fast_period in fast_periods.iter() {
							for slow_period in slow_periods.iter() {
								let indicator_result = PercentagePriceOscillator::new(*signal_period, *fast_period, *slow_period);
								Self::add_indicator(indicator_result, &mut indicators);
							}
						}
					}
				},
				BollingerBands::ID => {
					for period in periods.iter() {
						for multiplier in multipliers.iter() {
							let indicator_result = BollingerBands::new(*period, *multiplier);
							Self::add_indicator(indicator_result, &mut indicators);
						}
					}
				},
				_ => bail!("Unknown indicator type \"{indicator_string}\"")
			};
		}
		Ok(indicators)
	}

	fn add_indicator<T: Indicator + 'static>(indicator_result: Result<T>, indicators: &mut Vec<Box<dyn Indicator>>) {
		if let Ok(indicator) = indicator_result {
			indicators.push(Box::new(indicator));
		}
	}
}

impl<'a> Strategy for AutoIndicatorStrategy<'a> {
	fn next(&mut self) -> Result<()> {
		if self.periods_since_optimization >= self.optimization_period {
			// We have been running the same indicators for too long
			// Discard them and retrain them using more recent data
			self.indicators.clear();
			self.periods_since_optimization = 0;
		}
		for (symbol, contracts) in self.symbol_contracts.iter() {
			let backtest = self.backtest.borrow();
			if !backtest.is_available(symbol)? {
				// This symbol isn't available on the exchange yet, skip it
				continue;
			}
			let Some(auto_indicator) = self.indicators.iter_mut().find(|x| x.symbol_indicator.symbol == *symbol) else {
				// There is no indicator available for this symbol, train a new one
				todo!()
			};
			let mut indicator = &mut auto_indicator.symbol_indicator.indicator;
			if let Some(initialization_bars) = indicator.needs_initialization() {
				let initialization_records = backtest.get_records(symbol, initialization_bars)?;
				indicator.initialize(&initialization_records);
			}
			let record = backtest.most_recent_record(symbol)?;
			let state = IndicatorStrategy::get_position_state(symbol, &backtest);
			let Some(signal) = indicator.next(&record, state) else {
				return Ok(());
			};
			IndicatorStrategy::trade(signal, auto_indicator.enable_long, auto_indicator.enable_short, &auto_indicator.symbol_indicator, &self.backtest)?;
		}
		self.periods_since_optimization += 1;
		Ok(())
	}
}