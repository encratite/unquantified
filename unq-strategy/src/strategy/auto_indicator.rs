use std::cell::{Ref, RefCell};
use std::ops::Add;
use anyhow::{bail, Result};
use chrono::TimeDelta;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use unq_common::backtest::{Backtest, BacktestResult, EventType};
use unq_common::strategy::{Strategy, StrategyParameters};
use crate::strategy::indicator::{IndicatorStrategy, SymbolIndicator};
use crate::{get_symbol_contracts, SymbolContracts};
use crate::technical::*;

const WALK_FORWARD_WINDOW_MINIMUM: i64 = 60;
const OPTIMIZATION_PERIOD_MINIMUM: usize = 5;

pub struct AutoIndicatorStrategy {
	symbol_contracts: SymbolContracts,
	enabled_indicators: Vec<String>,
	indicators: Vec<AutoIndicator>,
	walk_forward_window: i64,
	optimization_period: usize,
	periods_since_optimization: usize,
	backtest: RefCell<Backtest>
}

#[derive(Clone)]
pub struct AutoIndicator {
	symbol_indicator: SymbolIndicator,
	enable_long: bool,
	enable_short: bool
}

impl AutoIndicatorStrategy {
	pub const ID: &'static str = "auto indicator";

	pub fn new(symbol_contracts: &SymbolContracts, enabled_indicators: &Vec<String>, walk_forward_window: i64, optimization_period: usize, backtest: RefCell<Backtest>) -> Result<Self> {
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
			BollingerBands::ID,
			KeltnerChannel::ID,
			DonchianChannel::ID
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

	pub fn from_parameters(symbols: &Vec<String>, parameters: &StrategyParameters, backtest: RefCell<Backtest>) -> Result<Self> {
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
		let indicators = self.get_indicators(symbol, contracts)?;
		let enable_table = vec![
			(false, true),
			(true, false),
			(true, true)
		];
		let performance = indicators.into_par_iter().map(|symbol_indicator| -> Result<Vec<(AutoIndicator, BacktestResult)>> {
			enable_table.iter().map(|(enable_long, enable_short)| {
				let enable_long = *enable_long;
				let enable_short = *enable_short;
				let mut optimization_backtest = Backtest::new(from, to, time_frame.clone(), configuration.clone(), asset_manager.clone())?;
				// Disable logging in order to improve performance of optimization runs
				optimization_backtest.disable_logging();
				let backtest_refcell = RefCell::new(optimization_backtest);
				let strategy_indicators = vec![symbol_indicator.clone()];
				let mut strategy = IndicatorStrategy::new(strategy_indicators, enable_long, enable_short, backtest_refcell.clone())?;
				let mut done = false;
				while !done {
					strategy.next()?;
					let mut backtest_mut = backtest_refcell.borrow_mut();
					done = backtest_mut.next()?;
				}
				let backtest = backtest_refcell.borrow_mut();
				let result = backtest.get_result()?;
				let auto_indicator = AutoIndicator {
					symbol_indicator: symbol_indicator.clone(),
					enable_long,
					enable_short
				};
				let output = (auto_indicator, result);
				Ok(output)
			})
				.collect::<Result<Vec<(AutoIndicator, BacktestResult)>>>()
		})
			.collect::<Result<Vec<Vec<(AutoIndicator, BacktestResult)>>>>()?
			.into_iter()
			.flatten()
			.collect::<Vec<(AutoIndicator, BacktestResult)>>();
		// Select best indicator by Sortino ratio, Sharpe ratio and total returns for the optimization period
		let Some((best_indicator, _)) = performance.into_iter().max_by(|(_, result1), (_, result2)| result1.cmp(result2)) else {
			bail!("Unable to determine best indicator");
		};
		Ok(best_indicator)
	}

	fn get_indicators(&self, symbol: &String, contracts: u32) -> Result<Vec<SymbolIndicator>> {
		let mut indicators: Vec<SymbolIndicator> = Vec::new();
		// Brute-force parameter space for all indicators
		let periods = vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 30, 40, 50];
		let fast_periods = vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20];
		let slow_periods = vec![10, 15, 20, 25, 30, 40, 50];
		let signal_periods = vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
		let lower_thresholds = vec![15.0, 20.0, 25.0, 30.0];
		let upper_thresholds = vec![70.0, 75.0, 80.0, 85.0];
		let channel_periods = vec![5, 6, 7, 8, 9, 10, 12, 15, 20, 30, 40, 50];
		let multipliers = vec![1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0];
		let exit_modes = vec![ChannelExitMode::Center, ChannelExitMode::Opposite];
		for indicator_string in self.enabled_indicators.iter() {
			match indicator_string.as_str() {
				MomentumIndicator::ID => {
					for period in periods.iter() {
						let indicator_result = MomentumIndicator::new(*period);
						Self::add_indicator(symbol, contracts, indicator_result, &mut indicators);
					}
				},
				SimpleMovingAverage::ID => {
					for period in periods.iter() {
						let indicator_result = SimpleMovingAverage::new(*period, None);
						Self::add_indicator(symbol, contracts, indicator_result, &mut indicators);
					}
				},
				LinearMovingAverage::ID => {
					for period in periods.iter() {
						let indicator_result = LinearMovingAverage::new(*period, None);
						Self::add_indicator(symbol, contracts, indicator_result, &mut indicators);
					}
				},
				ExponentialMovingAverage::ID => {
					for period in periods.iter() {
						let indicator_result = ExponentialMovingAverage::new(*period, None);
						Self::add_indicator(symbol, contracts, indicator_result, &mut indicators);
					}
				},
				SimpleMovingAverage::CROSSOVER_ID => {
					for fast_period in fast_periods.iter() {
						for slow_period in slow_periods.iter() {
							let indicator_result = SimpleMovingAverage::new(*fast_period, Some(*slow_period));
							Self::add_indicator(symbol, contracts, indicator_result, &mut indicators);
						}
					}
				},
				LinearMovingAverage::CROSSOVER_ID => {
					for fast_period in fast_periods.iter() {
						for slow_period in slow_periods.iter() {
							let indicator_result = LinearMovingAverage::new(*fast_period, Some(*slow_period));
							Self::add_indicator(symbol, contracts, indicator_result, &mut indicators);
						}
					}
				},
				ExponentialMovingAverage::CROSSOVER_ID => {
					for fast_period in fast_periods.iter() {
						for slow_period in slow_periods.iter() {
							let indicator_result = ExponentialMovingAverage::new(*fast_period, Some(*slow_period));
							Self::add_indicator(symbol, contracts, indicator_result, &mut indicators);
						}
					}
				},
				RelativeStrengthIndicator::ID => {
					for period in periods.iter() {
						for high_threshold in upper_thresholds.iter() {
							for low_threshold in lower_thresholds.iter() {
								let indicator_result = RelativeStrengthIndicator::new(*period, *low_threshold, *high_threshold);
								Self::add_indicator(symbol, contracts, indicator_result, &mut indicators);
							}
						}
					}
				},
				MovingAverageConvergence::ID => {
					for signal_period in signal_periods.iter() {
						for fast_period in fast_periods.iter() {
							for slow_period in slow_periods.iter() {
								let indicator_result = MovingAverageConvergence::new(*signal_period, *fast_period, *slow_period);
								Self::add_indicator(symbol, contracts, indicator_result, &mut indicators);
							}
						}
					}
				},
				PercentagePriceOscillator::ID => {
					for signal_period in signal_periods.iter() {
						for fast_period in fast_periods.iter() {
							for slow_period in slow_periods.iter() {
								let indicator_result = PercentagePriceOscillator::new(*signal_period, *fast_period, *slow_period);
								Self::add_indicator(symbol, contracts, indicator_result, &mut indicators);
							}
						}
					}
				},
				BollingerBands::ID => {
					for period in channel_periods.iter() {
						for multiplier in multipliers.iter() {
							for exit_mode in exit_modes.iter() {
								let indicator_result = BollingerBands::new(*period, *multiplier, exit_mode.clone());
								Self::add_indicator(symbol, contracts, indicator_result, &mut indicators);
							}
						}
					}
				},
				KeltnerChannel::ID => {
					for period in channel_periods.iter() {
						for multiplier in multipliers.iter() {
							for exit_mode in exit_modes.iter() {
								let indicator_result = KeltnerChannel::new(*period, *multiplier, exit_mode.clone());
								Self::add_indicator(symbol, contracts, indicator_result, &mut indicators);
							}
						}
					}
				},
				DonchianChannel::ID => {
					for period in channel_periods.iter() {
						for exit_mode in exit_modes.iter() {
							let indicator_result = DonchianChannel::new(*period, exit_mode.clone());
							Self::add_indicator(symbol, contracts, indicator_result, &mut indicators);
						}
					}
				},
				_ => bail!("Unknown indicator type \"{indicator_string}\"")
			};
		}
		Ok(indicators)
	}

	fn add_indicator<T: Indicator + 'static>(symbol: &String, contracts: u32, indicator_result: Result<T>, indicators: &mut Vec<SymbolIndicator>) {
		if let Ok(indicator) = indicator_result {
			let symbol_indicator = SymbolIndicator {
				symbol: symbol.clone(),
				contracts,
				indicator: Box::new(indicator)
			};
			indicators.push(symbol_indicator);
		}
	}
}

impl Strategy for AutoIndicatorStrategy {
	fn next(&mut self) -> Result<()> {
		if self.periods_since_optimization >= self.optimization_period {
			// We have been running the same indicators for too long
			// Discard them and retrain them using more recent data
			self.indicators.clear();
			self.periods_since_optimization = 0;
		}
		for (symbol, contracts) in self.symbol_contracts.iter() {
			if !self.backtest.borrow().is_available(symbol)? {
				// This symbol isn't available on the exchange yet, skip it
				continue;
			}
			let auto_indicator = if let Some(auto_indicator) = self.indicators.iter_mut().find(|x| x.symbol_indicator.symbol == *symbol) {
				// Reuse optimized indicator
				auto_indicator
			} else {
				// There is no indicator available for this symbol, train a new one
				let auto_indicator = self.optimize_indicator(symbol, *contracts, &self.backtest.borrow())?;
				let indicator_description = auto_indicator.symbol_indicator.indicator.get_description();
				let long_short_description = match (auto_indicator.enable_long, auto_indicator.enable_short) {
					(true, true) => "both long and short",
					(true, false) => "long only",
					(false, true) => "short only",
					_ => bail!("Invalid long/short flags")
				};
				let message = format!("New indicator for {symbol}: {indicator_description} ({long_short_description})");
				self.backtest.borrow_mut().log_event(EventType::Information, message);
				self.indicators.push(auto_indicator);
				self.indicators.last_mut().unwrap()
			};
			let indicator = &mut auto_indicator.symbol_indicator.indicator;
			if let Some(initialization_bars) = indicator.needs_initialization() {
				let backtest = self.backtest.borrow();
				let initialization_records = backtest.get_records(symbol, initialization_bars)?;
				indicator.initialize(&initialization_records);
			}
			let record = self.backtest.borrow().most_recent_record(symbol)?;
			let state = IndicatorStrategy::get_position_state(symbol, &self.backtest.borrow());
			let Some(signal) = indicator.next(&record, state) else {
				return Ok(());
			};
			IndicatorStrategy::trade(signal, auto_indicator.enable_long, auto_indicator.enable_short, &auto_indicator.symbol_indicator, self.backtest.clone())?;
		}
		self.periods_since_optimization += 1;
		Ok(())
	}
}