use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use anyhow::{Result, bail, anyhow, Context};
use regex::Regex;
use rhai::{Dynamic, Engine, EvalAltResult, ImmutableString, Scope, AST};
use unq_common::backtest::{Backtest, PositionSide};
use unq_common::strategy::{Strategy, StrategyParameter, StrategyParameterType, StrategyParameters};
use crate::CONTRACTS_PARAMETER;

const SCRIPT_PARAMETER: &'static str = "script";
const POSITIONS_PARAMETER: &'static str = "positions";
const MARGIN_RATIO_PARAMETER: &'static str = "margin";

type ApiContextCell = Rc<RefCell<ApiContext>>;
type ApiResult<T> = Result<T, Box<EvalAltResult>>;

/*
The scripting strategy uses one of the following three position sizing algorithms:

1. Fixed Contracts

The "contracts" parameter contains an array of integers representing a fixed number of contracts to purchase per symbol.
Both arrays must be the same length since contracts[i] is the number of contracts to be used for symbol symbols[i].

2. Fixed Slots

The user specifies a target margin ratio between 0.0 and 1.0 using the "margin" parameter.
This value represents a fraction of the account worth the algorithm should approximately allocate in total.
Warning: since this logic only considers initial margin the overnight margin will generally exceed this fraction.
This margin target is divided into a fixed number of slots, with each slot representing one of the symbols targeted by the script.
This is equivalent to an equal weight allocation in which individual assets may be left out due to a lack of trade signals.
This means that the actual margin used may be considerably lower than the margin target due to a lack of signals.

3. Dynamic Slots

This approach is identical to the "Fixed Slots" equal weight allocation, but without empty slots due to a lack of signals.
As long as there's at least one trade signal it will attempt to reach the total target margin.
Typically, this will increase the number of trades since position sizes are adjusted more aggressively.
*/
#[derive(Clone, PartialEq, Eq)]
pub enum PositionSizing {
	FixedContracts,
	FixedSlots,
	DynamicSlots
}

#[derive(Clone, PartialEq, Eq)]
enum TradeSignal {
	Long,
	Short,
	Close
}

pub struct ScriptStrategy<'a> {
	symbols: Vec<String>,
	position_sizing: PositionSizing,
	contracts: Option<Vec<u32>>,
	margin_ratio: Option<f64>,
	context: ApiContextCell,
	engine: Engine,
	scope: Scope<'a>,
	script: AST,
	backtest: RefCell<Backtest>
}

#[derive(Clone)]
struct ApiContext {
	current_symbol: String,
	parameters: HashMap<String, Dynamic>,
	signals: HashMap<String, TradeSignal>,
	backtest: RefCell<Backtest>
}

struct PositionTarget {
	symbol: String,
	side: PositionSide,
	contracts: u32
}

impl<'a> ScriptStrategy<'a> {
	pub const ID: &'static str = "script";

	pub fn new(script: String, script_directory: &String, symbols: &Vec<String>, position_sizing: PositionSizing, contracts: Option<Vec<u32>>, margin_ratio: Option<f64>, parameters: HashMap<String, Dynamic>, backtest: RefCell<Backtest>) -> Result<Self> {
		// Basic restriction to prevent directory traversal attacks
		let pattern = Regex::new("^[A-Za-z0-9 ]+$")?;
		if !pattern.is_match(script.as_str()) {
			bail!("Invalid characters in script path");
		}
		let file_name = format!("{script}.rhai");
		let path = Path::new(script_directory).join(&file_name);
		let engine = Engine::new();
		let script = engine.compile_file(path)
			.map_err(|error| anyhow!("Failed to compile script: {error}"))?;
		let current_symbol = symbols.first()
			.with_context(|| "No symbols specified")?
			.clone();
		match (&contracts, margin_ratio) {
			(Some(_), Some(_)) => bail!("You cannot specify both fixed contract numbers as well as a margin ratio"),
			(Some(_), None) => {
				if position_sizing != PositionSizing::FixedContracts {
					bail!("Cannot use contracts parameter with other position sizing modes");
				}
			},
			(None, Some(margin_ratio)) => {
				if margin_ratio <= 0.0 {
					bail!("The specified margin ratio is too low");
				} else if margin_ratio >= 1.0 {
					bail!("The specified margin ratio is too high");
				} else if position_sizing == PositionSizing::FixedContracts {
					bail!("Cannot use margin ratio parameter with fixed contracts position sizing");
				}
			},
			(None, None) => bail!("You must specify either fixed contract numbers or a margin ratio")
		};
		let context = ApiContext {
			current_symbol,
			parameters,
			signals: HashMap::new(),
			backtest: backtest.clone()
		};
		let context_cell = Rc::new(RefCell::new(context));
		let mut scope = Scope::new();
		let mut strategy = Self {
			symbols: symbols.clone(),
			position_sizing,
			contracts,
			margin_ratio,
			context: context_cell,
			engine,
			scope,
			script,
			backtest
		};
		strategy.register_functions();
		Ok(strategy)
	}

	pub fn from_parameters(script_directory: &String, symbols: &Vec<String>, parameters: &StrategyParameters, backtest: RefCell<Backtest>) -> Result<Self> {
		let script_parameter = parameters.get_string(SCRIPT_PARAMETER)?;
		let script = script_parameter.with_context(|| "Script parameter has not been specified")?;
		let positions_parameter = parameters.get_string(POSITIONS_PARAMETER)?;
		let contracts_parameter = parameters.get_values(CONTRACTS_PARAMETER)?;
		let margin_ratio = parameters.get_value(MARGIN_RATIO_PARAMETER)?;
		let (position_sizing, contracts) = match (positions_parameter, contracts_parameter) {
			(Some(positions_string), None) => {
				let position_sizing = match positions_string.as_str() {
					"fixed" => PositionSizing::FixedSlots,
					"dynamic" => PositionSizing::DynamicSlots,
					_ => bail!("Unknown positions sizing mode")
				};
				(position_sizing, None)
			},
			(None, Some(contracts)) => {
				let integers = contracts
					.iter()
					.map(|x| *x as u32)
					.collect();
				if contracts.len() != symbols.len() {
					bail!("The number of symbols and contracts must be identical");
				}
				(PositionSizing::FixedContracts, Some(integers))
			},
			_ => bail!("Invalid combination of positions/contracts parameters")
		};
		let mut dynamic_parameters = HashMap::new();
		for parameter in parameters.iter() {
			let name = parameter.name.as_str();
			if name != SCRIPT_PARAMETER && name != POSITIONS_PARAMETER && name != CONTRACTS_PARAMETER {
				let dynamic_value = Self::get_dynamic_value(parameter)?;
				dynamic_parameters.insert(parameter.name.clone(), dynamic_value);
			}
		}
		Self::new(script, script_directory, symbols, position_sizing, contracts, margin_ratio, dynamic_parameters, backtest)
	}

	fn get_dynamic_value(parameter: &StrategyParameter) -> Result<Dynamic> {
		match parameter.get_type()? {
			StrategyParameterType::NumericSingle => {
				if let Some(value) = parameter.value.clone() {
					return Ok(value.get().into());
				}
			},
			StrategyParameterType::NumericMulti => {
				if let Some(web_values) = parameter.values.clone() {
					let values: Vec<f64> = web_values.iter().map(|x| x.get()).collect();
					return Ok(values.into());
				}
			},
			StrategyParameterType::NumericRange => {
				bail!("The scripting engine does not support numeric range parameters");
			},
			StrategyParameterType::Bool => {
				if let Some(value) = parameter.bool_value.clone() {
					return Ok(value.into());
				}
			},
			StrategyParameterType::StringSingle => {
				if let Some(value) = parameter.string_value.clone() {
					return Ok(value.into());
				}
			},
			StrategyParameterType::StringMulti => {
				if let Some(values) = parameter.string_values.clone() {
					return Ok(values.into());
				}
			}
		};
		bail!("Unable to convert parameter to dynamic value for scripting engine");
	}

	fn get_side_from_signal(signal: &TradeSignal) -> Result<PositionSide> {
		match signal {
			TradeSignal::Long => Ok(PositionSide::Long),
			TradeSignal::Short => Ok(PositionSide::Short),
			TradeSignal::Close => bail!("Unable to translate close signal to side")
		}
	}

	fn get_position_targets(&self) -> Result<Vec<PositionTarget>> {
		let position_targets = match self.position_sizing {
			PositionSizing::FixedContracts => self.get_fixed_contract_targets()?,
			PositionSizing::FixedSlots | PositionSizing::DynamicSlots => self.get_slot_targets()?
		};
		Ok(position_targets)
	}

	fn is_valid_symbol_signal(&self, symbol: &String, signal: &TradeSignal) -> Result<bool> {
		if *signal == TradeSignal::Close {
			Ok(false)
		} else {
			let skip = !self.backtest.borrow().is_available(&symbol)?;
			Ok(skip)
		}
	}

	fn get_fixed_contract_targets(&self) -> Result<Vec<PositionTarget>> {
		let Some(contracts) = &self.contracts else {
			bail!("Unable to retrieve contracts");
		};
		let context = self.context.borrow();
		let mut position_targets = Vec::new();
		for i in 0..self.symbols.len() {
			let symbol = &self.symbols[i];
			let contracts = contracts[i];
			let Some(signal) = context.signals.get(symbol) else {
				bail!("Missing trade signal for symbol {symbol}");
			};
			if !self.is_valid_symbol_signal(symbol, signal)? {
				continue;
			}
			let side = Self::get_side_from_signal(signal)?;
			let position_target = PositionTarget {
				symbol: symbol.clone(),
				side,
				contracts
			};
			position_targets.push(position_target);
		}
		Ok(position_targets)
	}

	fn get_slot_targets(&self) -> Result<Vec<PositionTarget>> {
		let backtest = self.backtest.borrow();
		let account_value = backtest.get_account_value();
		let Some(margin_ratio) = self.margin_ratio else {
			bail!("Margin ratio must be set");
		};
		let target_margin = margin_ratio * account_value;
		let mut valid_symbol_signals: Vec<(&String, &TradeSignal)> = Vec::new();
		let context = self.context.borrow();
		for (symbol, signal) in context.signals.iter() {
			if self.is_valid_symbol_signal(symbol, signal)? {
				valid_symbol_signals.push((symbol, signal));
			}
		}
		let slots = if self.position_sizing == PositionSizing::FixedSlots {
			self.symbols.len()
		} else {
			valid_symbol_signals.len()
		};
		let position_margin = target_margin / (slots as f64);
		let mut position_targets = Vec::new();
		for (symbol, signal) in valid_symbol_signals.iter() {
			let side = Self::get_side_from_signal(signal)?;
			let symbol_margin = backtest.get_margin(symbol)?;
			let contracts = (position_margin / symbol_margin).round() as u32;
			let position_target = PositionTarget {
				symbol: (*symbol).clone(),
				side,
				contracts
			};
			position_targets.push(position_target);
		}
		Ok(position_targets)
	}

	fn close_positions(&mut self, position_targets: &Vec<PositionTarget>) -> Result<()> {
		let positions = self.backtest.borrow().get_positions();
		for position in positions {
			let close_position = if let Some(position_target) = position_targets.iter().find(|x| x.symbol == position.symbol) {
				// Close all positions whose current side does not match the signal
				position_target.side != position.side
			} else {
				// Close all positions for which we have no long/short signal
				true
			};
			if close_position {
				self.backtest.borrow_mut().close_position(position.id, position.count)?;
			}
		}
		Ok(())
	}

	fn get_contract_counts(&self) -> HashMap<String, u32> {
		// Count contracts per symbol using the remaining symbols
		let positions = self.backtest.borrow().get_positions();
		let mut contract_counts = HashMap::new();
		for position in positions {
			let position_symbol = position.asset.symbol;
			let new_count = if let Some(count) = contract_counts.get(&position_symbol) {
				count + position.count
			} else {
				position.count
			};
			contract_counts.insert(position_symbol.clone(), new_count);
		}
		contract_counts
	}

	fn adjust_positions(&mut self, position_targets: &Vec<PositionTarget>) -> Result<()> {
		let contract_counts = self.get_contract_counts();
		// Adjust positions based on the differences in contracts
		for position_target in position_targets {
			let Some(count) = contract_counts.get(&position_target.symbol) else {
				bail!("Missing contract count");
			};
			let mut difference = (position_target.contracts as i32) - (*count as i32);
			if difference > 0 {
				// Open an additional position, ignore errors
				let _ = self.backtest.borrow_mut().open_position(&position_target.symbol, difference as u32, position_target.side.clone());
			} else {
				// Reduce the number of contracts we're holding
				while difference > 0 {
					if let Some(position) = self.backtest.borrow().get_position_by_root(&position_target.symbol) {
						let close_count = position.count.min(difference as u32);
						self.backtest.borrow_mut().close_position(position.id, close_count)?;
						difference -= close_count as i32;
					} else {
						bail!("Failed to adjust number of contracts held");
					}
				}
			}
		}
		Ok(())
	}

	fn register_functions(&mut self) {
		self.register_general_functions();
		self.register_indicators();
	}

	fn register_general_functions(&mut self) {
		let engine = &mut self.engine;
		let context = self.context.clone();
		engine.register_fn("parameter", move |name: ImmutableString, default_value: Dynamic| {
			context.borrow().get_parameter(name, default_value)
		});
		let context = self.context.clone();
		engine.register_fn("buy", move || {
			context.borrow_mut().buy_signal()
		});
		let context = self.context.clone();
		engine.register_fn("sell", move || {
			context.borrow_mut().sell_signal()
		});
		let context = self.context.clone();
		engine.register_fn("close", move || {
			context.borrow_mut().close_position_signal()
		});
	}

	fn register_indicators(&mut self) {
		let engine = &mut self.engine;
		let context = self.context.clone();
		engine.register_fn("sma", move |period: i64| {
			context.borrow().simple_moving_average(period)
		});
		let context = self.context.clone();
		engine.register_fn("lma", move |period: i64| {
			context.borrow().linear_moving_average(period)
		});
		let context = self.context.clone();
		engine.register_fn("ema", move |period: i64| {
			context.borrow().exponential_moving_average(period)
		});
		let context = self.context.clone();
		engine.register_fn("rsi", move |period: i64| {
			context.borrow().relative_strength_indicator(period)
		});
		let context = self.context.clone();
		engine.register_fn("macd", move |fast_period: i64, slow_period: i64| {
			context.borrow().moving_average_convergence(fast_period, slow_period)
		});
		let context = self.context.clone();
		engine.register_fn("ppo", move |fast_period: i64, slow_period: i64| {
			context.borrow().percentage_price_oscillator(fast_period, slow_period)
		});
		let context = self.context.clone();
		engine.register_fn("bollinger", move |period: i64, multiplier: f64, upper_band: bool| {
			context.borrow().bollinger_band(period, multiplier, upper_band)
		});
		let context = self.context.clone();
		engine.register_fn("keltner", move |period: i64, multiplier: f64, upper_band: bool| {
			context.borrow().keltner_channel(period, multiplier, upper_band)
		});
		let context = self.context.clone();
		engine.register_fn("donchian", move |period: i64, upper_band: bool| {
			context.borrow().donchian_channel(period, upper_band)
		});
	}
}

impl<'a> Strategy for ScriptStrategy<'a> {
	fn next(&mut self) -> Result<()> {
		let close_signals = self.symbols.iter().map(|symbol| (symbol.clone(), TradeSignal::Close));
		// Reset trade signals
		self.context.borrow_mut().signals = HashMap::from_iter(close_signals);
		// Execute script once for each symbol to generate new signals
		for symbol in self.symbols.iter() {
			self.context.borrow_mut().current_symbol = symbol.clone();
			self.engine.run_ast_with_scope(&mut self.scope, &self.script)
				.map_err(|error| anyhow!("Failed to run script: {error}"))?;
		}
		let position_targets = self.get_position_targets()?;
		self.close_positions(&position_targets)?;
		self.adjust_positions(&position_targets)?;
		Ok(())
	}
}

impl ApiContext {
	fn get_close_values(&self, period: i64) -> ApiResult<Vec<f64>> {
		let period = period as usize;
		let values = match self.backtest.borrow().get_close_values(&self.current_symbol, period) {
			Ok(values) => values,
			Err(error) => return Err("Failed to get close values".into())
		};
		if values.len() < period {
			return Err(format!("Not enough records available ({} < {period})", values.len()).into());
		}
		Ok(values)
	}

	fn validate_period(period: i64) -> ApiResult<()> {
		if period < 1 {
			return Err(format!("Invalid period ({period})").into())
		}
		Ok(())
	}

	fn validate_periods(fast_period: i64, slow_period: i64) -> ApiResult<()> {
		if fast_period < 1 {
			return Err(format!("Invalid fast period ({fast_period})").into())
		} else if slow_period < 1 {
			return Err(format!("Invalid slow period ({slow_period})").into())
		} else if fast_period >= slow_period {
			return Err(format!("Fast period must be less than slow period ({fast_period}, {slow_period})").into())
		}
		Ok(())
	}

	fn validate_multiplier(multiplier: f64) -> ApiResult<()> {
		if multiplier <= 0.0 {
			return Err(format!("Invalid multiplier ({multiplier})").into())
		}
		Ok(())
	}

	fn get_parameter(&self, name: ImmutableString, default_value: Dynamic) -> Dynamic {
		match self.parameters.get(&name.to_string()) {
			Some(value) => value.clone(),
			None => default_value
		}
	}

	fn buy_signal(&mut self) {
		self.insert_signal(TradeSignal::Long);
	}

	fn sell_signal(&mut self) {
		self.insert_signal(TradeSignal::Short);
	}

	fn close_position_signal(&mut self) {
		self.insert_signal(TradeSignal::Close);
	}

	fn insert_signal(&mut self, signal: TradeSignal) {
		self.signals.insert(self.current_symbol.clone(), signal);
	}

	fn simple_moving_average(&self, period: i64) -> ApiResult<f64> {
		Self::validate_period(period)?;
		let values = self.get_close_values(period)?;
		let sum: f64 = values.iter().sum();
		let average = sum / (period as f64);
		Ok(average)
	}

	fn linear_moving_average(&self, period: i64) -> ApiResult<f64> {
		Self::validate_period(period)?;
		let values = self.get_close_values(period)?;
		let mut average = 0.0;
		let mut i = 0;
		for x in values.iter() {
			average += ((period - i) as f64) * x;
			i += 1;
		}
		average /= ((period * (period + 1)) as f64) / 2.0;
		Ok(average)
	}

	fn exponential_moving_average(&self, period: i64) -> ApiResult<f64> {
		Self::validate_period(period)?;
		let values = self.get_close_values(period)?;
		let mut sum = 0.0;
		let mut i = 0;
		let lambda = 2.0 / ((period + 1) as f64);
		for x in values.iter() {
			let coefficient = lambda * (1.0 - lambda).powi(i);
			sum += coefficient * x;
			i += 1;
		}
		Ok(sum)
	}

	fn relative_strength_indicator(&self, period: i64) -> ApiResult<f64> {
		Self::validate_period(period)?;
		todo!()
	}

	fn moving_average_convergence(&self, fast_period: i64, slow_period: i64) -> ApiResult<f64> {
		Self::validate_periods(fast_period, slow_period)?;
		todo!()
	}

	fn percentage_price_oscillator(&self, fast_period: i64, slow_period: i64) -> ApiResult<f64> {
		Self::validate_periods(fast_period, slow_period)?;
		todo!()
	}

	fn bollinger_band(&self, period: i64, multiplier: f64, upper_band: bool) -> ApiResult<f64> {
		Self::validate_period(period)?;
		Self::validate_multiplier(multiplier)?;
		todo!()
	}

	fn keltner_channel(&self, period: i64, multiplier: f64, upper_band: bool) -> ApiResult<f64> {
		Self::validate_period(period)?;
		Self::validate_multiplier(multiplier)?;
		todo!()
	}

	fn donchian_channel(&self, period: i64, upper_band: bool) -> ApiResult<f64> {
		Self::validate_period(period)?;
		todo!()
	}
}