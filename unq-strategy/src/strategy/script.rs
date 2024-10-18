use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use anyhow::{Result, bail, anyhow, Context};
use regex::Regex;
use rhai::{Dynamic, Engine, ImmutableString, Scope, AST};
use unq_common::backtest::{Backtest, PositionSide};
use unq_common::ohlc::OhlcRecord;
use unq_common::strategy::{Strategy, StrategyParameter, StrategyParameterType, StrategyParameters};
use crate::api_context::ApiContext;
use crate::CONTRACTS_PARAMETER;

const SCRIPT_PARAMETER: &'static str = "script";
const POSITIONS_PARAMETER: &'static str = "positions";
const MARGIN_RATIO_PARAMETER: &'static str = "margin";

const TRADE_SIGNAL_LONG: i64 = 1;
const TRADE_SIGNAL_CLOSE: i64 = 0;
const TRADE_SIGNAL_SHORT: i64 = -1;

type ApiContextCell = Rc<RefCell<ApiContext>>;

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
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PositionSizing {
	FixedContracts,
	FixedSlots,
	DynamicSlots
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TradeSignal {
	Long,
	Close,
	Short
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
	backtest: Rc<RefCell<Backtest>>
}

#[derive(Debug)]
struct PositionTarget {
	symbol: String,
	side: PositionSide,
	contracts: u32
}

impl<'a> ScriptStrategy<'a> {
	pub const ID: &'static str = "script";

	pub fn new(script: String, script_directory: &String, symbols: &Vec<String>, position_sizing: PositionSizing, contracts: Option<Vec<u32>>, margin_ratio: Option<f64>, parameters: HashMap<String, Dynamic>, backtest: Rc<RefCell<Backtest>>) -> Result<Self> {
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
		let context = ApiContext::new(current_symbol, parameters, backtest.clone());
		let context_cell = Rc::new(RefCell::new(context));
		let scope = Scope::new();
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
		strategy.initialize_engine()?;
		Ok(strategy)
	}

	pub fn from_parameters(script_directory: &String, symbols: &Vec<String>, parameters: &StrategyParameters, backtest: Rc<RefCell<Backtest>>) -> Result<Self> {
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

	fn get_fixed_contract_targets(&self) -> Result<Vec<PositionTarget>> {
		let Some(contracts) = &self.contracts else {
			bail!("Unable to retrieve contracts");
		};
		let context = self.context.borrow();
		let mut position_targets = Vec::new();
		for i in 0..self.symbols.len() {
			let symbol = &self.symbols[i];
			let contracts = contracts[i];
			let Some(signal) = context.get_signal(symbol) else {
				bail!("Missing trade signal for symbol {symbol}");
			};
			if !context.is_valid_symbol_signal(symbol, signal)? {
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
		let context = self.context.borrow();
		let valid_symbol_signals = context.get_valid_symbol_signals()?;
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
			let mut contracts = (position_margin / symbol_margin).round() as u32;
			if valid_symbol_signals.len() == 1 && contracts == 0 {
				contracts = 1;
			}
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
			let close_position = if let Some(position_target) = position_targets.iter().find(|x| x.symbol == position.asset.symbol) {
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
			let count = match contract_counts.get(&position_target.symbol) {
				Some(count) => *count,
				None => 0
			};
			let mut difference = (position_target.contracts as i32) - (count as i32);
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

	fn get_trade_signal(trade_signal_int: i64) -> Result<TradeSignal> {
		match trade_signal_int {
			TRADE_SIGNAL_LONG => Ok(TradeSignal::Long),
			TRADE_SIGNAL_CLOSE => Ok(TradeSignal::Close),
			TRADE_SIGNAL_SHORT => Ok(TradeSignal::Short),
			_ => bail!("Unable to convert trade signal integer ({trade_signal_int})")
		}
	}

	fn push_constants(&mut self) {
		self.scope.push_constant("LONG", TRADE_SIGNAL_LONG);
		self.scope.push_constant("CLOSE", TRADE_SIGNAL_CLOSE);
		self.scope.push_constant("SHORT", TRADE_SIGNAL_SHORT);
	}

	fn register_functions(&mut self) {
		self.register_general_functions();
		self.register_indicators();
	}

	fn register_general_functions(&mut self) {
		let engine = &mut self.engine;
		let context = self.context.clone();
		engine.register_fn("parameter", move |name: ImmutableString, default_value: i64| {
			let output = context.borrow().get_parameter_int(name, default_value);
			output
		});
		let context = self.context.clone();
		engine.register_fn("parameter", move |name: ImmutableString, default_value: f64| {
			let output = context.borrow().get_parameter_float(name, default_value);
			output
		});
		let context = self.context.clone();
		engine.register_fn("parameter", move |name: ImmutableString, default_value: ImmutableString| {
			let output = context.borrow().get_parameter_string(name, default_value);
			output
		});
		let context = self.context.clone();
		engine.register_fn("time", move || {
			context.borrow().time()
		});
		let context = self.context.clone();
		engine.register_fn("month", move || {
			context.borrow().month()
		});
		let context = self.context.clone();
		engine.register_fn("close", move || {
			context.borrow().close()
		});
	}

	fn register_indicators(&mut self) {
		let engine = &mut self.engine;
		let context = self.context.clone();
		engine.register_fn("sma", move |period: i64| {
			context.borrow_mut().simple_moving_average(period)
		});
		let context = self.context.clone();
		engine.register_fn("lma", move |period: i64| {
			context.borrow_mut().linear_moving_average(period)
		});
		let context = self.context.clone();
		engine.register_fn("ema", move |period: i64| {
			context.borrow_mut().exponential_moving_average(period)
		});
		let context = self.context.clone();
		engine.register_fn("rsi", move |period: i64| {
			context.borrow_mut().relative_strength_indicator(period)
		});
		let context = self.context.clone();
		engine.register_fn("macd", move |signal_period: i64, fast_period: i64, slow_period: i64| {
			context.borrow_mut().moving_average_convergence(signal_period, fast_period, slow_period)
		});
		let context = self.context.clone();
		engine.register_fn("ppo", move |signal_period: i64, fast_period: i64, slow_period: i64| {
			context.borrow_mut().percentage_price_oscillator(signal_period, fast_period, slow_period)
		});
		let context = self.context.clone();
		engine.register_fn("bollinger", move |period: i64, multiplier: f64| {
			context.borrow_mut().bollinger_band(period, multiplier)
		});
		let context = self.context.clone();
		engine.register_fn("keltner", move |period: i64, multiplier: f64| {
			context.borrow_mut().keltner_channel(period, multiplier)
		});
		let context = self.context.clone();
		engine.register_fn("donchian", move |period: i64| {
			context.borrow_mut().donchian_channel(period)
		});
		let context = self.context.clone();
		engine.register_fn("adx", move |period: i64| {
			context.borrow_mut().average_directional_index(period)
		});
		let context = self.context.clone();
		engine.register_fn("atr", move |period: i64| {
			context.borrow_mut().average_true_range(period)
		});

		let context = self.context.clone();
		engine.register_fn("roc", move |period: i64| {
			context.borrow_mut().rate_of_change(period)
		});
	}

	fn initialize_engine(&mut self) -> Result<()> {
		self.push_constants();
		self.register_functions();
		self.engine.run_ast_with_scope(&mut self.scope, &self.script)
			.map_err(|error| anyhow!("Failed to run script: {error}"))?;
		Ok(())
	}

	fn update_indicators(&mut self, symbol: &String, record: &OhlcRecord) {
		let mut context = self.context.borrow_mut();
		context.update_indicators(symbol, record)
	}
}

impl<'a> Strategy for ScriptStrategy<'a> {
	fn next(&mut self) -> Result<()> {
		self.context.borrow_mut().reset_signals(&self.symbols);
		// Execute function for each symbol to generate new signals
		for symbol in self.symbols.clone().iter() {
			self.context.borrow_mut().set_symbol(symbol);
			let record = match self.backtest.borrow().get_current_record(&symbol) {
				Ok(record) => record.clone(),
				_ => continue
			};
			self.update_indicators(symbol, &record);
			let signal_int = self.engine.call_fn::<i64>(&mut self.scope, &self.script, "next", ())
				.map_err(|error| anyhow!("Failed to execute next function: {error}"))?;
			let signal = Self::get_trade_signal(signal_int)?;
			self.context.borrow_mut().insert_signal(symbol, signal);
		}
		let position_targets = self.get_position_targets()?;
		self.close_positions(&position_targets)?;
		self.adjust_positions(&position_targets)?;
		Ok(())
	}
}