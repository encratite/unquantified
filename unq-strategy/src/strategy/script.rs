use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use anyhow::{Result, bail, anyhow, Context};
use regex::Regex;
use rhai::{Dynamic, Engine, ImmutableString, Scope, AST};
use unq_common::backtest::Backtest;
use unq_common::strategy::{Strategy, StrategyParameter, StrategyParameterType, StrategyParameters};
use crate::CONTRACTS_PARAMETER;

const SCRIPT_PARAMETER: &'static str = "script";
const POSITIONS_PARAMETER: &'static str = "positions";

const API_CONSTANT: &'static str = "api";

type ApiContextCell = Rc<RefCell<ApiContext>>;

#[derive(Clone)]
pub enum PositionSizing {
	FixedContracts,
	FixedSlots,
	DynamicSlots
}

#[derive(Clone)]
enum TradeSignal {
	Long,
	Short,
	Close
}

pub struct ScriptStrategy<'a> {
	symbols: Vec<String>,
	context: ApiContextCell,
	engine: Engine,
	scope: Scope<'a>,
	script: AST,
	backtest: RefCell<Backtest>
}

#[derive(Clone)]
struct ApiContext {
	current_symbol: String,
	position_sizing: PositionSizing,
	contracts: Option<Vec<u32>>,
	parameters: HashMap<String, Dynamic>,
	signals: HashMap<String, TradeSignal>,
	backtest: RefCell<Backtest>
}

impl<'a> ScriptStrategy<'a> {
	pub const ID: &'static str = "script";

	pub fn new(script: String, script_directory: &String, symbols: &Vec<String>, position_sizing: PositionSizing, contracts: Option<Vec<u32>>, parameters: HashMap<String, Dynamic>, backtest: RefCell<Backtest>) -> Result<Self> {
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
		let context = ApiContext {
			current_symbol,
			position_sizing,
			contracts,
			parameters,
			signals: HashMap::new(),
			backtest: backtest.clone()
		};
		let context_cell = Rc::new(RefCell::new(context));
		let mut scope = Scope::new();
		scope.push_constant(API_CONSTANT, context_cell.clone());
		let mut strategy = Self {
			symbols: symbols.clone(),
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
		Self::new(script, script_directory, symbols, position_sizing, contracts, dynamic_parameters, backtest)
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

	fn register_functions(&mut self) {
		let engine = &mut self.engine;
		engine.register_type_with_name::<ApiContextCell>(API_CONSTANT);
		engine.register_fn("parameter", |api: &ApiContextCell, name: ImmutableString, default_value: Dynamic| {
			api.borrow().get_parameter(name, default_value)
		});
		engine.register_fn("buy", |api: &mut ApiContextCell| api.borrow_mut().buy());
		engine.register_fn("sell", |api: &mut ApiContextCell| api.borrow_mut().sell());
		engine.register_fn("close", |api: &mut ApiContextCell| api.borrow_mut().close());
	}
}

impl<'a> Strategy for ScriptStrategy<'a> {
	fn next(&mut self) -> Result<()> {
		for symbol in self.symbols.iter() {
			self.context.borrow_mut().current_symbol = symbol.clone();
			self.engine.run_ast_with_scope(&mut self.scope, &self.script)
				.map_err(|error| anyhow!("Failed to run script: {error}"))?;
		}
		todo!()
	}
}

impl ApiContext {
	fn get_parameter(&self, name: ImmutableString, default_value: Dynamic) -> Dynamic {
		match self.parameters.get(&name.to_string()) {
			Some(value) => value.clone(),
			None => default_value
		}
	}

	fn buy(&mut self) {
		self.insert_signal(TradeSignal::Long);
	}

	fn sell(&mut self) {
		self.insert_signal(TradeSignal::Short);
	}

	fn close(&mut self) {
		self.insert_signal(TradeSignal::Close);
	}

	fn insert_signal(&mut self, signal: TradeSignal) {
		self.signals.insert(self.current_symbol.clone(), signal);
	}
}