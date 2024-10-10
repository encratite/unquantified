use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use anyhow::{Result, bail, anyhow, Context};
use regex::Regex;
use rhai::{Engine, Scope, AST};
use unq_common::backtest::Backtest;
use unq_common::strategy::{Strategy, StrategyParameter, StrategyParameterType, StrategyParameters};
use crate::CONTRACTS_PARAMETER;

const API_CONSTANT: &'static str = "api";
const POSITIONS_PARAMETER: &'static str = "positions";

type ApiContextCell = Rc<RefCell<ApiContext>>;

#[derive(Clone)]
enum PositionSizing {
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
	signals: Vec<TradeSignal>,
	backtest: RefCell<Backtest>
}

impl<'a> ScriptStrategy<'a> {
	pub fn new(script: &String, script_directory: &String, symbols: &Vec<String>, position_sizing: PositionSizing, contracts: Option<Vec<u32>>, scope: &Scope<'a>, backtest: RefCell<Backtest>) -> Result<Self> {
		// Basic restriction to prevent directory traversal attacks
		let pattern = Regex::new("^[A-Za-z0-9 ]+$")?;
		if !pattern.is_match(script.as_str()) {
			bail!("Invalid characters in script path");
		}
		let file_name = format!("{script}.rhai");
		let path = Path::new(script_directory).join(&file_name);
		let mut scope: Scope<'a> = scope.clone();
		let engine = Engine::new();
		let script = engine.compile_file(path)
			.map_err(|error| anyhow!("Failed to compile script: {error}"))?;
		let current_symbol = symbols.first()
			.with_context(|| "No symbols specified")?
			.clone();
		let signals = symbols
			.iter()
			.map(|_| TradeSignal::Close)
			.collect();
		let context = ApiContext {
			current_symbol,
			position_sizing,
			contracts,
			signals,
			backtest: backtest.clone()
		};
		let context_cell = Rc::new(RefCell::new(context));
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

	pub fn from_parameters(script: &String, script_directory: &String, symbols: &Vec<String>, parameters: &StrategyParameters, backtest: RefCell<Backtest>) -> Result<Self> {
		let mut scope = Scope::new();
		for parameter in parameters.iter() {
			let name = parameter.name.as_str();
			if name != POSITIONS_PARAMETER && name != CONTRACTS_PARAMETER {
				Self::add_parameter_to_scope(parameter, &mut scope)?;
			}
		}
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
		Self::new(script, script_directory, symbols, position_sizing, contracts, &scope, backtest)
	}

	fn add_parameter_to_scope(parameter: &StrategyParameter, scope: &mut Scope) -> Result<()> {
		match parameter.get_type()? {
			StrategyParameterType::NumericSingle => {
				if let Some(value) = parameter.value.clone() {
					scope.push(&parameter.name, value.get());
				}
			},
			StrategyParameterType::NumericMulti => {
				if let Some(web_values) = parameter.values.clone() {
					let values: Vec<f64> = web_values.iter().map(|x| x.get()).collect();
					scope.push(&parameter.name, values);
				}
			},
			StrategyParameterType::NumericRange => {
				bail!("The scripting engine does not support numeric range parameters");
			},
			StrategyParameterType::Bool => {
				if let Some(value) = parameter.bool_value.clone() {
					scope.push(&parameter.name, value);
				}
			},
			StrategyParameterType::StringSingle => {
				if let Some(value) = parameter.string_value.clone() {
					scope.push(&parameter.name, value);
				}
			},
			StrategyParameterType::StringMulti => {
				if let Some(values) = parameter.string_values.clone() {
					scope.push(&parameter.name, values);
				}
			}
		}
		Ok(())
	}

	fn register_functions(&mut self) {
		self.scope.push_constant(API_CONSTANT, self.context.clone());
		let engine = &mut self.engine;
		engine.register_type_with_name::<ApiContextCell>(API_CONSTANT);
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
	fn buy(&mut self) {
	}

	fn sell(&mut self) {
	}

	fn close(&mut self) {
	}
}