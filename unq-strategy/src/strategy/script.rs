use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use anyhow::{Result, bail, anyhow, Context};
use regex::Regex;
use rhai::{Engine, Expr, Scope, Stmt, AST};
use unq_common::backtest::Backtest;
use unq_common::strategy::{Strategy, StrategyParameter, StrategyParameterType, StrategyParameters};
use crate::CONTRACTS_PARAMETER;

const DECLARE_STATEMENT: &'static str = "declare";
const API_CONSTANT: &'static str = "api";
const POSITIONS_PARAMETER: &'static str = "positions";

type ApiContextCell<'a> = Rc<RefCell<ApiContext>>;

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
	context: ApiContextCell<'a>,
	script: CompiledScript<'a>,
	backtest: RefCell<Backtest>
}

struct CompiledScript<'a> {
	engine: Engine,
	scope: Scope<'a>,
	script_ast: AST,
	declared_variables: Vec<String>
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
		let script = Self::compile_script(path, &mut scope)?;
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
				Self::process_declared_variable(parameter, &mut scope)?;
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

	fn process_declared_variable(parameter: &StrategyParameter, scope: &mut Scope) -> Result<()> {
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

	fn compile_script(path: PathBuf, scope: &mut Scope<'a>) -> Result<CompiledScript<'a>> {
		let engine = Engine::new();
		let original_ast = engine.compile_file(path)
			.map_err(|error| anyhow!("Failed to compile script: {error}"))?;
		let mut declared_variables = Vec::new();
		/*
		The following code scans for special external variable declaration statements that look like this:

		declare(int_variable, 123);
		declare(float_variable, 45.6);
		declare(string_variable, "Some string.");

		These statements are removed from the AST prior to execution and create an explicit mapping from web UI parameters to the scope in the script.
		The first argument is the name of the variable while the second one is the default value that will be used if the user didn't specify it in the UI.
		*/
		for statement in original_ast.statements().to_vec() {
			if let Stmt::FnCall(function, _) = statement {
				if function.name == DECLARE_STATEMENT {
					let arguments = &function.args;
					let name_argument = &arguments[0];
					let value_argument = arguments[1].clone();
					let Expr::Variable(variable_data, _, _) = name_argument else {
						bail!("Invalid first argument in declare statement, expected a variable");
					};
					let (_, ref variable_name, _, _) = **variable_data;
					declared_variables.push(variable_name.to_string());
					match value_argument {
						Expr::IntegerConstant(value, _) => {
							scope.push(variable_name, value);
						},
						Expr::FloatConstant(value, _) => {
							scope.push(variable_name, value);
						},
						Expr::StringConstant(value, _) => {
							scope.push(variable_name, value);
						},
						_ => bail!("Invalid second argument in declare statement, expected an immediate value")
					}
				}
			}
		}
		let filtered_statements: Vec<Stmt> = original_ast.statements()
			.iter()
			.cloned()
			.filter(|statement| {
				if let Stmt::FnCall(function, _) = statement {
					function.name != DECLARE_STATEMENT
				} else {
					true
				}
			})
			.collect();
		let module = original_ast.shared_lib().clone();
		let transformed_ast = AST::new(filtered_statements, module);
		let script = CompiledScript {
			engine,
			scope: scope.clone(),
			script_ast: transformed_ast,
			declared_variables
		};
		Ok(script)
	}

	fn register_functions(&mut self) {
		self.script.scope.push_constant(API_CONSTANT, self.context.clone());
		let engine = &mut self.script.engine;
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
			let script = &mut self.script;
			script.engine.run_ast_with_scope(&mut script.scope, &script.script_ast)
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