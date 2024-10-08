use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use anyhow::{Result, bail, anyhow};
use regex::Regex;
use rhai::{Engine, Expr, Scope, Stmt, AST};
use unq_common::backtest::Backtest;
use unq_common::strategy::{Strategy, StrategyParameterType, StrategyParameters};

const DECLARE_STATEMENT: &'static str = "declare";

type ApiContextCell<'a> = Rc<RefCell<ApiContext<'a>>>;

pub struct ScriptStrategy<'a> {
	symbols: Vec<String>,
	script: CompiledScript<'a>,
	backtest: &'a RefCell<Backtest<'a>>,
	current_symbol: Option<String>
}

struct CompiledScript<'a> {
	engine: Engine,
	scope: Scope<'a>,
	script_ast: AST,
	declared_variables: Vec<String>
}

// #[derive(Clone)]
struct ApiContext<'a> {
	backtest: &'a RefCell<Backtest<'a>>,
	current_symbol: Option<String>
}

impl<'a> ScriptStrategy<'a> {
	pub fn from_parameters(script: &String, script_directory: &String, symbols: &Vec<String>, parameters: &StrategyParameters, backtest: &'a RefCell<Backtest<'a>>) -> Result<Self> {
		// Basic restriction to prevent directory traversal attacks
		let pattern = Regex::new("^[A-Za-z0-9 ]+$")?;
		if !pattern.is_match(script.as_str()) {
			bail!("Invalid characters in script path");
		}
		let file_name = format!("{script}.rhai");
		let path = Path::new(script_directory).join(&file_name);
		let mut script = Self::compile_script(path)?;
		for parameter in parameters.iter() {
			if !script.declared_variables.contains(&parameter.name) {
				bail!("Script \"{file_name}\" has not declared a variable called \"{}\"", parameter.name);
			}
			match parameter.get_type()? {
				StrategyParameterType::NumericSingle => {
					if let Some(value) = parameter.value.clone() {
						script.scope.push(&parameter.name, value.get());
					}
				},
				StrategyParameterType::NumericMulti => {
					if let Some(web_values) = parameter.values.clone() {
						let values: Vec<f64> = web_values.iter().map(|x| x.get()).collect();
						script.scope.push(&parameter.name, values);
					}
				},
				StrategyParameterType::NumericRange => {
					bail!("The scripting engine does not support numeric range parameters");
				},
				StrategyParameterType::Bool => {
					if let Some(value) = parameter.bool_value.clone() {
						script.scope.push(&parameter.name, value);
					}
				},
				StrategyParameterType::StringSingle => {
					if let Some(value) = parameter.string_value.clone() {
						script.scope.push(&parameter.name, value);
					}
				},
				StrategyParameterType::StringMulti => {
					if let Some(values) = parameter.string_values.clone() {
						script.scope.push(&parameter.name, values);
					}
				}
			}
		}
		let mut strategy = Self {
			symbols: symbols.clone(),
			script,
			backtest,
			current_symbol: None
		};
		strategy.register_functions();
		Ok(strategy)
	}

	fn compile_script(path: PathBuf) -> Result<CompiledScript<'a>> {
		let engine = Engine::new();
		let mut scope = Scope::new();
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
			scope,
			script_ast: transformed_ast,
			declared_variables
		};
		Ok(script)
	}

	fn register_functions(&mut self) {
		let mut engine = &mut self.script.engine;
		engine.register_type_with_name::<ApiContextCell>("api");
		engine.register_fn("buy", |api: &mut ApiContextCell| api.borrow_mut().buy());
		engine.register_fn("sell", |api: &mut ApiContextCell| api.borrow_mut().sell());
		engine.register_fn("close", |api: &mut ApiContextCell| api.borrow_mut().close());
	}
}

impl<'a> Strategy for ScriptStrategy<'a> {
	fn next(&mut self) -> Result<()> {
		for symbol in self.symbols.iter() {
			self.current_symbol = Some(symbol.clone());
			let mut script = &mut self.script;
			script.engine.run_ast_with_scope(&mut script.scope, &script.script_ast)
				.map_err(|error| anyhow!("Failed to run script: {error}"))?;
		}
		todo!()
	}
}

impl<'a> ApiContext<'a> {
	fn buy(&mut self) {
	}

	fn sell(&mut self) {
	}

	fn close(&mut self) {
	}
}