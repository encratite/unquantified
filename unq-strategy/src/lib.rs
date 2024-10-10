mod technical;
mod strategy {
	pub mod buy_and_hold;
	pub mod indicator;
	pub mod auto_indicator;
	pub mod script;
}

use std::cell::RefCell;
use std::iter;
use anyhow::{Result, bail};
use unq_common::backtest::Backtest;
use unq_common::strategy::{Strategy, StrategyParameter, StrategyParameterType, StrategyParameters};
use crate::strategy::auto_indicator::AutoIndicatorStrategy;
use crate::strategy::buy_and_hold::BuyAndHoldStrategy;
use crate::strategy::indicator::IndicatorStrategy;

const CONTRACTS_PARAMETER: &'static str = "contracts";

type SymbolContracts = Vec<(String, u32)>;

pub fn get_strategy<'a>(name: &String, symbols: &Vec<String>, parameters: &StrategyParameters, backtest: RefCell<Backtest>) -> Result<Box<dyn Strategy + 'a>> {
	match name.as_str() {
		BuyAndHoldStrategy::ID => {
			let strategy = BuyAndHoldStrategy::from_parameters(symbols, parameters, backtest)?;
			Ok(Box::new(strategy))
		},
		IndicatorStrategy::ID => {
			let strategy = IndicatorStrategy::from_parameters(symbols, parameters, backtest)?;
			Ok(Box::new(strategy))
		},
		AutoIndicatorStrategy::ID => {
			let strategy = AutoIndicatorStrategy::from_parameters(symbols, parameters, backtest)?;
			Ok(Box::new(strategy))
		},
		_ => bail!("No such strategy")
	}
}

pub fn expand_parameters(parameters: &StrategyParameters) -> Result<Vec<StrategyParameters>> {
	let parameters_output = StrategyParameters::new();
	let output = RefCell::new(Vec::new());
	generate_parameters(parameters, parameters_output, &output)?;
	let output_vec = output.borrow().clone();
	Ok(output_vec)
}

fn generate_parameters(parameters_input: &StrategyParameters, parameters_output: StrategyParameters, output: &RefCell<Vec<StrategyParameters>>) -> Result<()> {
	let mut parameters_input = parameters_input.clone();
	let Some(parameter) = parameters_input.pop_front() else {
		// There are no remaining parameters, terminate the recursion and add the result to the output
		output.borrow_mut().push(parameters_output.clone());
		return Ok(());
	};
	let generate = |new_parameter| -> Result<()> {
		let mut new_parameters_output = parameters_output.clone();
		new_parameters_output.push_back(new_parameter);
		generate_parameters(&parameters_input, new_parameters_output, output)?;
		Ok(())
	};
	let pass_through = || -> Result<()> {
		let mut new_parameters_output = parameters_output.clone();
		new_parameters_output.push_back(parameter.clone());
		generate_parameters(&parameters_input, new_parameters_output, output)?;
		Ok(())
	};
	if &parameter.name == CONTRACTS_PARAMETER {
		/*
		Hard-coded check to prevent this parameter from getting expanded by the StrategyParameterType::NumericMulti logic
		since it actually contains array data rather than variations in parameter values that are supposed to spawn multiple backtests.
		*/
		pass_through()?;
		return Ok(());
	}
	match parameter.get_type()? {
		StrategyParameterType::NumericRange => {
			// Expand {x: 5 to 15 step 5} to [{x: 5}, {x: 10}, {x: 15}]
			let (Some(value), Some(limit)) = (parameter.value.map(|x| x.get()), parameter.limit.map(|x| x.get())) else {
				bail!("Missing numeric range parameters");
			};
			if value >= limit {
				bail!("Invalid from/to parameters in numeric range");
			}
			// Increment defaults to 1.0
			let increment = parameter.increment.map(|x| x.get()).unwrap_or(1.0);
			if increment <= 0.0 {
				bail!("Invalid from/to parameters in numeric range");
			}
			let mut i = value;
			while i <= limit {
				let iteration_parameter = StrategyParameter::single(parameter.name.clone(), i);
				generate(iteration_parameter)?;
				i += increment;
			}
		},
		StrategyParameterType::NumericMulti => {
			// Expand {x: [1, 2, 3]} to [{x: 1}, {x: 2}, {x: 3}]
			let Some(values) = parameter.values else {
				bail!("Unable to extract values");
			};
			for x in values {
				let iteration_parameter = StrategyParameter::single(parameter.name.clone(), x.get());
				generate(iteration_parameter)?;
			}
		},
		_ => {
			// It's a regular single value parameter that requires no expansion
			pass_through()?;
		}
	}
	Ok(())
}

fn get_symbol_contracts(symbols: &Vec<String>, parameters: &StrategyParameters) -> Result<SymbolContracts> {
	let contracts: Vec<u32> = match parameters.get_values(CONTRACTS_PARAMETER)? {
		Some(count) => count
			.iter()
			.map(|x| *x as u32)
			.collect(),
		None => iter::repeat(1)
			.take(symbols.len())
			.collect()
	};
	if symbols.len() != contracts.len() {
		bail!("The number of symbols and contract counts must be identical");
	}
	let pairs: SymbolContracts = symbols.iter().cloned().zip(contracts.iter().cloned()).collect();
	Ok(pairs)
}