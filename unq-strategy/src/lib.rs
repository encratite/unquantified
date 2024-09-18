mod technical;
mod strategy {
	pub mod buy_and_hold;
	pub mod indicator;
}

use std::cell::RefCell;
use std::iter;
use anyhow::{Result, bail};
use unq_common::backtest::Backtest;
use unq_common::strategy::{Strategy, StrategyParameters};
use crate::strategy::buy_and_hold::BuyAndHoldStrategy;
use crate::strategy::indicator::IndicatorStrategy;

type SymbolContracts = Vec<(String, u32)>;

pub fn get_strategy<'a>(name: &String, symbols: Vec<String>, parameters: &StrategyParameters, backtest: &'a RefCell<Backtest<'a>>) -> Result<Box<dyn Strategy + 'a>> {
	match name.as_str() {
		"buy and hold" => {
			let strategy = BuyAndHoldStrategy::from_parameters(symbols, parameters, backtest)?;
			Ok(Box::new(strategy))
		},
		"indicator" => {
			let strategy = IndicatorStrategy::from_parameters(symbols, parameters, backtest)?;
			Ok(Box::new(strategy))
		},
		_ => bail!("No such strategy")
	}
}

fn get_symbol_contracts(symbols: &Vec<String>, parameters: &StrategyParameters) -> Result<SymbolContracts> {
	let contracts: Vec<u32> = match parameters.get_values("contracts")? {
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